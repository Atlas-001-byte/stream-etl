"""CSV source support: logical-record parsing and record extraction.

A CSV source is UTF-8 (an optional BOM is allowed only at the very start of
the file). Fields are separated by commas, may be wrapped in double quotes
with ``""`` as an escaped quote, and records are terminated by LF or CRLF.
Newlines may appear inside quoted fields, so one logical record can span
several physical lines.

The first logical record is the header: column names must be unique and
non-empty and must include ``source_id`` and ``event_id``. Those two control
columns never enter ``data``; the remaining columns, in header order, form
the flat payload of each data record. Empty cells stay empty strings.

All format problems (duplicate or empty header names, missing required
columns, empty logical records, column count mismatches, unterminated
quotes, stray BOMs, invalid UTF-8) are DataValidationError.
"""

from .errors import DataValidationError, SourceError

ENCODING = "utf-8"
CONTROL_COLUMNS = ("source_id", "event_id")

_BOM = b"\xef\xbb\xbf"
_CHUNK_SIZE = 65536

_QUOTE = 0x22  # "
_COMMA = 0x2C  # ,
_LF = 0x0A
_CR = 0x0D

_START, _UNQUOTED, _QUOTED, _AFTER_QUOTE = range(4)


class _ByteReader:
    """Buffered byte reader tracking the absolute offset and the bytes of
    the logical record currently being parsed (for the input digest)."""

    def __init__(self, fp, path, offset):
        fp.seek(offset)
        self._fp = fp
        self._path = path
        self._buf = b""
        self._i = 0
        self._tail = bytearray()
        self.offset = offset

    def read(self):
        """Return the next byte as an int, or None at EOF."""
        if self._i >= len(self._buf):
            try:
                self._buf = self._fp.read(_CHUNK_SIZE)
            except OSError as exc:
                raise SourceError("cannot read %s: %s" % (self._path, exc))
            self._i = 0
            if not self._buf:
                return None
        b = self._buf[self._i]
        self._i += 1
        self.offset += 1
        self._tail.append(b)
        return b

    def take_tail(self):
        """Bytes consumed since the last call (one logical record)."""
        tail = bytes(self._tail)
        del self._tail[:]
        return tail


def _parse_record(reader, where):
    """Parse one logical record into raw byte fields.

    Returns None at a clean EOF (no bytes consumed). Structural characters
    are ASCII, which never appears inside a multi-byte UTF-8 sequence, so
    scanning bytes is safe; fields are decoded afterwards.
    """
    fields = []
    field = bytearray()
    state = _START
    while True:
        b = reader.read()
        if state == _START:
            if b is None:
                if not fields and not field:
                    return None
                fields.append(bytes(field))
                return fields
            if b == _QUOTE:
                state = _QUOTED
            elif b == _COMMA:
                fields.append(b"")
            elif b == _LF:
                if not fields and not field:
                    raise DataValidationError(
                        "%s: empty logical record" % where)
                fields.append(bytes(field))
                return fields
            elif b == _CR:
                if reader.read() != _LF:
                    raise DataValidationError(
                        "%s: unexpected carriage return" % where)
                if not fields and not field:
                    raise DataValidationError(
                        "%s: empty logical record" % where)
                fields.append(bytes(field))
                return fields
            else:
                field.append(b)
                state = _UNQUOTED
        elif state == _UNQUOTED:
            if b is None:
                fields.append(bytes(field))
                return fields
            if b == _COMMA:
                fields.append(bytes(field))
                field = bytearray()
                state = _START
            elif b == _LF:
                fields.append(bytes(field))
                return fields
            elif b == _CR:
                if reader.read() != _LF:
                    raise DataValidationError(
                        "%s: unexpected carriage return" % where)
                fields.append(bytes(field))
                return fields
            elif b == _QUOTE:
                raise DataValidationError(
                    "%s: unexpected quote in unquoted field" % where)
            else:
                field.append(b)
        elif state == _QUOTED:
            if b is None:
                raise DataValidationError(
                    "%s: unterminated quoted field" % where)
            if b == _QUOTE:
                state = _AFTER_QUOTE
            else:
                field.append(b)
        else:  # _AFTER_QUOTE
            if b is None:
                fields.append(bytes(field))
                return fields
            if b == _QUOTE:  # escaped quote
                field.append(_QUOTE)
                state = _QUOTED
            elif b == _COMMA:
                fields.append(bytes(field))
                field = bytearray()
                state = _START
            elif b == _LF:
                fields.append(bytes(field))
                return fields
            elif b == _CR:
                if reader.read() != _LF:
                    raise DataValidationError(
                        "%s: unexpected carriage return" % where)
                fields.append(bytes(field))
                return fields
            else:
                raise DataValidationError(
                    "%s: unexpected character after closing quote" % where)


def _decode_fields(raw_fields, where):
    fields = []
    for raw in raw_fields:
        try:
            text = raw.decode(ENCODING)
        except UnicodeDecodeError:
            raise DataValidationError("%s: invalid UTF-8" % where)
        if "\ufeff" in text:
            raise DataValidationError(
                "%s: BOM is only allowed at the start of the file" % where)
        fields.append(text)
    return fields


def _validate_header(header, where):
    seen = set()
    for name in header:
        if name == "":
            raise DataValidationError("%s: empty column name" % where)
        if name in seen:
            raise DataValidationError(
                "%s: duplicate column name %r" % (where, name))
        seen.add(name)
    for required in CONTROL_COLUMNS:
        if required not in seen:
            raise DataValidationError(
                "%s: missing required column %r" % (where, required))


def prepare_csv_header(fp, spec, state):
    """Return the header column names, or None when the input has no
    logical records at all (empty file, possibly with a BOM).

    On a fresh run (offset 0) the optional BOM and the header record are
    consumed and counted toward the source offset and input digest. On
    replay the committed header is re-read from the already
    digest-verified prefix without touching the state.
    """
    where = "source %r record 1" % spec["id"]
    fresh = state.offset == 0
    try:
        fp.seek(0)
        head = fp.read(len(_BOM))
    except OSError as exc:
        raise SourceError("cannot read %s: %s" % (spec["path"], exc))
    start = len(_BOM) if head == _BOM else 0
    reader = _ByteReader(fp, spec["path"], start)
    raw = _parse_record(reader, where)
    if fresh:
        if start:
            state.note_bytes(_BOM)
        state.note_bytes(reader.take_tail())
        state.offset = reader.offset
    if raw is None:
        return None
    header = _decode_fields(raw, where)
    _validate_header(header, where)
    return header


def iter_csv_records(fp, spec, state, header):
    """Yield (where, source_id, event_id, payload) per data record.

    ``state.offset`` always ends a fully parsed record on a logical
    boundary, so a failure never leaves half a record committed.
    """
    record_number = state.records + 2  # the header is record 1
    reader = _ByteReader(fp, spec["path"], state.offset)
    while True:
        where = "source %r record %d" % (spec["id"], record_number)
        raw = _parse_record(reader, where)
        if raw is None:
            return
        state.offset = reader.offset
        state.note_bytes(reader.take_tail())
        fields = _decode_fields(raw, where)
        if len(fields) != len(header):
            raise DataValidationError(
                "%s: expected %d columns, got %d"
                % (where, len(header), len(fields)))
        row = dict(zip(header, fields))
        source_id = row.pop("source_id")
        event_id = row.pop("event_id")
        if source_id != spec["id"]:
            raise DataValidationError(
                "%s: source_id %r does not match configured source"
                % (where, source_id))
        if event_id == "":
            raise DataValidationError(
                "%s: event_id must be non-empty" % where)
        # ``row`` now holds exactly the payload columns, in header order.
        yield where, source_id, event_id, row
        record_number += 1
