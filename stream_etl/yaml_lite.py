"""Minimal block-style YAML parser.

Only the subset needed by stream-etl configuration files is supported:

* block mappings (``key: value``) and block sequences (``- item``)
* nested mappings and sequences (``- key: value``)
* plain scalars (string / integer / number / boolean / null)
* single- and double-quoted strings
* comments (``#``) and blank lines
* a single optional leading ``---`` document marker

Anything outside this subset (flow style, anchors, multiple documents, ...)
raises :class:`YAMLError`. This keeps configuration parsing dependency-free
while still accepting ordinary, hand-written YAML configs.
"""


class YAMLError(ValueError):
    """Raised when input is not YAML in the supported subset."""


class _Line:
    __slots__ = ("indent", "text", "lineno", "col")

    def __init__(self, indent, text, lineno, col=None):
        self.indent = indent
        self.text = text
        self.lineno = lineno
        # column at which text begins in the original source (0-based)
        self.col = indent if col is None else col


def _strip_comment(raw):
    """Remove a trailing comment, respecting quoted strings."""
    out = []
    quote = None
    i = 0
    while i < len(raw):
        ch = raw[i]
        if quote:
            out.append(ch)
            if ch == "\\" and quote == '"' and i + 1 < len(raw):
                out.append(raw[i + 1])
                i += 2
                continue
            if ch == quote:
                quote = None
        else:
            if ch in ("'", '"'):
                quote = ch
                out.append(ch)
            elif ch == "#" and (i == 0 or raw[i - 1] in " \t"):
                break
            else:
                out.append(ch)
        i += 1
    return "".join(out).rstrip()


def _tokenize(text):
    lines = []
    document_open = False
    for lineno, raw in enumerate(text.splitlines(), start=1):
        content = _strip_comment(raw)
        if not content.strip():
            continue
        body = content.lstrip(" \t")
        indent = len(content) - len(body)
        if "\t" in content[:indent]:
            raise YAMLError("tab character in indentation at line %d" % lineno)
        if body == "---":
            if document_open:
                raise YAMLError(
                    "multiple documents are not supported (line %d)" % lineno
                )
            continue
        if body == "...":
            if not document_open:
                continue
            raise YAMLError("unexpected document end marker at line %d" % lineno)
        lines.append(_Line(indent, body, lineno))
        document_open = True
    return lines


def _split_key(text, lineno):
    """Split ``key: value`` on the first colon that ends a key."""
    quote = None
    i = 0
    while i < len(text):
        ch = text[i]
        if quote:
            if ch == quote:
                quote = None
        elif ch in ("'", '"'):
            quote = ch
        elif ch == ":" and (i + 1 == len(text) or text[i + 1] == " "):
            return text[:i], text[i + 1 :].strip()
        i += 1
    raise YAMLError("expected ':' in mapping entry at line %d" % lineno)


def _unquote(token, lineno):
    token = token.strip()
    if len(token) >= 2 and token[0] == token[-1] and token[0] in ("'", '"'):
        quote = token[0]
        body = token[1:-1]
        if quote == "'":
            if "'" in body.replace("''", ""):
                raise YAMLError("invalid single-quoted string at line %d" % lineno)
            return body.replace("''", "'")
        out = []
        i = 0
        escapes = {"n": "\n", "t": "\t", "r": "\r", "\\": "\\", '"': '"', "0": "\0"}
        while i < len(body):
            ch = body[i]
            if ch != "\\":
                out.append(ch)
                i += 1
                continue
            i += 1
            if i >= len(body):
                raise YAMLError("dangling escape in string at line %d" % lineno)
            e = body[i]
            if e == "u" and i + 4 < len(body):
                try:
                    out.append(chr(int(body[i + 1 : i + 5], 16)))
                except ValueError:
                    raise YAMLError("bad unicode escape at line %d" % lineno)
                i += 5
                continue
            if e not in escapes:
                raise YAMLError("unsupported escape \\%s at line %d" % (e, lineno))
            out.append(escapes[e])
            i += 1
        return "".join(out)
    return token


def _parse_scalar(token, lineno):
    token = token.strip()
    if not token:
        return None
    if token[0] in ("'", '"'):
        if len(token) < 2 or token[-1] != token[0]:
            raise YAMLError("unterminated quoted string at line %d" % lineno)
        return _unquote(token, lineno)
    low = token.lower()
    if low in ("null", "~"):
        return None
    if low in ("true", "yes"):
        return True
    if low in ("false", "no"):
        return False
    try:
        return int(token, 10)
    except ValueError:
        pass
    try:
        float(token)
        return float(token)
    except ValueError:
        pass
    if token == "[]":
        return []
    if token == "{}":
        return {}
    if token[0] in "[]{}" or token[0] == "*" or token[0] == "&":
        raise YAMLError(
            "unsupported YAML feature %r at line %d" % (token[:8], lineno)
        )
    return token


def _parse_map(lines, i, indent):
    result = {}
    n = len(lines)
    while i < n:
        ln = lines[i]
        if ln.indent < indent:
            break
        if ln.indent > indent:
            raise YAMLError("unexpected indentation at line %d" % ln.lineno)
        if ln.text == "-" or ln.text.startswith("- "):
            break
        key_tok, val_tok = _split_key(ln.text, ln.lineno)
        key = _unquote(key_tok.strip(), ln.lineno)
        if key == "":
            raise YAMLError("empty mapping key at line %d" % ln.lineno)
        if key in result:
            raise YAMLError("duplicate key %r at line %d" % (key, ln.lineno))
        i += 1
        if val_tok == "":
            if i < n and lines[i].indent > indent:
                value, i = _parse_node(lines, i, lines[i].indent)
            else:
                value = None
        else:
            value = _parse_scalar(val_tok, ln.lineno)
        result[key] = value
    return result, i


def _parse_seq(lines, i, indent):
    result = []
    n = len(lines)
    while i < n:
        ln = lines[i]
        if ln.indent < indent:
            break
        if ln.indent > indent:
            raise YAMLError("unexpected indentation at line %d" % ln.lineno)
        if not (ln.text == "-" or ln.text.startswith("- ")):
            break
        if ln.text == "-":
            i += 1
            if i < n and lines[i].indent > indent:
                value, i = _parse_node(lines, i, lines[i].indent)
            else:
                value = None
            result.append(value)
            continue
        rest = ln.text[2:]
        col = ln.indent + 2
        if _is_map_entry(rest):
            # The rest of the dash line starts a synthetic mapping node at
            # ``col``; following lines belonging to this item align at >= col.
            tail = [_Line(col, rest, ln.lineno, col=col)] + lines[i + 1 :]
            value, consumed = _parse_node(tail, 0, col)
            result.append(value)
            i += consumed
        elif rest == "":
            raise YAMLError("malformed sequence entry at line %d" % ln.lineno)
        else:
            result.append(_parse_scalar(rest, ln.lineno))
            i += 1
    return result, i


def _is_map_entry(text):
    """True if text starts a ``key: value`` mapping entry (respecting quotes)."""
    quote = None
    for idx, ch in enumerate(text):
        if quote:
            if ch == quote:
                quote = None
        elif ch in ("'", '"'):
            quote = ch
        elif ch == ":" and (idx + 1 == len(text) or text[idx + 1] == " "):
            return True
    return False


def _parse_node(lines, i, indent):
    ln = lines[i]
    if ln.indent != indent:
        raise YAMLError("unexpected indentation at line %d" % ln.lineno)
    if ln.text == "-" or ln.text.startswith("- "):
        return _parse_seq(lines, i, indent)
    return _parse_map(lines, i, indent)


def loads(text):
    """Parse a YAML document from a string."""
    if not isinstance(text, str):
        raise YAMLError("YAML input must be text")
    lines = _tokenize(text)
    if not lines:
        return None
    value, i = _parse_node(lines, 0, lines[0].indent)
    if i != len(lines):
        raise YAMLError("unparsed content at line %d" % lines[i].lineno)
    return value


def load(fp):
    """Parse a YAML document from a file-like object."""
    return loads(fp.read())
