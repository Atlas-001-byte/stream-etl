"""最小 YAML 子集解析器。

仅支持本项目配置所需的 YAML 特性：

- 两空格缩进的映射（``key: value``）与序列（``- item``），序列项可为行内起始的映射；
- 纯量标量、单引号、双引号字符串；整数、布尔、null；
- ``#`` 注释（整行或行尾，引号内的 # 视为普通字符）。

不支持锚点、流式写法、多行字符串等其余 YAML 特性；遇到不支持或缩进
非法的输入时抛出 :class:`YAMLParseError`，由上层转换为 ConfigurationError。
"""

import json


class YAMLParseError(Exception):
    """YAML 语法不被接受。"""


class _Line:
    __slots__ = ("indent", "text", "lineno")

    def __init__(self, indent, text, lineno):
        self.indent = indent
        self.text = text
        self.lineno = lineno


def _strip_comment(s):
    out = []
    quote = None
    i = 0
    while i < len(s):
        ch = s[i]
        if quote is not None:
            out.append(ch)
            if ch == "\\" and quote == '"' and i + 1 < len(s):
                out.append(s[i + 1])
                i += 2
                continue
            if ch == quote:
                quote = None
        else:
            if ch in ("'", '"'):
                quote = ch
                out.append(ch)
            elif ch == "#" and (i == 0 or s[i - 1] in " \t"):
                break
            else:
                out.append(ch)
        i += 1
    return "".join(out).rstrip()


def _preprocess(text):
    if text.startswith("﻿"):
        text = text[1:]
    lines = []
    for lineno, raw in enumerate(text.split("\n"), start=1):
        body = raw[: len(raw) - len(raw.lstrip(" \t"))]
        if "\t" in body:
            raise YAMLParseError("第 %d 行缩进不允许使用 tab" % lineno)
        content = _strip_comment(raw.strip(" \t\r"))
        if not content:
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        lines.append(_Line(indent, content, lineno))
    return lines


def _split_map_entry(text):
    """返回 (key, value)；不是映射项时返回 None。"""
    quote = None
    i = 0
    while i < len(text):
        ch = text[i]
        if quote is not None:
            if ch == "\\" and quote == '"' and i + 1 < len(text):
                i += 2
                continue
            if ch == quote:
                quote = None
        elif ch in ("'", '"'):
            quote = ch
        elif ch == ":" and (i + 1 == len(text) or text[i + 1] == " "):
            return text[:i].strip(), text[i + 1 :].strip()
        i += 1
    return None


def _parse_scalar(token):
    token = token.strip()
    if token == "":
        return None
    if token[0] in ("'", '"'):
        if len(token) < 2 or token[-1] != token[0]:
            raise YAMLParseError("引号未闭合: %r" % token)
        if token[0] == "'":
            return token[1:-1].replace("''", "'")
        try:
            value = json.loads(token)
        except ValueError as exc:
            raise YAMLParseError("双引号字符串无法解析: %r" % token) from exc
        if not isinstance(value, str):
            raise YAMLParseError("标量必须是字符串: %r" % token)
        return value
    low = token.lower()
    if low in ("null", "~"):
        return None
    if low == "true":
        return True
    if low == "false":
        return False
    try:
        return int(token)
    except ValueError:
        pass
    return token


def _parse_block(lines, i, indent):
    text = lines[i].text
    if text == "-" or text.startswith("- "):
        return _parse_seq(lines, i, indent)
    return _parse_map(lines, i, indent)


def _parse_map(lines, i, indent):
    result = {}
    while i < len(lines):
        line = lines[i]
        if line.indent < indent:
            break
        if line.indent > indent:
            raise YAMLParseError("第 %d 行缩进层级不合法" % line.lineno)
        entry = _split_map_entry(line.text)
        if entry is None:
            raise YAMLParseError("第 %d 行不是合法的键值对: %r" % (line.lineno, line.text))
        key_token, rest = entry
        key = _parse_scalar(key_token)
        if not isinstance(key, str) or key == "":
            raise YAMLParseError("第 %d 行键必须是非空字符串" % line.lineno)
        if key in result:
            raise YAMLParseError("第 %d 行键 %r 重复" % (line.lineno, key))
        if rest == "":
            j = i + 1
            if j < len(lines) and lines[j].indent > indent:
                value, i = _parse_block(lines, j, lines[j].indent)
            else:
                value = None
                i = j
        else:
            value = _parse_scalar(rest)
            i += 1
        result[key] = value
    return result, i


def _parse_seq(lines, i, indent):
    items = []
    while i < len(lines):
        line = lines[i]
        if line.indent < indent:
            break
        if line.indent > indent:
            raise YAMLParseError("第 %d 行缩进层级不合法" % line.lineno)
        if line.text != "-" and not line.text.startswith("- "):
            raise YAMLParseError("第 %d 行应当是序列项" % line.lineno)
        p = 1
        while p < len(line.text) and line.text[p] == " ":
            p += 1
        first = line.text[p:]
        child_indent = indent + p
        if first == "":
            j = i + 1
            if j < len(lines) and lines[j].indent > indent:
                value, i = _parse_block(lines, j, lines[j].indent)
            else:
                value = None
                i = j
        elif _split_map_entry(first) is not None:
            window = [_Line(child_indent, first, line.lineno)]
            j = i + 1
            while j < len(lines) and lines[j].indent >= child_indent:
                window.append(lines[j])
                j += 1
            value, consumed = _parse_map(window, 0, child_indent)
            if consumed != len(window):
                raise YAMLParseError("第 %d 行附近缩进层级不合法" % line.lineno)
            i = j
        else:
            value = _parse_scalar(first)
            i += 1
        items.append(value)
    return items, i


def parse(text):
    """解析 YAML 文本，返回 Python 数据结构；根节点必须是映射。"""
    if not isinstance(text, str):
        raise YAMLParseError("配置内容必须是文本")
    lines = _preprocess(text)
    if not lines:
        raise YAMLParseError("配置为空")
    if lines[0].indent != 0:
        raise YAMLParseError("第 %d 行不允许有顶层缩进" % lines[0].lineno)
    value, consumed = _parse_block(lines, 0, 0)
    if consumed != len(lines):
        raise YAMLParseError("第 %d 行缩进层级不合法" % lines[consumed].lineno)
    if not isinstance(value, dict):
        raise YAMLParseError("配置顶层必须是映射")
    return value
