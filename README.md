# Stream ETL

实时流式 ETL 编排引擎：多源接入、Schema 演进与断点重放。

## 范围

本仓库从零开始实现上述方向的可用工具，不依赖外部同类实现。

## 状态

已实现可用版本：零第三方依赖（仅需 Python 3 标准库），提供 `run` 与
`replay` 两个命令，配置为 YAML，记录为 JSON Lines。

## 约定

- 公开行为以 README 与源码为准。
- 后续需求在此基线上增量实现。

## 命令

```sh
bin/stream-etl run    --config config.yaml --output out.jsonl --checkpoint cp.json
bin/stream-etl replay --config config.yaml --output out.jsonl --checkpoint cp.json
```

- `run`：全新运行。`--output` 与 `--checkpoint` 都必须尚不存在。
- `replay`：断点重放。`--output` 与 `--checkpoint` 都必须已存在；从最后一个
  已提交批次之后继续，先截断输出中未提交的尾部，再追加记录，不重复、不跳过。

成功时 stdout 不输出任何记录，退出码为 0，输出文件与检查点均存在。

## 配置（YAML）

顶层只接受 `sources` 与 `transforms` 两个键。

```yaml
sources:
  - id: orders            # 唯一非空 id
    type: jsonl           # jsonl（逐行 JSON 对象）或 csv
    path: data/orders.jsonl
    batch_size: 100       # 正整数，每处理这么多条提交一次检查点
    transforms:            # 可省略；来源级转换，在公共 transforms 之后按序执行
      - op: cast
        field: total
        type: number
transforms:                # 可省略；按序执行，对全部 source 生效
  - op: rename
    from: amount
    to: total
  - op: drop
    field: secret.token
  - op: set
    field: pipeline
    value: orders-v1
  - op: cast
    field: total
    type: number          # string | integer | number | boolean
  - op: filter
    field: total
    compare: gte          # eq | ne | lt | lte | gt | gte
    value: 0
```

`type` 取 `jsonl` 或 `csv`，两者可在 `sources` 中混用并按配置顺序处理；
`id`、`path`、`batch_size`、`transforms` 的含义对两种类型完全一致，CSV
源不引入额外配置键。

每个 source 可省略 `transforms`（行为与之前一致）；配置后先执行顶层公共
转换，再按自身顺序执行该来源的转换。来源级转换的元素与公共转换相同
（`rename` / `drop` / `set` / `cast` / `filter`），校验规则、错误类型与
数据语义完全一致；两者都进入配置指纹，任一变化都会使旧检查点不可恢复。

字段路径为点分路径（如 `a.b.c`），支持嵌套 payload：

- `rename`：源路径必须存在，目标父路径必须存在，目标字段不得已存在；
- `drop`、`cast`：路径必须存在；
- `set`：父路径必须存在（可新建叶子字段）；
- `cast` 仅接受 `string`、`integer`、`number`、`boolean`，转换失败即
  数据校验错误，当前批次不提交；
- `filter`：路径必须存在且字段值必须是标量；条件成立时保留记录，不成立
  时该输入记录不产生任何输出（但仍计入已消费输入与 `batch_size`）。

`filter` 的 `compare` 仅接受 `eq`、`ne`、`lt`、`lte`、`gt`、`gte`：

- `eq` / `ne` 按 JSON 标量语义比较：`null` 只等于 `null`，字符串按内容
  精确比较，布尔只与布尔相等，整数与浮点按数值相等判断（布尔不作为
  数值）；类型不匹配的标量一律不相等。
- `lt` / `lte` / `gt` / `gte` 只比较有限的非布尔数值；字段值不是有限
  数值（字符串、布尔、null、非有限浮点）即数据校验错误。
- 配置中 `filter` 缺少 `field` / `compare` / `value`、`compare` 非法、
  `value` 不是标量，或排序比较的 `value` 不是有限数值，都是
  `ConfigurationError`（退出码 2）。

转换不得静默丢字段：任何路径缺失或覆盖已有字段都会报错。

## 记录格式（JSON Lines）

输入每行一个 JSON 对象，至少包含：

```json
{"source_id": "orders", "event_id": "o-1", "payload": {"amount": "10.5"}}
```

处理后输出：

```json
{"source_id": "orders", "event_id": "o-1", "schema_version": 1, "data": {"amount": 10.5}}
```

- 按配置中 source 的顺序处理；`source_id` 必须与所属 source 的 id 一致。
- 每条输入恰好输出一次；被 `filter` 过滤的输入记录不输出，但仍计入
  `batch_size` 与检查点进度，重复 replay 不会重复或跳过。
- 同一 source 实际输出的 `data` 发生字段新增、字段删除或 cast 类型改变
  时，`schema_version` 单调递增；值变化本身不产生新版本，被过滤的记录
  也不制造版本变化。旧记录不回写。
- 每 `batch_size` 条以及每个 source 结束时刷盘并原子替换检查点。

### CSV 源

`type: csv` 的输入为 UTF-8（可带仅位于文件开头的 BOM），按 RFC 4180
常见子集解析：逗号分隔、双引号包裹字段、`""` 转义双引号；接受 LF 与
CRLF，引号内允许换行，因此一条逻辑记录可跨越多条物理行。

- 第一条逻辑记录是表头：列名唯一且非空，必须同时含 `source_id` 与
  `event_id`；其余列按声明顺序组成扁平的字符串 `data`，两个控制列不
  进入 `data`，空单元格保留为空字符串。
- 每条数据记录输出一条与 jsonl 完全相同格式的记录；CSV 的 `event_id`
  始终是字符串且不能为空，`source_id` 必须等于所属 source 的 id。
- 公共 `transforms` 先执行，来源转换随后执行；CSV 字段均为字符串，需
  要数值/布尔语义时用 `cast`。
- 以下情形都是 `DataValidationError`（退出码 3）：重复或空表头、缺少
  必需列、空逻辑记录、列数不一致、引号未闭合、BOM 出现在非开头位置、
  无效 UTF-8。
- 非法 csv 配置仍为 `ConfigurationError`（退出码 2），输入不可读为
  `SourceError`，输出/检查点不可写为 `SinkError`（均为退出码 5）。

## 断点与恢复

检查点按逻辑记录边界记录每个 source 的进度：`offset` 为最后已提交记录
结束处的字节偏移，`input_hash` 为输入字节前缀 `[0:offset]` 的 SHA-256。

- `run` 要求 `--output` 与 `--checkpoint` 均不存在；`replay` 要求两者
  均存在。
- replay 先校验每个 source 的已提交输入前缀哈希一致且文件未变短，否则
  报告 `CheckpointError`（退出码 4）；随后截断 output 中超过已提交偏移
  的未提交尾部，再从最后已提交批次之后的下一条记录继续，不重复、不跳过。
- 对 csv 源，进度定位于逻辑记录边界，因此跨物理行的记录也能精确恢复；
- 无新记录时文件结果确定，可反复 replay。

## 错误与退出码

错误输出到 stderr，形如 `Error: <类型>: <细节>`：

| 类型 | 退出码 | 触发场景 |
| --- | --- | --- |
| `ConfigurationError` | 2 | 配置缺失/非法、source 不完整/类型非法、未知操作、非法路径、filter 配置非法 |
| `DataValidationError` | 3 | jsonl 记录缺字段/格式错、CSV 表头或记录非法、路径不存在、cast 失败、filter 字段缺失或类型不适用于比较 |
| `CheckpointError` | 4 | 检查点损坏、版本不匹配、配置不匹配、已提交前缀改变或输入变短 |
| `SourceError` | 5 | 输入不可读 |
| `SinkError` | 5 | 输出或检查点不可写 |

## 测试

```sh
python3 -m pytest tests/
```
