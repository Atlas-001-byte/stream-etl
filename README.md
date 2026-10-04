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
    type: jsonl           # 目前仅支持逐行 JSON 对象的 jsonl
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
```

每个 source 可省略 `transforms`（行为与之前一致）；配置后先执行顶层公共
转换，再按自身顺序执行该来源的转换。来源级转换的元素与公共转换相同
（`rename` / `drop` / `set` / `cast`），校验规则、错误类型与数据语义完全
一致；两者都进入配置指纹，任一变化都会使旧检查点不可恢复。

字段路径为点分路径（如 `a.b.c`），支持嵌套 payload：

- `rename`：源路径必须存在，目标父路径必须存在，目标字段不得已存在；
- `drop`、`cast`：路径必须存在；
- `set`：父路径必须存在（可新建叶子字段）；
- `cast` 仅接受 `string`、`integer`、`number`、`boolean`，转换失败即
  数据校验错误，当前批次不提交。

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
- 每条输入恰好输出一次。
- 同一 source 的 `data` 发生字段新增、字段删除或 cast 类型改变时，
  `schema_version` 单调递增；值变化本身不产生新版本。旧记录不回写。
- 每 `batch_size` 条以及每个 source 结束时刷盘并原子替换检查点。

## 错误与退出码

错误输出到 stderr，形如 `Error: <类型>: <细节>`：

| 类型 | 退出码 | 触发场景 |
| --- | --- | --- |
| `ConfigurationError` | 2 | 配置缺失/非法、source 不完整、未知操作、非法路径 |
| `DataValidationError` | 3 | 记录缺 `source_id`/`event_id`/`payload`、路径不存在、cast 失败 |
| `CheckpointError` | 4 | 检查点损坏、版本不匹配、配置不匹配、无法恢复 |
| `SourceError` | 5 | 输入不可读 |
| `SinkError` | 5 | 输出或检查点不可写 |

## 测试

```sh
python3 -m pytest tests/
```
