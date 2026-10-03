# Stream ETL

实时流式 ETL 编排引擎：多源接入、Schema 演进与断点重放。

## 范围

本仓库从零开始实现上述方向的可用工具，不依赖外部同类实现。

## 状态

已实现：JSON Lines 数据源、按序变换管线、Schema 版本演进、`run` / `replay` 两个命令与原子检查点。仅依赖 Python 3 标准库。

## 用法

```sh
./stream-etl run    --config <配置.yaml> --output <输出.jsonl> --checkpoint <检查点.json>
./stream-etl replay --config <配置.yaml> --output <输出.jsonl> --checkpoint <检查点.json>
```

也可使用 `python3 -m stream_etl ...`。

- `run`：要求输出与检查点路径均不存在，从零处理全部输入。
- `replay`：要求输出与检查点均已存在，从最后一个已提交批次之后继续；
  输出中未提交的残留记录先被截断，再按输入重新处理 —— 追加但不重复、不跳过。

成功时 stdout 不输出任何记录，退出码为 0，输出文件与检查点均存在。

## 配置（YAML）

顶层仅接受 `sources` 与 `transforms` 两个键。

```yaml
sources:
  - id: orders          # 唯一 source id
    type: jsonl         # 目前仅支持 jsonl（逐行 JSON 对象）
    path: data/a.jsonl
    batch_size: 100     # 每处理多少条提交一次检查点（正整数）
transforms:              # 可省略；按序执行
  - op: rename
    from: address.city  # 点分嵌套路径，必须存在
    to: address.town
  - op: drop
    path: temp
  - op: set
    path: meta.kind
    value: order
  - op: cast
    path: score
    type: integer       # 仅接受 string / integer / number / boolean
```

处理顺序：按配置中的 source 顺序逐个处理；每条输入识别 `source_id`、
`event_id`、`payload`，对 `payload` 施加全部变换后写出
`source_id`、`event_id`、`schema_version`、`data` 四元组的 JSON Lines。

## Schema 演进

同一 source 内，以每条输出 `data` 的叶子字段签名（点分路径 + 叶子类型）判定：

- 首条记录的 `schema_version` 为 1；
- 字段新增、字段删除，或 `cast` 改变了叶子类型时，版本号递增；
- 签名不变则版本号不变；不回写旧记录。

各 source 的版本号各自从 1 开始。

## 检查点与提交

- 每累计 `batch_size` 条、以及每个 source 处理结束时提交：先 flush/fsync 输出，
  再以临时文件原子替换检查点。
- 数据错误发生时，当前批次不提交；已提交批次不受影响。
- 检查点记录每个 source 的已提交行数、当前 `schema_version` 与字段签名。

## 错误码

错误统一输出到 stderr，格式为 `Error: <错误类型>: <详情>`：

| 退出码 | 错误类型 | 触发情形 |
| --- | --- | --- |
| 2 | `ConfigurationError` | 配置失败、source 不完整、操作未知、路径非法、参数非法 |
| 3 | `DataValidationError` | 记录缺少 `source_id`/`event_id`/`payload`、字段路径不存在、cast 失败；当前批次不提交 |
| 4 | `CheckpointError` | 检查点损坏、版本不匹配、输出与检查点无法对齐、无法恢复 |
| 5 | `SourceError` / `SinkError` | 输入不可读 / 输出不可写 |

## 约定

- 公开行为以 README 与源码为准。
- 后续需求在此基线上增量实现。
- 兼容性仅覆盖本文档描述的 YAML 配置、JSON Lines 记录格式与 `run`、`replay`
  两个命令；其余键、文件格式与落盘位置不在承诺范围内。

## 测试

```sh
python3 tests/e2e_test.py
```
