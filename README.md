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
    dedup_window: 100     # 可省略；正整数 N，按 event_id 对最近 N-1 条去重
    event_time: meta.ts   # 可省略；三键必须同时配置，指向转换前 payload 的点分路径
    watermark_delay: 5    # 可省略；与 event_time 同一刻度的有限非负数值
    late_policy: drop     # 可省略；drop 或 error
    schema_policy: allow  # 可省略；allow 或 compatible
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
  - op: explode
    field: items          # 待展开数组字段的点分路径
```

`type` 取 `jsonl` 或 `csv`，两者可在 `sources` 中混用并按配置顺序处理；
`id`、`path`、`batch_size`、`transforms` 的含义对两种类型完全一致，CSV
源不引入额外配置键。

每个 source 可省略 `transforms`（行为与之前一致）；配置后先执行顶层公共
转换，再按自身顺序执行该来源的转换。来源级转换的元素与公共转换相同
（`rename` / `drop` / `set` / `cast` / `filter` / `explode`），校验规则、
错误类型与数据语义完全一致；两者都进入配置指纹，任一变化都会使旧检查点
不可恢复。

字段路径为点分路径（如 `a.b.c`），支持嵌套 payload：

- `rename`：源路径必须存在，目标父路径必须存在，目标字段不得已存在；
- `drop`、`cast`：路径必须存在；
- `set`：父路径必须存在（可新建叶子字段）；
- `cast` 仅接受 `string`、`integer`、`number`、`boolean`，转换失败即
  数据校验错误，当前批次不提交；
- `filter`：路径必须存在且字段值必须是标量；条件成立时保留记录，不成立
  时该输入记录不产生输出。

转换不得静默丢字段：任何路径缺失或覆盖已有字段都会报错。

### 过滤（op: filter）

`filter` 需要 `field`、`compare`、`value` 三个键，按其在转换序列中的
位置作用于当前 `data`（公共过滤先于来源级过滤）：

- `compare` 仅接受 `eq`、`ne`、`lt`、`lte`、`gt`、`gte`；`value` 必须是
  标量。`lt`/`lte`/`gt`/`gte` 的 `value` 还必须是有限的非布尔数值。
  缺少键、`compare` 越界、`value` 非标量或类型不适用于所选 `compare`
  都是 `ConfigurationError`（退出码 2）。
- `eq`/`ne` 按 JSON 标量语义比较：`null` 只等于 `null`；字符串按内容
  精确比较；布尔只等于布尔；`integer` 与 `number` 按数值比较，布尔不
  作为数值。`lt`/`lte`/`gt`/`gte` 只比较有限的非布尔数值。
- 字段缺失、父路径不是对象、字段值是数组或对象、字段数值非有限，或
  字段值类型不适用于所选 `compare`（如对字符串做 `lt`）都是
  `DataValidationError`（退出码 3），当前批次不提交。
- 被过滤的记录仍计入 `batch_size` 与已提交输入前缀，只是不产生输出、
  不引发 `schema_version` 变化；`schema_version` 只依据实际输出的连续
  `data` 形状演进。未配置过滤与 `explode` 时每条输入恰好输出一次；有
  过滤项时命中的记录输出零次、其余恰好一次，replay 依旧不重复、不跳过。

### 数组展开（op: explode）

`explode` 只接受 `field` 一个键（点分字段路径），可放在公共 transforms
或来源级 transforms 的任意位置。执行到 `explode` 时读取当前 `data` 中该
路径的数组，按元素顺序把一条记录展开为多条分支：原数组字段被移除，每个
元素（必须是 JSON 对象）的键与数组所在层的其余字段合并，形成一条独立
分支；`source_id` 与 `event_id` 信封原样保留，输出仍是常规 JSON Lines
记录，不增加新字段。

- 每条分支从 `explode` 之后的下一个转换开始独立处理：后续 `filter` 可以
  只保留部分分支，后续 `explode` 可以继续展开；输出顺序按数组顺序与配置
  顺序稳定确定。
- 空数组产生零条输出，但该输入仍被消费（计入 `batch_size` 与已提交输入
  前缀）；非空数组每个元素产生一条输出，同一输入的各条输出共享
  `event_id`。
- `schema_version` 只观察每条实际输出的 `data` 形状，按现有规则递增；
  空数组或被过滤消除的分支不创建版本。
- 缺少 `field`、`field` 非字符串或路径非法、含未知配置键都是
  `ConfigurationError`（退出码 2）。
- 字段路径不存在、父路径不是对象、目标值不是数组、元素不是对象、元素键
  与同层保留字段冲突，都是 `DataValidationError`（退出码 3），当前批次
  不提交；任一分支的后续转换或过滤错误同样不提交当前输入批次。
- 未配置 `explode` 的配置行为完全不变：每条输入恰好输出一次（被 `filter`
  命中的除外），旧配置仍可读取旧检查点并确定性续跑。

### 来源级 Schema 策略（schema_policy）

每个 source 可配置 `schema_policy`，只接受 `allow` 与 `compatible`
两个字符串；键缺失或显式 `allow` 完全保留既有行为（形状自由演进，
`schema_version` 按现有规则递增），且不进入配置指纹——为旧配置追加
`schema_policy: allow` 后既有检查点仍可正常 replay。未知键、非法值
或非字符串值都是 `ConfigurationError`（退出码 2）。

`compatible` 在公共 transforms 与来源 transforms 按原顺序执行完之后，
对该 source **实际产生**的每条 `data` 按输出顺序逐条判定：

- 第一条实际输出建立兼容基线；此后对象**新增字段**（含嵌套对象）为
  兼容变化，按现有规则单调递增 `schema_version`；
- 删除字段、既有字段在 `string`/`integer`/`number`/`boolean`/`null`
  之间改类、值在对象/数组/标量之间改结构、数组元素形状集合发生变化，
  均为不兼容变化；
- 被 `filter` 过滤的记录、`explode` 的空数组以及未输出的分支不参与
  判定，既不建立也不推进基线。

第一条不兼容输出抛 `DataValidationError`（退出码 3），所在批次不提
交；replay 先截掉本批未提交输出，再从同一输入边界继续，因此修复输
入后重放与连续运行结果一致。`compatible` 进入配置指纹，检查点版本
提升为 5，并只在已提交边界保存兼容基线（`schema_policy` 与
`schema_baseline`）；replay 时校验基线与配置指纹、记录数、
`schema_version`、`schema_fingerprint` 相互一致，状态缺失、损坏、
版本不匹配或互相矛盾都是 `CheckpointError`（退出码 4）。未配置
`schema_policy` 的 source 行为完全不变，版本 2/3/4 的旧检查点仍可
正常 replay。

### 有界去重（dedup_window）

每个 source 可配置正整数 `dedup_window: N`。该 source 为其已消费输入
记录维护一个容量 `N - 1` 的滑动窗口，保存最近 `N - 1` 条记录的
`event_id`：

- 判定时机在记录信封校验之后、任何转换（公共与来源级）之前；窗口只以
  `event_id` 为键，与 `payload`、转换、`filter` 无关。
- 键比较按 JSON 标量语义：`null` 只匹配 `null`；布尔只匹配同值布尔；
  整数与小数按数值相等（`1` 与 `1.0` 相同）；字符串按内容精确相等；
  布尔不作为数值，数字不等于字符串。
- 当前记录的键命中窗口中任意键即视为重复：重复项**仍被消费**——计入
  `batch_size` 与已提交输入前缀，并滑入窗口——但不执行任何转换、不产生
  输出、不触发 `schema_version` 变化。
- 首次出现的键正常进入公共转换、来源级转换与 `filter`；无论该记录最终
  是否被 `filter` 丢弃，它都已滑入窗口，因此窗口内后续同键记录仍按重复
  处理。
- `N = 1` 时容量为 0、窗口中没有任何历史记录，因此不会去重；不配置
  `dedup_window` 的 source 行为与之前完全一致。窗口随每条已消费记录
  滑动，滑出窗口的旧键再次出现会被当作首次出现而保留。
- 窗口按 source 各自独立维护，source 之间互不影响；jsonl 与 csv 的去重
  口径完全一致（CSV 的 `event_id` 恒为非空字符串）。
- `dedup_window` 取布尔、零、负数、小数、字符串或其他类型都是
  `ConfigurationError`（退出码 2）；jsonl 的 `event_id` 为数组或对象是
  `DataValidationError`（退出码 3），`null` 与其他标量则是合法的键。
- 配置 `dedup_window` 会进入配置指纹，改动它会使旧检查点不可恢复；
  未配置时配置指纹与旧版逐字节一致，既有检查点可正常 replay。

### 事件时间水位线（event_time / watermark_delay / late_policy）

每个 source 可同时配置 `event_time`、`watermark_delay`、`late_policy`
三个键（必须同时出现，缺少任意一个都是 `ConfigurationError`），用于按
事件时间处理迟到数据：

- `event_time` 是指向**转换前** payload 的点分路径；`watermark_delay`
  是与事件时间同一数值刻度上的有限非负数值（整数或小数）；
  `late_policy` 只接受 `drop` 或 `error`。路径非法、类型或取值越界、
  含未知键都是 `ConfigurationError`（退出码 2）；三键均进入配置指纹。
- 启用后每个 source 独立维护已见非迟到记录的**最大事件时间**，水位线为
  `最大事件时间 - watermark_delay`。判定发生在信封校验之后、去重与任何
  转换之前，且使用记录进入**之前**的水位线：`event_time` 严格早于水位线
  才算迟到，等于水位线不迟到，首条记录（尚无水位线）不迟到。
- 每条非迟到记录的 `event_time` 随后参与最大值更新——即使该记录之后被
  `dedup_window` 判重、被 `filter` 丢弃，或因 `explode` 空数组产生零条
  输出。
- 记录转换前 payload 中的 `event_time` 路径必须存在且为有限的非布尔
  数值；路径缺失、父路径不是对象，或值为 `null`、字符串、布尔、数组、
  对象、NaN、无穷，都是 `DataValidationError`（退出码 3），当前批次不
  提交。注意 CSV 源的 payload 全是字符串，配置水位线后每条记录都会因此
  报错。
- `late_policy: drop`：迟到记录仍被消费——计入 `batch_size` 与已提交
  输入前缀，并滑入去重窗口——但不产生输出、不引发 `schema_version`
  变化、不抬高最大事件时间。
- `late_policy: error`：迟到记录返回 `DataValidationError`（退出码 3），
  当前批次及其后的结果不提交。
- 最大事件时间与水位线随每个 source 的已提交边界持久化（检查点版本
  提升为 4），replay 恢复后继续判定；状态缺失、损坏、类型错误、水位线
  与最大值/延迟不一致，或与配置不一致，都是 `CheckpointError`
  （退出码 4）。未配置水位线的 source 行为完全不变，版本 2/3 的旧检查点
  仍可正常 replay。

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
- 未配置 `filter` 与 `explode` 时每条输入恰好输出一次；被 `filter` 命中
  的记录不产生输出，`explode` 则按数组元素数产生零到多条输出。
- 同一 source 的 `data` 发生字段新增、字段删除或 cast 类型改变时，
  `schema_version` 单调递增；值变化本身不产生新版本。旧记录不回写。
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
配置了 `dedup_window` 的 source 还会在每个已提交边界保存完整滑动窗口
（`dedup_window` 与 `dedup_keys`），因此窗口随批次持久化、崩溃后可精确
恢复；这会把检查点版本提升为 3，未配置去重的检查点仍为版本 2。配置了
水位线的 source 还会保存 `event_time`、`watermark_delay`、`late_policy`
与 `max_event_time`、`watermark`，检查点版本提升为 4。配置了
`schema_policy: compatible` 的 source 还会保存 `schema_policy` 与
`schema_baseline`（兼容基线），检查点版本提升为 5。

- `run` 要求 `--output` 与 `--checkpoint` 均不存在；`replay` 要求两者
  均存在。
- replay 先校验每个 source 的已提交输入前缀哈希一致且文件未变短，否则
  报告 `CheckpointError`（退出码 4）；随后截断 output 中超过已提交偏移
  的未提交尾部，再从最后已提交批次之后的下一条记录继续，不重复、不跳过。
  去重窗口与水位线（最大事件时间）随已提交边界恢复，因此 replay 截断
  未提交输出后继续处理，得到与一次性连续运行完全相同的保留记录与顺序；
  反复 replay 而无新输入时结果确定。
- 对 csv 源，进度定位于逻辑记录边界，因此跨物理行的记录也能精确恢复；
- 检查点缺少或写坏去重窗口状态、保存的窗口与当前配置不一致（如
  `dedup_window` 被改动）、窗口长度与记录数矛盾，或版本 2 检查点配合
  配置了去重的配置，都是 `CheckpointError`（退出码 4）；水位线状态
  （`max_event_time` / `watermark` 等）缺失、损坏、与配置或记录数不一致，
  或版本 2/3 检查点配合配置了水位线的配置，同样是 `CheckpointError`；
  兼容基线状态（`schema_policy` / `schema_baseline`）缺失、损坏、与
  `schema_version` / `schema_fingerprint` / 记录数矛盾，或版本 2/3/4
  检查点配合配置了 `schema_policy: compatible` 的配置，同样是
  `CheckpointError`；
- 无新记录时文件结果确定，可反复 replay。

## 错误与退出码

错误输出到 stderr，形如 `Error: <类型>: <细节>`：

| 类型 | 退出码 | 触发场景 |
| --- | --- | --- |
| `ConfigurationError` | 2 | 配置缺失/非法、source 不完整/类型非法、未知操作、非法路径、过滤或 explode 配置非法、`dedup_window` 非正整数、水位线三键不全或取值非法、`schema_policy` 取值非法或非字符串 |
| `DataValidationError` | 3 | jsonl 记录缺字段/格式错、CSV 表头或记录非法、路径不存在、cast 失败、过滤条件无法求值、explode 目标非数组/元素非对象/键冲突、`event_id` 为数组或对象、`event_time` 缺失或非法、`late_policy: error` 下记录迟到、`schema_policy: compatible` 下出现不兼容输出 |
| `CheckpointError` | 4 | 检查点损坏、版本不匹配、配置不匹配、已提交前缀改变或输入变短、去重窗口或水位线状态缺失/损坏/与配置或记录数不一致、兼容基线状态缺失/损坏/互相矛盾 |
| `SourceError` | 5 | 输入不可读 |
| `SinkError` | 5 | 输出或检查点不可写 |

## 测试

```sh
python3 -m pytest tests/
```
