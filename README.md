# multi-source-watermark-coordinator

Python 多源批次完成度协调库,处理分区水位、迟到文件、缺源和跨源合并。
仅依赖标准库,适用于"每日合并任务依赖订单、支付、退款等多个独立上传的
数据源"的场景:不再使用固定时刻调度,而是在一个批次真正达到合并条件后
才推进统一 watermark。

## 快速开始

```python
from datetime import datetime, timedelta
from watermark_coordinator import (
    MissingPolicy, SourceConfig, WatermarkCoordinator,
)

coordinator = WatermarkCoordinator(
    sources=[
        SourceConfig("orders", allowed_lateness=timedelta(hours=6)),
        SourceConfig("payments", allowed_lateness=timedelta(hours=6)),
        SourceConfig("refunds", allowed_lateness=timedelta(hours=6),
                     on_missing=MissingPolicy.SKIP),
    ],
    state_path="coordinator_state.json",  # 持久化,进程重启可恢复
)

now = datetime.now()
coordinator.declare_partition("orders", "2026-11-15",
                              expected_files=["part-0", "part-1"], now=now)
coordinator.file_arrived("orders", "2026-11-15", "part-0", now)
coordinator.file_arrived("orders", "2026-11-15", "part-1", now)
events = coordinator.complete_signal("orders", "2026-11-15",
                                     signal_id="upload-123", now=now)

events += coordinator.tick(datetime.now())  # 定时驱动:超时检测 + 水位推进
for event in events:
    print(event.to_dict())
```

## Completeness 判定

单个 `(source, partition)` 分区满足以下**全部**条件才算完整:

1. **已声明**:源通过 `declare_partition` 声明了该分区和预期文件清单;
2. **收到完成信号**:`complete_signal` 已到达(按 `signal_id` 去重,
   重复信号返回 `DUPLICATE_SIGNAL_IGNORED`,幂等);
3. **无文件缺口**:声明的 `expected_files` 全部通过 `file_arrived` 到达。
   完成信号先于最后一个文件到达时,分区仍不完整,直到缺口补齐;
4. **未取消**:未被 `cancel_upload` 取消。

除"完整"外,分区还有两种结案方式,同样视为**已有最终结论**:

- **取消**:合并前取消按"无数据"结案,不再阻塞水位;
- **跳过**:超过允许延迟且策略为 `MissingPolicy.SKIP` 时按缺数结案。

## Watermark 推进规则

- **合并条件**:一个批次(时间分区)下所有 `required=True` 的源都已
  有最终结论(完整 / 取消 / 跳过)。非必需源不阻塞水位。
- **连续推进**:watermark 按分区日期顺序只跨过连续就绪的批次;某个
  批次未就绪时,后续批次即使就绪也不会跳过它。
- **单调不回退**:watermark 只前进。迟到数据到达已合并分区时,发出
  `RECOMPUTE_REQUIRED` 通知下游重算该批次;已合并分区被取消时发出
  `CORRECTION_REQUIRED`。水位本身绝不动。
- **跨日边界**:分区按日(`YYYY-MM-DD`)划分,分区结束时刻为次日
  00:00。文件在次日到达、但仍在 `allowed_lateness` 内,属于正常迟到,
  不判超时。
- **长时间缺源**:`tick(now)` 时检查,超过 `分区结束 + allowed_lateness`
  仍未完整的源:
  - `MissingPolicy.BLOCK`(默认):发一次 `SOURCE_OVERDUE`,水位停在该
    批次等待补齐;
  - `MissingPolicy.SKIP`:发 `SOURCE_SKIPPED` 并按缺数结案,水位继续推进。
- **进程重启**:指定 `state_path` 后,每次状态变更原子写入 JSON
  (临时文件 + `os.replace`),重启后自动恢复水位、各分区进度和
  信号去重集合。

## 事件类型

| 事件 | 含义 |
| --- | --- |
| `BATCH_READY` | 批次满足合并条件 |
| `WATERMARK_ADVANCED` | 统一水位推进到该批次 |
| `RECOMPUTE_REQUIRED` | 迟到数据到达已合并分区,需重算 |
| `CORRECTION_REQUIRED` | 已合并分区被取消,需更正结果 |
| `SOURCE_OVERDUE` | 源超过允许延迟未完整(阻塞中) |
| `SOURCE_SKIPPED` | 源超时,按缺数跳过 |
| `UPLOAD_CANCELLED` | 上传在合并前取消,按无数据结案 |
| `DUPLICATE_SIGNAL_IGNORED` | 重复完成信号被忽略 |

## 测试

```bash
python -m unittest
```
