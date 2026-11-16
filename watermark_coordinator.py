"""多源水位协调器。

协调多个独立上传的数据源(如订单、支付、退款),在每个时间分区批次
满足合并条件后才推进统一 watermark。watermark 单调递增,绝不回退;
迟到数据通过重算/更正通知处理。

仅依赖 Python 标准库。
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import Enum
from typing import Iterable, Optional


class EventType(str, Enum):
    """协调器产生的事件类型。"""

    WATERMARK_ADVANCED = "watermark_advanced"
    BATCH_READY = "batch_ready"
    RECOMPUTE_REQUIRED = "recompute_required"
    CORRECTION_REQUIRED = "correction_required"
    SOURCE_OVERDUE = "source_overdue"
    SOURCE_SKIPPED = "source_skipped"
    UPLOAD_CANCELLED = "upload_cancelled"
    DUPLICATE_SIGNAL_IGNORED = "duplicate_signal_ignored"


@dataclass
class Event:
    """协调器输出的事件。迟到数据通过 RECOMPUTE_REQUIRED /
    CORRECTION_REQUIRED 通知下游,而不是回退 watermark。""" 

    type: EventType
    source: Optional[str]
    partition: Optional[str]
    message: str
    watermark: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "type": self.type.value,
            "source": self.source,
            "partition": self.partition,
            "message": self.message,
            "watermark": self.watermark,
        }


class MissingPolicy(str, Enum):
    """长时间缺源时的策略。"""

    BLOCK = "block"  # 一直等待,watermark 停在该批次
    SKIP = "skip"  # 超过允许延迟后按缺数处理,允许 watermark 推进


@dataclass
class SourceConfig:
    """单个数据源的配置。

    allowed_lateness: 分区结束后允许多久的延迟(跨日迟到据此判定)。
    required: 是否为合并必需源;非必需源不阻塞 watermark。
    on_missing: 超过允许延迟仍未完整时的处理策略。
    """

    name: str
    allowed_lateness: timedelta = timedelta(hours=6)
    required: bool = True
    on_missing: MissingPolicy = MissingPolicy.BLOCK


@dataclass
class PartitionState:
    """单个 (source, partition) 的上传状态。""" 

    declared: bool = False
    expected_files: set = field(default_factory=set)
    arrived_files: set = field(default_factory=set)
    signal_ids: set = field(default_factory=set)
    signal_received: bool = False
    cancelled: bool = False
    skipped: bool = False
    overdue_notified: bool = False
    declared_at: Optional[str] = None
    completed_at: Optional[str] = None

    @property
    def missing_files(self) -> set:
        return self.expected_files - self.arrived_files

    @property
    def is_complete(self) -> bool:
        """completeness 判定:已声明 + 收到完成信号 + 无文件缺口 + 未取消。

        完成信号先于最后一个文件到达时,signal_received 已为 True,
        但 missing_files 非空,分区仍不算完整,直到缺口补齐。
        """
        return (
            self.declared
            and self.signal_received
            and not self.cancelled
            and not self.missing_files
        )

    @property
    def is_resolved(self) -> bool:
        """该分区对本源已有最终结论:完整、被取消或被跳过。""" 
        return self.is_complete or self.cancelled or self.skipped

    def to_dict(self) -> dict:
        return {
            "declared": self.declared,
            "expected_files": sorted(self.expected_files),
            "arrived_files": sorted(self.arrived_files),
            "signal_ids": sorted(self.signal_ids),
            "signal_received": self.signal_received,
            "cancelled": self.cancelled,
            "skipped": self.skipped,
            "overdue_notified": self.overdue_notified,
            "declared_at": self.declared_at,
            "completed_at": self.completed_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "PartitionState":
        return cls(
            declared=data["declared"],
            expected_files=set(data["expected_files"]),
            arrived_files=set(data["arrived_files"]),
            signal_ids=set(data["signal_ids"]),
            signal_received=data["signal_received"],
            cancelled=data["cancelled"],
            skipped=data["skipped"],
            overdue_notified=data["overdue_notified"],
            declared_at=data["declared_at"],
            completed_at=data["completed_at"],
        )


def _partition_end(partition: str) -> datetime:
    """日分区 YYYY-MM-DD 的结束时刻(次日 00:00,跨日边界的基准)。"""
    day = date.fromisoformat(partition)
    return datetime.combine(day + timedelta(days=1), datetime.min.time())


class WatermarkCoordinator:
    """多源水位协调器。

    用法:各源通过 declare_partition / file_arrived / complete_signal /
    cancel_upload 上报进度;每次上报和 tick() 都会重新评估批次,
    满足合并条件即推进 watermark 并返回事件列表。
    指定 state_path 后状态持久化到磁盘,进程重启可恢复。
    """

    def __init__(
        self,
        sources: Iterable[SourceConfig],
        state_path: Optional[str] = None,
    ):
        self._configs = {s.name: s for s in sources}
        if not self._configs:
            raise ValueError("至少需要一个数据源")
        self._state_path = state_path
        # source -> partition -> PartitionState
        self._partitions: dict = {name: {} for name in self._configs}
        self._watermark: Optional[str] = None
        if state_path and os.path.exists(state_path):
            self._load(state_path)

    # ------------------------------------------------------------------
    # 只读属性
    # ------------------------------------------------------------------

    @property
    def watermark(self) -> Optional[str]:
        """当前统一水位:小于等于它的批次均已达到合并条件。""" 
        return self._watermark

    def partition_state(self, source: str, partition: str) -> PartitionState:
        return self._partitions[source][partition]

    def is_merged(self, partition: str) -> bool:
        """该分区是否已被 watermark 覆盖(即已参与合并)。"""
        return self._watermark is not None and partition <= self._watermark

    # ------------------------------------------------------------------
    # 上报接口
    # ------------------------------------------------------------------

    def declare_partition(
        self,
        source: str,
        partition: str,
        expected_files: Iterable[str],
        now: datetime,
    ) -> list:
        """源声明一个时间分区及其预期文件清单。"""
        state = self._get_state(source, partition)
        state.declared = True
        state.expected_files = set(expected_files)
        state.declared_at = now.isoformat()
        return self._after_mutation(source, partition, now)

    def file_arrived(
        self, source: str, partition: str, file_id: str, now: datetime
    ) -> list:
        """源的一个文件到达。重复到达是幂等的。"""
        state = self._get_state(source, partition)
        state.arrived_files.add(file_id)
        return self._after_mutation(source, partition, now)

    def complete_signal(
        self, source: str, partition: str, signal_id: str, now: datetime
    ) -> list:
        """源发出上传完成信号。signal_id 用于去重,重复信号被忽略。"""
        state = self._get_state(source, partition)
        if signal_id in state.signal_ids:
            return [
                Event(
                    EventType.DUPLICATE_SIGNAL_IGNORED,
                    source,
                    partition,
                    f"重复的完成信号 {signal_id} 已忽略",
                    self._watermark,
                )
            ]
        state.signal_ids.add(signal_id)
        state.signal_received = True
        return self._after_mutation(source, partition, now)

    def cancel_upload(self, source: str, partition: str, now: datetime) -> list:
        """源取消某分区的上传。

        未合并:分区按"无数据"结案,不再阻塞 watermark。
        已合并:发出 CORRECTION_REQUIRED,由下游更正已合并结果。
        """
        events = []
        state = self._get_state(source, partition)
        if self.is_merged(partition):
            events.append(
                Event(
                    EventType.CORRECTION_REQUIRED,
                    source,
                    partition,
                    f"源 {source} 取消了已合并分区 {partition} 的上传,需要更正",
                    self._watermark,
                )
            )
        else:
            state.cancelled = True
            events.append(
                Event(
                    EventType.UPLOAD_CANCELLED,
                    source,
                    partition,
                    f"源 {source} 分区 {partition} 的上传已取消,按无数据结案",
                    self._watermark,
                )
            )
        events.extend(self._evaluate(now))
        self._save()
        return events

    def tick(self, now: datetime) -> list:
        """时钟推进:检查缺源超时,并尝试推进 watermark。"""
        events = self._check_overdue(now)
        events.extend(self._evaluate(now))
        self._save()
        return events

    # ------------------------------------------------------------------
    # 内部逻辑
    # ------------------------------------------------------------------

    def _get_state(self, source: str, partition: str) -> PartitionState:
        if source not in self._configs:
            raise KeyError(f"未知数据源: {source}")
        _partition_end(partition)  # 校验分区格式
        return self._partitions[source].setdefault(partition, PartitionState())

    def _after_mutation(self, source: str, partition: str, now: datetime) -> list:
        events = []
        state = self._partitions[source][partition]
        if state.is_complete and state.completed_at is None:
            state.completed_at = now.isoformat()
        # 迟到数据:分区已被 watermark 覆盖,不能再回退水位,
        # 改为通知下游重算该批次。
        if self.is_merged(partition):
            events.append(
                Event(
                    EventType.RECOMPUTE_REQUIRED,
                    source,
                    partition,
                    f"源 {source} 的迟到数据到达已合并分区 {partition},需要重算",
                    self._watermark,
                )
            )
        events.extend(self._evaluate(now))
        self._save()
        return events

    def _check_overdue(self, now: datetime) -> list:
        """长时间缺源检测:超过 分区结束 + 允许延迟 仍未完整。"""
        events = []
        for name, config in self._configs.items():
            for partition, state in sorted(self._partitions[name].items()):
                if state.is_resolved or state.overdue_notified:
                    continue
                if self.is_merged(partition):
                    continue
                deadline = _partition_end(partition) + config.allowed_lateness
                if now <= deadline:
                    continue
                state.overdue_notified = True
                if config.required and config.on_missing is MissingPolicy.SKIP:
                    state.skipped = True
                    events.append(
                        Event(
                            EventType.SOURCE_SKIPPED,
                            name,
                            partition,
                            f"源 {name} 分区 {partition} 超过允许延迟,按缺数跳过",
                            self._watermark,
                        )
                    )
                else:
                    events.append(
                        Event(
                            EventType.SOURCE_OVERDUE,
                            name,
                            partition,
                            f"源 {name} 分区 {partition} 超过允许延迟仍未完整,"
                            f"watermark 阻塞等待",
                            self._watermark,
                        )
                    )
        return events

    def _batch_ready(self, partition: str) -> bool:
        """合并条件:所有必需源在该分区均有最终结论。"""
        for name, config in self._configs.items():
            if not config.required:
                continue
            state = self._partitions[name].get(partition)
            if state is None or not state.is_resolved:
                return False
        return True

    def _evaluate(self, now: datetime) -> list:
        """按分区顺序推进 watermark,只跨过连续就绪的批次,单调不回退。"""
        events = []
        required = [n for n, c in self._configs.items() if c.required]
        candidates = sorted(
            {p for n in required for p in self._partitions[n]}
        )
        for partition in candidates:
            if self.is_merged(partition):
                continue
            if not self._batch_ready(partition):
                break  # 不跳过未就绪批次,保证水位语义连续
            self._watermark = partition
            events.append(
                Event(
                    EventType.BATCH_READY,
                    None,
                    partition,
                    f"批次 {partition} 满足合并条件",
                    self._watermark,
                )
            )
            events.append(
                Event(
                    EventType.WATERMARK_ADVANCED,
                    None,
                    partition,
                    f"watermark 推进到 {partition}",
                    self._watermark,
                )
            )
        return events

    # ------------------------------------------------------------------
    # 持久化(进程重启恢复)
    # ------------------------------------------------------------------

    def _save(self) -> None:
        if not self._state_path:
            return
        payload = {
            "watermark": self._watermark,
            "partitions": {
                name: {p: s.to_dict() for p, s in parts.items()}
                for name, parts in self._partitions.items()
            },
        }
        directory = os.path.dirname(os.path.abspath(self._state_path))
        fd, tmp_path = tempfile.mkstemp(dir=directory, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2)
            os.replace(tmp_path, self._state_path)
        except BaseException:
            os.unlink(tmp_path)
            raise

    def _load(self, path: str) -> None:
        with open(path, encoding="utf-8") as fh:
            payload = json.load(fh)
        self._watermark = payload["watermark"]
        for name, parts in payload["partitions"].items():
            if name not in self._partitions:
                continue
            self._partitions[name] = {
                p: PartitionState.from_dict(s) for p, s in parts.items()
            }
