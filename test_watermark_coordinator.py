"""多源水位协调器单元测试。"""

import os
import tempfile
import unittest
from datetime import datetime, timedelta

from watermark_coordinator import (
    EventType,
    MissingPolicy,
    SourceConfig,
    WatermarkCoordinator,
)

DAY = "2026-11-15"
NEXT_DAY = "2026-11-16"
T0 = datetime(2026, 11, 15, 12, 0, 0)


def make_coordinator(**overrides):
    configs = [
        SourceConfig("orders", allowed_lateness=timedelta(hours=6)),
        SourceConfig("payments", allowed_lateness=timedelta(hours=6)),
        SourceConfig("refunds", allowed_lateness=timedelta(hours=6)),
    ]
    for cfg in configs:
        if cfg.name in overrides:
            cfg.on_missing = overrides[cfg.name]
    return WatermarkCoordinator(configs)


def upload_complete(coord, source, partition, files, now, signal_id=None):
    """模拟一次完整上传:声明 -> 文件 -> 完成信号。"""
    events = []
    events += coord.declare_partition(source, partition, files, now)
    for f in files:
        events += coord.file_arrived(source, partition, f, now)
    events += coord.complete_signal(
        source, partition, signal_id or f"{source}-{partition}-sig", now
    )
    return events


def types(events):
    return [e.type for e in events]


class CompletenessTest(unittest.TestCase):
    def test_watermark_advances_when_all_sources_complete(self):
        coord = make_coordinator()
        events = []
        for src in ("orders", "payments", "refunds"):
            events += upload_complete(coord, src, DAY, ["f1", "f2"], T0)
        self.assertEqual(coord.watermark, DAY)
        self.assertIn(EventType.WATERMARK_ADVANCED, types(events))
        self.assertIn(EventType.BATCH_READY, types(events))

    def test_incomplete_source_blocks_watermark(self):
        coord = make_coordinator()
        upload_complete(coord, "orders", DAY, ["f1"], T0)
        upload_complete(coord, "payments", DAY, ["f1"], T0)
        self.assertIsNone(coord.watermark)

    def test_signal_before_last_file_is_not_complete(self):
        """完成信号先于最后一个文件:信号到齐前不算完整。"""
        coord = make_coordinator()
        upload_complete(coord, "orders", DAY, ["f1"], T0)
        upload_complete(coord, "payments", DAY, ["f1"], T0)
        coord.declare_partition("refunds", DAY, ["a", "b"], T0)
        coord.file_arrived("refunds", DAY, "a", T0)
        events = coord.complete_signal("refunds", DAY, "sig-1", T0)
        self.assertIsNone(coord.watermark)
        self.assertNotIn(EventType.WATERMARK_ADVANCED, types(events))
        events = coord.file_arrived("refunds", DAY, "b", T0)
        self.assertEqual(coord.watermark, DAY)
        self.assertIn(EventType.WATERMARK_ADVANCED, types(events))

    def test_file_gap_blocks_completeness(self):
        coord = make_coordinator()
        upload_complete(coord, "orders", DAY, ["f1"], T0)
        upload_complete(coord, "payments", DAY, ["f1"], T0)
        coord.declare_partition("refunds", DAY, ["a", "b", "c"], T0)
        coord.file_arrived("refunds", DAY, "a", T0)
        coord.file_arrived("refunds", DAY, "b", T0)
        coord.complete_signal("refunds", DAY, "sig-1", T0)
        self.assertIsNone(coord.watermark)

    def test_pending_batches_advance_in_order(self):
        """迟到的源补齐后,积压的多个批次按顺序连续推进。"""
        coord = make_coordinator()
        for day in (DAY, NEXT_DAY):
            upload_complete(coord, "orders", day, ["f1"], T0)
            upload_complete(coord, "payments", day, ["f1"], T0)
        upload_complete(coord, "refunds", NEXT_DAY, ["f1"], T0)
        self.assertIsNone(coord.watermark)
        events = upload_complete(coord, "refunds", DAY, ["f1"], T0)
        self.assertEqual(coord.watermark, NEXT_DAY)
        advanced = [
            e.partition
            for e in events
            if e.type is EventType.WATERMARK_ADVANCED
        ]
        self.assertEqual(advanced, [DAY, NEXT_DAY])


class LateDataTest(unittest.TestCase):
    def test_late_backfill_triggers_recompute_not_rollback(self):
        """迟到源补齐已合并分区:发重算通知,watermark 不回退。"""
        coord = make_coordinator()
        for src in ("orders", "payments", "refunds"):
            upload_complete(coord, src, DAY, ["f1"], T0)
        for src in ("orders", "payments", "refunds"):
            upload_complete(coord, src, NEXT_DAY, ["f1"], T0)
        self.assertEqual(coord.watermark, NEXT_DAY)
        events = coord.file_arrived("orders", DAY, "late-file", T0)
        self.assertIn(EventType.RECOMPUTE_REQUIRED, types(events))
        self.assertEqual(coord.watermark, NEXT_DAY)

    def test_late_complete_signal_on_merged_partition(self):
        coord = make_coordinator()
        for src in ("orders", "payments", "refunds"):
            upload_complete(coord, src, DAY, ["f1"], T0)
        events = coord.complete_signal("orders", DAY, "sig-new", T0)
        self.assertIn(EventType.RECOMPUTE_REQUIRED, types(events))
        self.assertEqual(coord.watermark, DAY)

    def test_watermark_never_moves_backward(self):
        coord = make_coordinator()
        for src in ("orders", "payments", "refunds"):
            upload_complete(coord, src, DAY, ["f1"], T0)
            upload_complete(coord, src, NEXT_DAY, ["f1"], T0)
        self.assertEqual(coord.watermark, NEXT_DAY)
        coord.cancel_upload("orders", DAY, T0)
        coord.file_arrived("refunds", DAY, "x", T0)
        self.assertEqual(coord.watermark, NEXT_DAY)


class DuplicateAndCancelTest(unittest.TestCase):
    def test_duplicate_complete_signal_ignored(self):
        coord = make_coordinator()
        coord.declare_partition("orders", DAY, ["f1"], T0)
        coord.file_arrived("orders", DAY, "f1", T0)
        coord.complete_signal("orders", DAY, "sig-1", T0)
        events = coord.complete_signal("orders", DAY, "sig-1", T0)
        self.assertEqual(types(events), [EventType.DUPLICATE_SIGNAL_IGNORED])

    def test_cancel_before_merge_resolves_partition(self):
        """未合并前取消:按无数据结案,不再阻塞 watermark。"""
        coord = make_coordinator()
        upload_complete(coord, "orders", DAY, ["f1"], T0)
        upload_complete(coord, "payments", DAY, ["f1"], T0)
        events = coord.cancel_upload("refunds", DAY, T0)
        self.assertIn(EventType.UPLOAD_CANCELLED, types(events))
        self.assertEqual(coord.watermark, DAY)

    def test_cancel_after_merge_triggers_correction(self):
        """已合并后取消:发更正通知,watermark 不动。"""
        coord = make_coordinator()
        for src in ("orders", "payments", "refunds"):
            upload_complete(coord, src, DAY, ["f1"], T0)
        events = coord.cancel_upload("refunds", DAY, T0)
        self.assertIn(EventType.CORRECTION_REQUIRED, types(events))
        self.assertEqual(coord.watermark, DAY)


class CrossDayAndMissingTest(unittest.TestCase):
    def test_cross_day_arrival_within_lateness(self):
        """跨日边界:次日到达但在允许延迟内,不算超时。"""
        coord = make_coordinator()
        upload_complete(coord, "orders", DAY, ["f1"], T0)
        upload_complete(coord, "payments", DAY, ["f1"], T0)
        late = datetime(2026, 11, 16, 4, 0, 0)
        events = upload_complete(coord, "refunds", DAY, ["f1"], late)
        self.assertEqual(coord.watermark, DAY)
        self.assertNotIn(EventType.SOURCE_OVERDUE, types(events))

    def test_missing_source_blocks_and_reports_overdue(self):
        """长时间缺源(block 策略):发超时事件,watermark 停住。"""
        coord = make_coordinator()
        upload_complete(coord, "orders", DAY, ["f1"], T0)
        upload_complete(coord, "payments", DAY, ["f1"], T0)
        coord.declare_partition("refunds", DAY, ["f1"], T0)
        after_deadline = datetime(2026, 11, 16, 7, 0, 0)
        events = coord.tick(after_deadline)
        self.assertIn(EventType.SOURCE_OVERDUE, types(events))
        self.assertIsNone(coord.watermark)
        events = coord.tick(after_deadline + timedelta(hours=1))
        self.assertNotIn(EventType.SOURCE_OVERDUE, types(events))
        upload_complete(coord, "refunds", DAY, ["f1"], after_deadline)
        self.assertEqual(coord.watermark, DAY)

    def test_missing_source_skip_policy(self):
        """长时间缺源(skip 策略):按缺数跳过,watermark 继续推进。"""
        coord = make_coordinator(refunds=MissingPolicy.SKIP)
        upload_complete(coord, "orders", DAY, ["f1"], T0)
        upload_complete(coord, "payments", DAY, ["f1"], T0)
        coord.declare_partition("refunds", DAY, ["f1"], T0)
        events = coord.tick(datetime(2026, 11, 16, 7, 0, 0))
        self.assertIn(EventType.SOURCE_SKIPPED, types(events))
        self.assertEqual(coord.watermark, DAY)

    def test_optional_source_does_not_block(self):
        configs = [
            SourceConfig("orders"),
            SourceConfig("payments"),
            SourceConfig("refunds", required=False),
        ]
        coord = WatermarkCoordinator(configs)
        upload_complete(coord, "orders", DAY, ["f1"], T0)
        upload_complete(coord, "payments", DAY, ["f1"], T0)
        self.assertEqual(coord.watermark, DAY)


class PersistenceTest(unittest.TestCase):
    def test_restart_recovers_state(self):
        """进程重启:状态从磁盘恢复,watermark 与分区进度不丢。"""
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "state.json")
            configs = [
                SourceConfig(n) for n in ("orders", "payments", "refunds")
            ]
            coord = WatermarkCoordinator(configs, state_path=path)
            for src in ("orders", "payments", "refunds"):
                upload_complete(coord, src, DAY, ["f1"], T0)
            upload_complete(coord, "orders", NEXT_DAY, ["f1"], T0)
            self.assertEqual(coord.watermark, DAY)
            restored = WatermarkCoordinator(configs, state_path=path)
            self.assertEqual(restored.watermark, DAY)
            state = restored.partition_state("orders", NEXT_DAY)
            self.assertTrue(state.is_complete)
            upload_complete(restored, "payments", NEXT_DAY, ["f1"], T0)
            upload_complete(restored, "refunds", NEXT_DAY, ["f1"], T0)
            self.assertEqual(restored.watermark, NEXT_DAY)

    def test_restart_preserves_duplicate_signal_dedup(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "state.json")
            configs = [
                SourceConfig(n) for n in ("orders", "payments", "refunds")
            ]
            coord = WatermarkCoordinator(configs, state_path=path)
            coord.declare_partition("orders", DAY, ["f1"], T0)
            coord.complete_signal("orders", DAY, "sig-1", T0)
            restored = WatermarkCoordinator(configs, state_path=path)
            events = restored.complete_signal("orders", DAY, "sig-1", T0)
            self.assertEqual(
                types(events), [EventType.DUPLICATE_SIGNAL_IGNORED]
            )


if __name__ == "__main__":
    unittest.main()
