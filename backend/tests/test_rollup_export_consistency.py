"""累计归并与序列导出的结果一致性场景测试。

场景（三个连续窗口，原贡献 10/20/30，第二窗更正为 25）：
1. 第一份累计交付先领取首片；
2. 另一请求在更正前受理、在完整新轮发布后首次领取，必须整轮来自新轮，
   累计序列为 10/35/65，而不是混轮的 10/30/65；
3. 用旧领取号续领，仍读到原轮，末值可复算为 60；
4. 更正重送一次后重新下载，只补算受影响后缀，新轮末值仍为 65。
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def setup_data_dir():
    with tempfile.TemporaryDirectory() as tmp:
        with patch("app.services.base.DATA_ROOT", Path(tmp)):
            # rollup_rounds 通过 app.services.base.DATA_ROOT 动态取根目录，
            # 这里同时 patch 各服务模块内引用的 DATA_ROOT，保持与既有测试一致。
            import app.services.rollup_window as rw
            import app.services.series_export as se
            rw.DATA_ROOT = Path(tmp)
            se.DATA_ROOT = Path(tmp)
            yield Path(tmp)


def _write_downsampled(root: Path, point: str, values: list[float]) -> None:
    ds_dir = root / "downsampled"
    ds_dir.mkdir(parents=True, exist_ok=True)
    records = [
        {"task_id": f"DOWN-{point}", "point": point, "window_index": i + 1,
         "aggregated_value": float(v), "method": "求和", "target_freq": "1h"}
        for i, v in enumerate(values)
    ]
    with open(ds_dir / f"DOWN-{point}_downsampled.jsonl", "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _make_rollup(point: str) -> int:
    from app.services.rollup_window import RollupWindowService
    from app.store import store

    rid = max((int(r.get("id", 0)) for r in store.rows("rollup_window")), default=0) + 1
    store.rows("rollup_window").append({
        "id": rid, "窗口编号": f"ROLL-{point}", "测点编号": point,
        "窗口类型": "固定窗口", "窗口长度": "1", "滑动步长": "1",
        "聚合指标": "求和", "窗口条数": "0", "归并水位": "0",
        "status": "待归并", "pending": True, "abnormal": False,
    })
    svc = RollupWindowService()
    result = svc.compute_cumulative_rollup(rid)
    assert "error" not in result
    return rid


def _make_export_task(point: str) -> int:
    from app.services.series_export import SeriesExportService
    from app.store import store

    eid = max((int(r.get("id", 0)) for r in store.rows("series_export")), default=0) + 1
    store.rows("series_export").append({
        "id": eid, "导出编号": f"EXPORT-{point}-{eid}", "测点范围": point,
        "时间范围": "2026-09-01", "导出格式": "jsonl", "采样粒度": "1h",
        "记录条数": "0", "分片大小": "0", "导出水位": "0",
        "status": "待导出", "pending": True, "abnormal": False,
    })
    return eid


class TestRollupExportConsistency:
    def test_full_correction_claim_resend_scenario(self, setup_data_dir):
        from app.services.rollup_window import RollupWindowService
        from app.services.series_export import SeriesExportService
        from app.services.rollup_rounds import rollup_round_catalog

        root = setup_data_dir
        point = "SENS-C"
        rollup_key = f"ROLL-{point}"
        _write_downsampled(root, point, [10, 20, 30])
        rid = _make_rollup(point)
        rw_svc = RollupWindowService()
        ex_svc = SeriesExportService()

        # --- 首轮：窗口 10/20/30，累计 10/30/60 ---
        round1 = rw_svc.get_round(rid, 1)
        assert [r["rollup_value"] for r in round1["rollup"]] == [10.0, 20.0, 30.0]
        assert [r["cumulative_value"] for r in round1["cumulative"]] == [10.0, 30.0, 60.0]
        assert round1["manifest"]["source_files"] == [f"DOWN-{point}_downsampled.jsonl"]
        assert round1["manifest"]["window_count"] == 3

        # --- 第一份累计交付：受理后领取首片（窗口1，累计 10） ---
        eid1 = _make_export_task(point)
        accept1 = ex_svc.accept_export_request(eid1, shard_size=1)
        claim_a = accept1["claim_no"]
        shard_a1 = ex_svc.claim_export_shard(claim_a)
        assert shard_a1["round_no"] == 1
        assert shard_a1["records"][0]["window_index"] == 1
        assert shard_a1["records"][0]["window_value"] == 10.0
        assert shard_a1["records"][0]["cumulative_value"] == 10.0
        # 分片自带可复算信息
        meta = shard_a1["shard"]
        assert meta["window_start"] == 1 and meta["window_end"] == 1
        assert meta["source_files"] == [f"DOWN-{point}_downsampled.jsonl"]
        assert meta["checksum"]

        # --- 第二个请求在更正前受理（此刻还没有任何分片、也未绑定轮次） ---
        eid2 = _make_export_task(point)
        accept2 = ex_svc.accept_export_request(eid2, shard_size=3)
        claim_b = accept2["claim_no"]
        receipt_b_before = ex_svc._load_receipt(claim_b)
        assert receipt_b_before["round_no"] is None

        # --- 第二窗 20 -> 25，发布完整新轮，累计 10/35/65 ---
        correction = rw_svc.correct_rollup_window(rid, window_index=2, new_value=25,
                                                  reason="第二窗来源更正")
        assert correction["base_round"] == 1
        assert correction["corrected_round"] == 2
        assert correction["changed_from"] == 2  # 只从第二窗起补算后缀
        round2 = rw_svc.get_round(rid, 2)
        assert [r["rollup_value"] for r in round2["rollup"]] == [10.0, 25.0, 30.0]
        assert [r["cumulative_value"] for r in round2["cumulative"]] == [10.0, 35.0, 65.0]
        assert round2["manifest"]["correction_of"] == 1
        assert round2["manifest"]["final_cumulative_value"] == 65.0

        # 旧轮文件不可变：旧合计与来源仍对得上
        old_cum = rollup_round_catalog.get_cumulative(rollup_key, 1)
        assert [r["cumulative_value"] for r in old_cum] == [10.0, 30.0, 60.0]

        # --- 新请求在完整新轮发布后首次领取：整轮来自同一轮（轮次2） ---
        shard_b1 = ex_svc.claim_export_shard(claim_b)
        assert shard_b1["round_no"] == 2, "首次领取必须绑定更正后的完整新轮"
        rounds_in_shard = {r["round_no"] for r in shard_b1["records"]}
        assert rounds_in_shard == {2}, "同一领取里的累计序列必须来自同一轮"
        assert [r["cumulative_value"] for r in shard_b1["records"]] == [10.0, 35.0, 65.0]
        assert [r["window_value"] for r in shard_b1["records"]] == [10.0, 25.0, 30.0]
        verify_b = ex_svc.verify_claim(claim_b)
        assert verify_b["ok"] is True
        assert verify_b["stale"] is False

        # --- 用旧领取号续领：仍按原轮（轮次1）交付，末值可复算为 60 ---
        shard_a2 = ex_svc.claim_export_shard(claim_a)
        assert shard_a2["round_no"] == 1
        assert shard_a2["records"][0]["window_index"] == 2
        assert shard_a2["records"][0]["cumulative_value"] == 30.0
        shard_a3 = ex_svc.claim_export_shard(claim_a)
        assert shard_a3["round_no"] == 1
        assert shard_a3["records"][0]["window_index"] == 3
        assert shard_a3["records"][0]["cumulative_value"] == 60.0
        verify_a = ex_svc.verify_claim(claim_a)
        assert verify_a["ok"] is True
        assert verify_a["pinned_round_no"] == 1
        assert verify_a["stale"] is True, "已有新轮，但旧轮不可变、分片仍适用"
        assert verify_a["recomputed_final_cumulative_value"] == 60.0
        assert len(verify_a["shard_checks"]) == 3
        assert all(c["same_round"] and c["checksum_ok"]
                   and c["cumulative_recomputed_ok"] and c["window_range_ok"]
                   for c in verify_a["shard_checks"])
        # 续领结束
        again = ex_svc.claim_export_shard(claim_a)
        assert again["status"] == "已领完"

        # --- 更正重送一次：只补算原受影响后缀，新轮末值仍为 65 ---
        resend = rw_svc.republish_correction(rid)
        assert resend["republished_round"] == 3
        assert resend["base_round"] == 2
        assert resend["changed_from"] == 2, "重送不扩大后缀补算范围"
        assert resend["final_cumulative_value"] == 65.0
        round3 = rw_svc.get_round(rid, 3)
        assert [r["cumulative_value"] for r in round3["cumulative"]] == [10.0, 35.0, 65.0]
        # 第一窗未受影响：轮次2与轮次3的前缀窗口值与累计值完全一致
        assert round3["rollup"][0] == round2["rollup"][0]
        assert round3["cumulative"][0] == round2["cumulative"][0]

        # --- 重送后重新发起下载：绑定最新轮次3 ---
        eid3 = _make_export_task(point)
        claim_c = ex_svc.accept_export_request(eid3, shard_size=3)["claim_no"]
        shard_c = ex_svc.claim_export_shard(claim_c)
        assert shard_c["round_no"] == 3
        assert [r["cumulative_value"] for r in shard_c["records"]] == [10.0, 35.0, 65.0]
        verify_c = ex_svc.verify_claim(claim_c)
        assert verify_c["ok"] is True and verify_c["stale"] is False

        # --- 轮次目录里三轮清单齐全且不可变 ---
        rounds = rw_svc.list_rounds(rid)["rounds"]
        assert [m["round_no"] for m in rounds] == [1, 2, 3]
        assert [m["final_cumulative_value"] for m in rounds] == [60.0, 65.0, 65.0]

    def test_correction_midway_export_never_mixes_rounds(self, setup_data_dir):
        """归并窗口正在被更正时，领取要么整轮旧值要么整轮新值，不会半新半旧。"""
        from app.services.rollup_window import RollupWindowService
        from app.services.series_export import SeriesExportService

        point = "SENS-D"
        _write_downsampled(setup_data_dir, point, [10, 20, 30])
        rid = _make_rollup(point)
        rw_svc = RollupWindowService()
        ex_svc = SeriesExportService()

        eid = _make_export_task(point)
        claim = ex_svc.accept_export_request(eid, shard_size=1)["claim_no"]

        # 更正发布是原子的：发布前后领取都只会绑定完整轮
        first = ex_svc.claim_export_shard(claim)
        assert first["round_no"] == 1
        rw_svc.correct_rollup_window(rid, 2, 25)
        second = ex_svc.claim_export_shard(claim)
        assert second["round_no"] == 1, "已绑定轮次的续领不得漂移到新轮"
        assert second["records"][0]["window_index"] == 2
        assert second["records"][0]["window_value"] == 20.0
        assert second["records"][0]["cumulative_value"] == 30.0

        # 新领取号看到的是完整新轮，而不是混轮 10/30/65
        eid2 = _make_export_task(point)
        claim2 = ex_svc.accept_export_request(eid2, shard_size=3)["claim_no"]
        new_shard = ex_svc.claim_export_shard(claim2)
        assert new_shard["round_no"] == 2
        assert [r["cumulative_value"] for r in new_shard["records"]] == [10.0, 35.0, 65.0]

    def test_legacy_full_export_uses_atomic_overwrite(self, setup_data_dir):
        """旧的整文件导出重复执行不拼接，且当前累计文件为覆盖写。"""
        from app.services.series_export import SeriesExportService

        point = "SENS-E"
        _write_downsampled(setup_data_dir, point, [10, 20, 30])
        _make_rollup(point)
        eid = _make_export_task(point)
        svc = SeriesExportService()
        first = svc.export_series(eid)
        second = svc.export_series(eid)
        assert first["records_exported"] == second["records_exported"] == 6  # 归并3 + 降采样3
        lines = (setup_data_dir / "exports" / f"EXPORT-{point}-{eid}_export.jsonl").read_text(
            encoding="utf-8").strip().split("\n")
        assert len(lines) == 6
