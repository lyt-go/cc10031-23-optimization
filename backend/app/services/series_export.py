"""序列导出领域服务：维护时序结果按测点与时间范围的分片导出任务与导出水位。
按测点范围和时间窗口选择性导出归并数据，写入分片导出数据文件。

一致性机制
----------
- 导出请求先"受理"生成领取号（receipt），此时不读任何窗口数据；首次领取时在
  轮次目录锁内原子绑定"当前最新的完整已发布轮"，同一份交付的所有分片永远来自
  同一轮，更正中途不会混读半新半旧的窗口。
- 每个分片文件自带可复算信息：轮次号、窗口范围、实际来源、窗口值与累计值、
  内容校验值；凭旧领取号续领仍读绑定的旧轮，旧合计与来源对得上。
- 核对接口按轮次清单重算累计并逐分片校验，同时提示绑定轮是否已被新轮取代
  （旧轮不可变，分片仍可复算、仍适用）。
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService
from app.services.rollup_rounds import rollup_round_catalog
from app.store import store


class SeriesExportService(BaseModuleService):
    MODULE = "series_export"
    ENTITY = "导出任务"
    OPERATION_TERM = "导出"
    LIST_FIELDS = ["导出编号", "测点范围", "时间范围", "导出格式", "采样粒度", "记录条数", "分片大小", "导出水位"]
    REQUIRED_FIELDS = ["导出编号", "测点范围", "时间范围"]
    STATUS_ORDER = ["草稿", "待导出", "导出中", "已交付"]
    # 数据导出操作：按测点范围时间窗口导出降采样与归并结果到分片导出文件
    ACTION_RULES = {"启动导出": "导出中", "确认交付": "已交付", "标记失败": "已交付"}
    NEGATIVE_ACTIONS = []

    # -- 路径与回执持久化 --

    def _receipt_dir(self) -> Path:
        return self._data_dir("exports") / "receipts"

    def _receipt_path(self, claim_no: str) -> Path:
        return self._receipt_dir() / f"{claim_no}.json"

    def _save_receipt(self, receipt: dict[str, Any]) -> None:
        self._overwrite_json(self._receipt_path(receipt["claim_no"]), receipt)

    def _load_receipt(self, claim_no: str) -> dict[str, Any] | None:
        path = self._receipt_path(claim_no)
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            return None

    def _find_rollup_for_point(self, point: str) -> dict[str, Any] | None:
        rows = [r for r in store.rows("rollup_window") if str(r.get("测点编号", "")) == str(point)]
        if not rows:
            return None
        return max(rows, key=lambda r: int(r.get("id", 0)))

    # -- 兼容旧接口：整文件导出 --

    def export_series(self, export_id: int) -> dict[str, Any]:
        """按测点范围和时间窗口导出降采样与归并结果数据到分片导出交付文件。"""
        task = store.find(self.MODULE, export_id)
        if task is None:
            return {"error": f"导出任务 {export_id} 不存在"}
        self.run_action(export_id, "启动导出")
        point_range = task.get("测点范围", "")
        export_fmt = task.get("导出格式", "jsonl")

        # 扫描归并窗口和降采样数据文件导出（原子覆盖，避免重复导出时新旧拼接）
        exported = []
        rollup_dir = self._data_dir("rollup_windows")
        if rollup_dir.exists():
            for f in rollup_dir.glob("*_rollup.jsonl"):
                records = self._read_jsonl(f)
                for r in records:
                    exported.append({
                        "export_id": task["导出编号"],
                        # 数据产物输出：写入导出结果为分片交付JSONL数据文件
                        "source": "rollup_window",
                        "point": r.get("point", ""),
                        "window_index": r.get("window_start", 0),
                        "value": r.get("rollup_value", 0),
                        "metric": r.get("metric", ""),
                    })

        ds_dir = self._data_dir("downsampled")
        if ds_dir.exists():
            for f in ds_dir.glob("*_downsampled.jsonl"):
                records = self._read_jsonl(f)
                for r in records:
                    exported.append({
                        "export_id": task["导出编号"],
                        "source": "downsample",
                        "point": r.get("point", ""),
                        "window_index": r.get("window_index", 0),
                        "value": r.get("aggregated_value", 0),
                        "method": r.get("method", ""),
                    })

        # 将导出结果写入分片导出交付文件
        output_file = self._data_dir("exports") / f"{task['导出编号']}_export.{export_fmt}"
        written = self._overwrite_jsonl(output_file, exported)
        task["记录条数"] = str(written)
        task["分片大小"] = str(len(exported) * 80)
        task["导出水位"] = str(written)
        self.run_action(export_id, "确认交付")
        return {"export_id": task["导出编号"], "format": export_fmt,
                "records_exported": written, "total_size_estimate": len(exported) * 80}

    def export_by_time_window(self, point_range: str, time_window: str,
                               fmt: str = "jsonl") -> dict[str, Any]:
        """按指定时间窗口和测点范围创建导出任务并输出交付数据。"""
        tid = max((int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0) + 1
        self._auto_create_seed(self.MODULE, {
            "导出编号": f"EXPORT-{point_range}-{tid}",
            "测点范围": point_range,
            "时间范围": time_window,
            "导出格式": fmt,
            "采样粒度": "1h",
            "记录条数": "0",
            "分片大小": "0",
            "导出水位": "0",
        })
        return self.export_series(tid)

    # -- 领取式分片导出（轮次一致、可复算、可续领） --

    def accept_export_request(self, export_id: int, shard_size: int = 1) -> dict[str, Any]:
        """受理导出请求：生成领取号，此刻不绑定任何轮次，首次领取时再原子绑定。"""
        task = store.find(self.MODULE, export_id)
        if task is None:
            return {"error": f"导出任务 {export_id} 不存在"}
        shard_size = max(1, int(shard_size))
        point = str(task.get("测点范围", ""))
        rollup_entry = self._find_rollup_for_point(point)
        seq = max(
            (int(p.stem.split("-")[-1]) for p in self._receipt_dir().glob("CLAIM-*.json")),
            default=0,
        ) + 1
        claim_no = f"CLAIM-{seq:06d}"
        receipt = {
            "claim_no": claim_no,
            "export_id": export_id,
            "export_key": task["导出编号"],
            "point": point,
            "rollup_key": rollup_entry["窗口编号"] if rollup_entry else None,
            "round_no": None,  # 首次领取时在轮次锁内绑定
            "status": "待领取",
            "shard_size": shard_size,
            "next_window_index": 1,
            "total_windows": 0,
            "shards": [],
            "accepted_at": time.time(),
        }
        self._save_receipt(receipt)
        return {"claim_no": claim_no, "export_id": export_id, "point": point,
                "rollup_key": receipt["rollup_key"], "shard_size": shard_size,
                "status": receipt["status"]}

    def claim_export_shard(self, claim_no: str) -> dict[str, Any]:
        """凭领取号领取下一个分片；首次领取原子绑定当前最新完整轮次。"""
        receipt = self._load_receipt(claim_no)
        if receipt is None:
            return {"error": f"领取号 {claim_no} 不存在"}
        rollup_key = receipt.get("rollup_key")
        if not rollup_key:
            return {"error": f"领取号 {claim_no} 找不到测点 {receipt.get('point')} 的归并窗口"}

        # 与轮次发布共用同一把锁：领取绑定的要么是完整旧轮、要么是完整新轮。
        with rollup_round_catalog.locked():
            if receipt["round_no"] is None:
                round_no = rollup_round_catalog.latest_round_no(rollup_key)
                if round_no is None:
                    return {"error": f"归并窗口 {rollup_key} 尚无已发布轮次，暂不能领取"}
                receipt["round_no"] = round_no
                manifest = rollup_round_catalog.get_manifest(rollup_key, round_no)
                receipt["total_windows"] = int(manifest["window_count"])
                receipt["status"] = "领取中"
            else:
                round_no = receipt["round_no"]
                manifest = rollup_round_catalog.get_manifest(rollup_key, round_no)
                if manifest is None:
                    return {"error": f"绑定轮次 {round_no} 已不存在，无法续领"}

            cumulative = rollup_round_catalog.get_cumulative(rollup_key, round_no)
            start = int(receipt["next_window_index"])
            end = min(start + int(receipt["shard_size"]) - 1, len(cumulative))
            window = [r for r in cumulative
                      if start <= int(r.get("window_index", 0)) <= end]
            if not window:
                receipt["status"] = "已领完"
                self._save_receipt(receipt)
                return {"claim_no": claim_no, "round_no": round_no, "shards": receipt["shards"],
                        "status": "已领完", "message": "该领取号下的分片已全部领取"}

            shard_no = len(receipt["shards"]) + 1
            records = [{
                "claim_no": claim_no,
                "export_id": receipt["export_id"],
                "source": "rollup_round",
                "rollup_key": rollup_key,
                "round_no": round_no,
                "point": r.get("point", ""),
                "window_index": r.get("window_index"),
                "window_start": r.get("window_start"),
                "window_end": r.get("window_end"),
                "window_value": r.get("window_value"),
                "cumulative_value": r.get("cumulative_value"),
                "metric": r.get("metric", ""),
            } for r in window]
            checksum = self._canonical_checksum(records)
            shard_file = (
                self._data_dir("exports")
                / f"{receipt['export_key']}_{claim_no}_shard-{shard_no}.jsonl"
            )
            self._overwrite_jsonl(shard_file, records)

            shard_meta = {
                "shard_no": shard_no,
                "file": str(shard_file),
                "round_no": round_no,
                "window_start": start,
                "window_end": end,
                "record_count": len(records),
                "checksum": checksum,
                "source_files": list(manifest.get("source_files", [])),
                "delivered_at": time.time(),
            }
            receipt["shards"].append(shard_meta)
            receipt["next_window_index"] = end + 1
            if receipt["next_window_index"] > len(cumulative):
                receipt["status"] = "已领完"
            self._save_receipt(receipt)

        return {
            "claim_no": claim_no,
            "round_no": round_no,
            "shard": shard_meta,
            "records": records,
            "manifest": manifest,
            "next_window_index": receipt["next_window_index"],
            "total_windows": receipt["total_windows"],
            "status": receipt["status"],
            "latest_round_no": rollup_round_catalog.latest_round_no(rollup_key),
        }

    def verify_claim(self, claim_no: str) -> dict[str, Any]:
        """按绑定轮的清单复算累计序列，逐分片核对窗口范围、来源、校验值与累计值。"""
        receipt = self._load_receipt(claim_no)
        if receipt is None:
            return {"error": f"领取号 {claim_no} 不存在"}
        if receipt.get("round_no") is None:
            return {"ok": False, "claim_no": claim_no, "message": "尚未领取任何分片，无轮次可核对"}
        rollup_key = receipt["rollup_key"]
        round_no = receipt["round_no"]
        manifest = rollup_round_catalog.get_manifest(rollup_key, round_no)
        if manifest is None:
            return {"ok": False, "claim_no": claim_no,
                    "error": f"绑定轮次 {round_no} 缺失，无法复算"}

        rollup_records = rollup_round_catalog.get_rollup(rollup_key, round_no)
        running = 0.0
        recomputed: dict[int, float] = {}
        for r in rollup_records:
            running += float(r.get("rollup_value", 0))
            recomputed[int(r.get("window_index", 0))] = round(running, 4)

        shard_checks = []
        all_ok = True
        for shard in receipt["shards"]:
            path = Path(shard["file"])
            records = self._read_jsonl(path)
            file_ok = path.exists()
            checksum_ok = self._canonical_checksum(records) == shard["checksum"]
            same_round = all(int(r.get("round_no", -1)) == int(round_no) for r in records)
            range_ok = [int(r["window_index"]) for r in records] == list(
                range(shard["window_start"], shard["window_end"] + 1))
            cumulative_ok = all(
                float(r["cumulative_value"]) == recomputed.get(int(r["window_index"]))
                for r in records
            )
            ok = file_ok and checksum_ok and same_round and range_ok and cumulative_ok
            all_ok = all_ok and ok
            shard_checks.append({
                "shard_no": shard["shard_no"],
                "file": shard["file"],
                "file_exists": file_ok,
                "checksum_ok": checksum_ok,
                "same_round": same_round,
                "window_range_ok": range_ok,
                "cumulative_recomputed_ok": cumulative_ok,
                "window_start": shard["window_start"],
                "window_end": shard["window_end"],
                "round_no": round_no,
                "source_files": shard.get("source_files", []),
                "ok": ok,
            })

        latest_round = rollup_round_catalog.latest_round_no(rollup_key)
        return {
            "ok": all_ok,
            "claim_no": claim_no,
            "rollup_key": rollup_key,
            "pinned_round_no": round_no,
            "latest_round_no": latest_round,
            # 旧轮不可变：即使有更新轮，已交付分片仍可复算、仍适用，仅提示存在新轮。
            "stale": latest_round != round_no,
            "shard_checks": shard_checks,
            "recomputed_final_cumulative_value": round(running, 4),
            "manifest_final_cumulative_value": manifest.get("final_cumulative_value"),
            "manifest": manifest,
        }
