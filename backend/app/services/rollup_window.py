"""窗口归并领域服务：维护固定、滑动与会话窗口的时序归并聚合指标与窗口条数。
对降采样后的序列数据按窗口类型进行窗口归并计算，生成归并结果数据集。

一致性机制
----------
- 归并结果与累计序列除当前文件外，还以"不可变轮次"发布到轮次目录，每轮带清单
  （窗口范围、来源文件、校验值、更正父轮、首个变化窗口）。
- 当前文件用原子覆盖写，更正中途不会把新旧窗口拼在同一文件里；即便外部读到
  当前文件的中间态，已发布轮次仍保持整轮一致，导出按领取号绑定轮次读取。
- 更正时只重算从首个变化窗口起的后缀，前缀窗口原样保留，新轮末值由累计序列
  重算得出，旧轮文件不动，旧领取号仍能取到原轮合计。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.rollup_rounds import rollup_round_catalog
from app.store import store


class RollupWindowService(BaseModuleService):
    MODULE = "rollup_window"
    ENTITY = "归并窗口"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["窗口编号", "测点编号", "窗口类型", "窗口长度", "滑动步长", "聚合指标", "窗口条数", "归并水位"]
    REQUIRED_FIELDS = ["窗口编号", "测点编号", "窗口类型"]
    STATUS_ORDER = ["草稿", "待归并", "已归并", "已作废"]
    ACTION_RULES = {"启动归并": "待归并", "确认归并": "已归并", "标记作废": "已作废"}
    # 数据归并操作：按窗口归并规则执行聚合计算生成归并结果数据集
    NEGATIVE_ACTIONS = []

    # -- 内部工具 --

    def _current_rollup_file(self, rollup_key: str):
        return self._data_dir("rollup_windows") / f"{rollup_key}_rollup.jsonl"

    def _current_cumulative_file(self, rollup_key: str):
        return self._data_dir("rollup_windows") / f"{rollup_key}_cumulative.jsonl"

    def _load_source_records(self, entry: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
        """读取降采样（或过滤兜底）来源数据，返回记录与实际采用的来源文件名。"""
        point_key = entry["测点编号"]
        ds_dir = self._data_dir("downsampled")
        ds_files = list(ds_dir.glob(f"*{point_key}*_downsampled.jsonl")) if ds_dir.exists() else []
        records: list[dict[str, Any]] = []
        source_files: list[str] = []
        for f in ds_files[:3]:
            records.extend(self._read_jsonl(f))
            source_files.append(f.name)
        if not records:
            filtered_file = self._data_dir("filtered_series") / f"{point_key}_filtered.jsonl"
            records = self._read_jsonl(filtered_file)
            if records:
                source_files.append(filtered_file.name)
        return records, source_files

    @staticmethod
    def _aggregate(values: list[float], agg_metric: str) -> float:
        if agg_metric in ("求和", "sum"):
            return round(sum(values), 4)
        if agg_metric in ("最大值", "max"):
            return round(max(values), 4)
        return round(sum(values) / len(values), 4)

    def _build_rollup_records(self, entry: dict[str, Any],
                              records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        point_key = entry["测点编号"]
        window_type = entry.get("窗口类型", "固定窗口")
        window_len = int(entry.get("窗口长度", "5"))
        agg_metric = entry.get("聚合指标", "均值")
        merged: list[dict[str, Any]] = []
        for i in range(0, len(records), window_len):
            window = records[i:i + window_len]
            values: list[float] = []
            for r in window:
                v = r.get("aggregated_value", r.get("calibrated_value_V", 0))
                try:
                    values.append(float(v))
                except (ValueError, TypeError):
                    values.append(0.0)
            if not values:
                continue
            merged.append({
                "window_id": entry["窗口编号"],
                "point": point_key,
                "window_type": window_type,
                "window_start": i + 1,
                "window_end": i + len(window),
                "record_count": len(window),
                "rollup_value": self._aggregate(values, agg_metric),
                "metric": agg_metric,
            })
        # 统一补上 1 起始的窗口序号，轮次累计与导出分片都按它对齐。
        for idx, rec in enumerate(merged, start=1):
            rec["window_index"] = idx
        return merged

    def _publish(self, entry: dict[str, Any], rollup_records: list[dict[str, Any]],
                 source_files: list[str], *, correction_of: int | None = None,
                 changed_from: int | None = None, reason: str = "首次发布") -> dict[str, Any]:
        """发布不可变轮次，并把当前文件原子切换到该轮内容。"""
        rollup_key = entry["窗口编号"]
        manifest = rollup_round_catalog.publish_round(
            rollup_key,
            rollup_records,
            source_files=source_files,
            correction_of=correction_of,
            changed_from=changed_from,
            reason=reason,
            publisher=self,
        )
        round_no = manifest["round_no"]
        # 当前文件做原子覆盖：读者要么看到整轮旧版，要么看到整轮新版。
        self._overwrite_jsonl(self._current_rollup_file(rollup_key), rollup_records)
        self._overwrite_jsonl(
            self._current_cumulative_file(rollup_key),
            rollup_round_catalog.get_cumulative(rollup_key, round_no))
        entry["当前轮次"] = str(round_no)
        entry["末位累计值"] = str(manifest["final_cumulative_value"])
        entry["轮次清单"] = str(rollup_round_catalog.manifest_path(rollup_key, round_no))
        return manifest

    # -- 归并与累计 --

    def rollup_windows(self, rollup_id: int) -> dict[str, Any]:
        """按窗口归并规则对降采样后的时序数据执行聚合计算，生成归并结果数据集。"""
        entry = store.find(self.MODULE, rollup_id)
        if entry is None:
            return {"error": f"归并窗口 {rollup_id} 不存在"}
        self.run_action(rollup_id, "启动归并")
        records, source_files = self._load_source_records(entry)
        merged = self._build_rollup_records(entry, records)

        rollup_key = entry["窗口编号"]
        if rollup_round_catalog.latest_round_no(rollup_key) is None:
            # 首轮发布：固定该轮采用的来源与窗口值，后续更正只产生新轮，不改旧轮。
            self._publish(entry, merged, source_files, reason="首次归并发布")
        else:
            # 已有发布轮次时，普通重算只原子刷新当前文件，不静默改动历史轮次。
            self._overwrite_jsonl(self._current_rollup_file(rollup_key), merged)

        written = len(merged)
        entry["窗口条数"] = str(written)
        entry["归并水位"] = str(written)
        self.run_action(rollup_id, "确认归并")
        return {"rollup_id": rollup_key, "point": entry["测点编号"],
                "window_type": entry.get("窗口类型", "固定窗口"),
                "window_period_count": written,
                "metric": entry.get("聚合指标", "均值"),
                "round_no": rollup_round_catalog.latest_round_no(rollup_key),
                "source_files": source_files}

    def compute_cumulative_rollup(self, rollup_id: int) -> dict[str, Any]:
        """对归并窗口数据计算累计聚合指标写入累计快照数据文件。"""
        entry = store.find(self.MODULE, rollup_id)
        if entry is None:
            return {"error": f"归并窗口 {rollup_id} 不存在"}
        # 先执行归并（内部保证首轮已发布）
        rollup_result = self.rollup_windows(rollup_id)
        if "error" in rollup_result:
            return rollup_result
        rollup_key = entry["窗口编号"]
        round_no = rollup_round_catalog.latest_round_no(rollup_key)
        cumulative = rollup_round_catalog.get_cumulative(rollup_key, round_no)
        self._overwrite_jsonl(self._current_cumulative_file(rollup_key), cumulative)
        return {"rollup_id": rollup_key, "round_no": round_no,
                "cumulative_periods": len(cumulative),
                "final_cumulative_value": cumulative[-1]["cumulative_value"] if cumulative else 0.0}

    # -- 更正与重送 --

    def correct_rollup_window(self, rollup_id: int, window_index: int,
                              new_value: float, reason: str = "窗口更正") -> dict[str, Any]:
        """更正指定窗口的归并值并发布新轮：前缀原样保留，仅补算受影响后缀的累计。"""
        entry = store.find(self.MODULE, rollup_id)
        if entry is None:
            return {"error": f"归并窗口 {rollup_id} 不存在"}
        rollup_key = entry["窗口编号"]
        base_round = rollup_round_catalog.latest_round_no(rollup_key)
        if base_round is None:
            return {"error": f"归并窗口 {rollup_key} 尚未发布过轮次，无法更正"}
        records = rollup_round_catalog.get_rollup(rollup_key, base_round)
        target = next((r for r in records if int(r.get("window_index", 0)) == int(window_index)), None)
        if target is None:
            return {"error": f"窗口序号 {window_index} 超出已发布范围 1..{len(records)}"}

        old_value = float(target["rollup_value"])
        if float(new_value) == old_value:
            changed_from = None
        else:
            changed_from = int(window_index)
        target["rollup_value"] = round(float(new_value), 4)

        base_manifest = rollup_round_catalog.get_manifest(rollup_key, base_round) or {}
        manifest = self._publish(
            entry, records,
            source_files=base_manifest.get("source_files", []),
            correction_of=base_round,
            changed_from=changed_from,
            reason=reason,
        )
        entry["窗口条数"] = str(manifest["window_count"])
        entry["归并水位"] = str(manifest["window_count"])
        return {"rollup_id": rollup_key, "corrected_round": manifest["round_no"],
                "base_round": base_round, "window_index": int(window_index),
                "old_value": old_value, "new_value": target["rollup_value"],
                "changed_from": changed_from,
                "final_cumulative_value": manifest["final_cumulative_value"]}

    def republish_correction(self, rollup_id: int, reason: str = "更正重送") -> dict[str, Any]:
        """把最近一次更正再重送一轮：内容不变、轮次号递增，仍只补算原受影响后缀。"""
        entry = store.find(self.MODULE, rollup_id)
        if entry is None:
            return {"error": f"归并窗口 {rollup_id} 不存在"}
        rollup_key = entry["窗口编号"]
        latest_no = rollup_round_catalog.latest_round_no(rollup_key)
        latest_manifest = rollup_round_catalog.get_manifest(rollup_key, latest_no) if latest_no else None
        if not latest_manifest or latest_manifest.get("correction_of") is None:
            return {"error": f"归并窗口 {rollup_key} 没有可重送的更正轮"}
        records = rollup_round_catalog.get_rollup(rollup_key, latest_no)
        # 沿用原更正的首个变化窗口，后缀补算范围不扩大。
        manifest = self._publish(
            entry, records,
            source_files=latest_manifest.get("source_files", []),
            correction_of=latest_manifest.get("correction_of"),
            changed_from=latest_manifest.get("changed_from"),
            reason=reason,
        )
        return {"rollup_id": rollup_key, "republished_round": manifest["round_no"],
                "base_round": latest_no,
                "changed_from": manifest["changed_from"],
                "final_cumulative_value": manifest["final_cumulative_value"]}

    # -- 轮次查询（供导出与核对使用） --

    def list_rounds(self, rollup_id: int) -> dict[str, Any]:
        entry = store.find(self.MODULE, rollup_id)
        if entry is None:
            return {"error": f"归并窗口 {rollup_id} 不存在"}
        rounds = rollup_round_catalog.list_rounds(entry["窗口编号"])
        return {"rollup_id": entry["窗口编号"], "rounds": rounds,
                "latest_round_no": rounds[-1]["round_no"] if rounds else None}

    def get_round(self, rollup_id: int, round_no: int | None = None) -> dict[str, Any]:
        """取某一轮的完整内容（清单 + 窗口 + 累计），不传轮次号取最新已发布轮。"""
        entry = store.find(self.MODULE, rollup_id)
        if entry is None:
            return {"error": f"归并窗口 {rollup_id} 不存在"}
        rollup_key = entry["窗口编号"]
        if round_no is None:
            round_no = rollup_round_catalog.latest_round_no(rollup_key)
        manifest = rollup_round_catalog.get_manifest(rollup_key, round_no) if round_no else None
        if manifest is None:
            return {"error": f"归并窗口 {rollup_key} 的轮次 {round_no} 不存在"}
        return {
            "manifest": manifest,
            "rollup": rollup_round_catalog.get_rollup(rollup_key, round_no),
            "cumulative": rollup_round_catalog.get_cumulative(rollup_key, round_no),
        }
