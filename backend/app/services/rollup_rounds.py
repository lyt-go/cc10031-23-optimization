"""归并轮次目录：维护每个归并窗口不可变的发布轮次。

设计要点
--------
- 每一轮（round）一旦完整发布就不可变：窗口数据、累计序列和轮次清单各自落到
  独立文件，发布过程用临时文件 + 原子 rename，读者不会读到写了一半的轮次。
- 归并窗口正在更正时，当前文件可能半新半旧；已发布轮次不受影响，导出按领取号
  绑定的轮次号读取，保证同一份交付里的累计序列来自同一轮。
- 更正轮次只重算实际受影响的后缀（从第一个变化的窗口起），前缀窗口原样沿用，
  并在清单里记录 ``changed_from`` 与来源校验值，支持事后复算核对。
- 模块内用锁串行化发布与领取绑定，保证"完整新轮次发布后首次领取"拿到的是整轮
  新数据，而不是更正中间态。
"""
from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any

from app.services import base


class RollupRoundCatalog:
    def __init__(self) -> None:
        self._lock = threading.RLock()

    def locked(self):
        """对外暴露轮次锁：发布与首次领取绑定互斥，杜绝读到更正中间态。"""
        return self._lock

    # -- 路径 --

    @property
    def _root(self) -> Path:
        # 每次调用时读取 base.DATA_ROOT，兼容测试里对 DATA_ROOT 的 patch。
        return Path(base.DATA_ROOT) / "rollup_windows" / "rounds"

    def _dir(self, rollup_key: str) -> Path:
        return self._root / str(rollup_key)

    def rollup_path(self, rollup_key: str, round_no: int) -> Path:
        return self._dir(rollup_key) / f"round-{round_no}_rollup.jsonl"

    def cumulative_path(self, rollup_key: str, round_no: int) -> Path:
        return self._dir(rollup_key) / f"round-{round_no}_cumulative.jsonl"

    def manifest_path(self, rollup_key: str, round_no: int) -> Path:
        return self._dir(rollup_key) / f"round-{round_no}_manifest.json"

    # -- 查询 --

    def list_rounds(self, rollup_key: str) -> list[dict[str, Any]]:
        """返回某归并窗口已发布的全部轮次清单，按轮次号升序。"""
        manifests: list[dict[str, Any]] = []
        d = self._dir(rollup_key)
        if not d.exists():
            return manifests
        for mf in sorted(d.glob("round-*_manifest.json")):
            payload = _read_json(mf)
            if payload:
                manifests.append(payload)
        manifests.sort(key=lambda m: int(m.get("round_no", 0)))
        return manifests

    def latest_round_no(self, rollup_key: str) -> int | None:
        rounds = self.list_rounds(rollup_key)
        return int(rounds[-1]["round_no"]) if rounds else None

    def get_manifest(self, rollup_key: str, round_no: int) -> dict[str, Any] | None:
        mf = self.manifest_path(rollup_key, round_no)
        return _read_json(mf) if mf.exists() else None

    def get_rollup(self, rollup_key: str, round_no: int) -> list[dict[str, Any]]:
        return _read_jsonl(self.rollup_path(rollup_key, round_no))

    def get_cumulative(self, rollup_key: str, round_no: int) -> list[dict[str, Any]]:
        return _read_jsonl(self.cumulative_path(rollup_key, round_no))

    # -- 发布 --

    def publish_round(
        self,
        rollup_key: str,
        rollup_records: list[dict[str, Any]],
        *,
        source_files: list[str] | None = None,
        correction_of: int | None = None,
        changed_from: int | None = None,
        reason: str = "首次发布",
        publisher: Any = None,
    ) -> dict[str, Any]:
        """原子发布一个完整轮次：窗口数据 + 累计序列 + 轮次清单。

        ``publisher`` 为领域服务实例，提供原子写与校验值工具；不传时用基类静态方法。
        返回新轮次的清单。已发布轮次不可变，重复发布相同内容也会产生新轮次号。
        """
        publisher = publisher or _DefaultPublisher()
        with self._lock:
            existing = self.list_rounds(rollup_key)
            round_no = (max((int(m["round_no"]) for m in existing), default=0) + 1)
            parent_round = correction_of or (existing[-1]["round_no"] if existing else None)

            cumulative_records = _build_cumulative(rollup_records)
            window_count = len(rollup_records)
            final_value = (
                cumulative_records[-1]["cumulative_value"] if cumulative_records else 0.0
            )

            manifest = {
                "rollup_key": str(rollup_key),
                "round_no": round_no,
                "parent_round": parent_round,
                "correction_of": correction_of,
                "changed_from": changed_from,
                "reason": reason,
                "published_at": time.time(),
                "window_count": window_count,
                "window_range": {
                    "window_start": (
                        rollup_records[0].get("window_start", 1) if rollup_records else None
                    ),
                    "window_end": (
                        rollup_records[-1].get("window_end", window_count)
                        if rollup_records else None
                    ),
                },
                "metric": rollup_records[0].get("metric", "") if rollup_records else "",
                "point": rollup_records[0].get("point", "") if rollup_records else "",
                "final_cumulative_value": final_value,
                "source_files": list(source_files or []),
                "rollup_checksum": publisher._canonical_checksum(rollup_records),
                "cumulative_checksum": publisher._canonical_checksum(cumulative_records),
                "immutable": True,
            }

            d = self._dir(rollup_key)
            # 三个文件先各自原子落盘，最后发布清单；清单存在即代表整轮可见。
            publisher._overwrite_jsonl(self.rollup_path(rollup_key, round_no), rollup_records)
            publisher._overwrite_jsonl(
                self.cumulative_path(rollup_key, round_no), cumulative_records)
            publisher._overwrite_json(self.manifest_path(rollup_key, round_no), manifest)
            return manifest


def _build_cumulative(rollup_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """按窗口顺序重算累计序列，前缀变化时后缀累计值一并修正。"""
    cumulative: list[dict[str, Any]] = []
    running = 0.0
    for i, r in enumerate(rollup_records):
        val = float(r.get("rollup_value", 0))
        running += val
        cumulative.append({
            "window_index": int(r.get("window_index", i + 1)),
            "window_start": r.get("window_start", i + 1),
            "window_end": r.get("window_end", i + 1),
            "point": r.get("point", ""),
            "window_id": r.get("window_id", ""),
            "window_value": val,
            "cumulative_value": round(running, 4),
            "metric": r.get("metric", ""),
        })
    return cumulative


def _read_json(path: Path) -> dict[str, Any] | None:
    import json
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    import json
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return records


class _DefaultPublisher:
    """无领域服务实例时的兜底，直接委托基类的原子写与校验值工具。"""

    _overwrite_jsonl = staticmethod(base.BaseModuleService._overwrite_jsonl)
    _overwrite_json = staticmethod(base.BaseModuleService._overwrite_json)
    _canonical_checksum = staticmethod(base.BaseModuleService._canonical_checksum)


# 进程内单例：所有归并窗口的轮次目录共享一份注册表与锁。
rollup_round_catalog = RollupRoundCatalog()
