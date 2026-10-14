# -*- coding: utf-8 -*-
"""云端小时数据聚合：把每轮抓到的 rows 压成「每通货每小时的价」。

★ 为什么要单独一份（2026-10-14）：
  日内节律分析要求「24 小时连续覆盖」，但本机只在程序开着时记录 ——
  实测 8 天里3 天有空档（最大 8 小时），导致时段信号被采样偏差污染：
    · 每天各小时的数据量差异极大（2:00~7:00 连续多天为 0，19:00 后天天有）
    · 报告算出「最便宜 10:00 / 最贵 16:00」，那两个点恰好是采样最密的时段
    · 前半 vs 后半的 Spearman ρ 只有 +0.44，最贵时段从22 点漂到 16 点
  云端 24 小时都在跑，是唯一能填满 24 小时的办法。

数据格式（列式，与 data.json 同一套思路）：
  {
    "v": 1, "generated": <ts>, "league": "...", "window_hours": 24*14,
    "hours": [1759000000, ...],          ← 整点（UTC 秒）
    "items": {"<currency_id>": {"cat": "...", "e": [...], "d": [...], "c": [...]}}
  }
  数组与 hours 一一对应，缺值 null。
★ 为什么小时轴要对齐到 3600：每轮抓取落在哪个「小时」是随机的，
  不对齐会出现同一小时两条记录，聚合时要取中位数才有意义。
★ 为什么每小时取**中位数**而不是最后一次：同一小时里源站可能刷新过好几次，
  中位数对单次异常值不敏感（last 会被一次脏价带跑）。
"""
import json
import os
import pathlib
import time

HOUR_FILE = pathlib.Path(
    os.environ.get("POE2_CLOUD_HOURLY") or
    pathlib.Path(__file__).resolve().parent / "hourly.json")

HOUR = 3600
WINDOW_HOURS = 24 * 14          # 留 14 天：跨两个完整周才能分开时段/周内


def _hour(ts: int) -> int:
    return int(ts) // HOUR * HOUR


def load_existing() -> dict:
    """读回上一轮的小时文件；读不到就当空（首轮从零开始）。"""
    try:
        raw = json.loads(HOUR_FILE.read_text(encoding="utf-8"))
        if isinstance(raw, dict) and raw.get("items") and raw.get("hours"):
            raw.setdefault("v", 1)
            return raw
    except Exception:
        pass
    return {"v": 1, "hours": [], "items": {}}


def _median(vals: list) -> float | None:
    vals = sorted(v for v in vals if v is not None and v > 0)
    if not vals:
        return None
    n = len(vals)
    return vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2


def append_hour(data: dict, rows, ts: int) -> int:
    """把这一轮并进小时轴。同一个小时重复并入时取中位数。"""
    h = _hour(ts)
    axes = data["hours"]
    if h not in axes:
        # 时间轴必须递增（后面 normalize/prune 都按序处理）
        pos = len(axes)
        while pos > 0 and axes[pos - 1] > h:
            pos -= 1
        axes.insert(pos, h)
        for cid in data["items"]:
            it = data["items"][cid]
            for f in ("e", "d", "c"):
                it.setdefault(f, [])
                it[f].insert(pos, None)

    idx = axes.index(h)
    written = 0
    for r in rows:
        cid = r["currency_id"] if hasattr(r, "keys") else r[0]
        cat = (r["category"] if hasattr(r, "keys") else None) or "currency"
        e = r["value_exalted"] if hasattr(r, "keys") else None
        d = r["value_divine"] if hasattr(r, "keys") else None
        c = r["value_chaos"] if hasattr(r, "keys") else None

        it = data["items"].setdefault(cid, {"cat": cat, "e": [], "d": [], "c": []})
        it["cat"] = cat
        pad = idx - len(it["e"])
        if pad > 0:                       # 中间有空位先补 null
            for f in ("e", "d", "c"):
                it[f].extend([None] * pad)
        for f, v in (("e", e), ("d", d), ("c", c)):
            arr = it[f]
            if len(arr) <= idx:
                arr.extend([None] * (idx + 1 - len(arr)))
            cur = arr[idx]
            # 同一小时第二次并入：与已有值取中位数（对单次脏价不敏感）
            arr[idx] = _median([x for x in (cur, v) if x is not None])
        written += 1

    # ★ 本轮写入过时间轴之后，把**所有**通货（哪怕这一轮没出现过）的数组
    #   补齐到 len(hours)。理由：它们的货号是在这一刻才被发现的，
    #   而 hours 里已经有一批更早的时间点了 —— 不补齐的话这些格是空的，
    #   下次再往中间插时间点时索引全错（自检 §4 抓到的就是这条）。
    n_hours = len(data["hours"])
    for it in data["items"].values():
        for f in ("e", "d", "c"):
            arr = it.setdefault(f, [])
            if len(arr) < n_hours:
                arr.extend([None] * (n_hours - len(arr)))
    return written


def prune_hours(data: dict, window_hours: int | None = None) -> int:
    # ⚠️ 不要写成 `window_hours: int = WINDOW_HOURS`：默认参数在函数定义时
    #   就绑定了当时的值，之后改模块级 WINDOW_HOURS 不起作用（自检踩过）。
    if window_hours is None:
        window_hours = WINDOW_HOURS
    axes = data["hours"]
    if not axes:
        return 0
    cutoff = max(axes) - window_hours * HOUR
    keep = [i for i, t in enumerate(axes) if t >= cutoff]
    if len(keep) == len(axes):
        return 0
    dropped = len(axes) - len(keep)
    data["hours"] = [axes[i] for i in keep]
    for cid in list(data["items"]):
        it = data["items"][cid]
        for f in ("e", "d", "c"):
            arr = it.get(f) or []
            it[f] = [arr[i] if i < len(arr) else None for i in keep]
    return dropped


def normalize_hours(data: dict) -> int:
    """写盘前兜底：保证每个数组与 hours 等长（不然客户端会错位取值）。

    ⚠️ 旧版本写出的文件里可能有 "e": null（字段被显式写成 null 而不是 []），
      这里要一并兜住 —— 直接 len(None) 会直接抛异常打挂整轮云端抓取。
    """
    n = len(data["hours"])
    fixed = 0
    for cid, it in data["items"].items():
        if not isinstance(it, dict):
            data["items"][cid] = it = {}
        it.setdefault("cat", "currency")
        for f in ("e", "d", "c"):
            arr = it.get(f)
            if not isinstance(arr, list):     # None / 缺字段 / 别的类型，一律重来
                it[f] = [None] * n
                fixed += 1
                continue
            if len(arr) < n:
                arr.extend([None] * (n - len(arr)))
                fixed += 1
            elif len(arr) > n:
                del arr[n:]
                fixed += 1
    return fixed


def write_hourly(data: dict, rows, ts: int, league: str) -> dict:
    data.setdefault("items", {})
    data.setdefault("hours", [])
    written = append_hour(data, rows, ts)
    dropped = prune_hours(data)
    normalize_hours(data)
    data["v"] = 1
    data["generated"] = int(ts)
    data["league"] = league
    data["window_hours"] = WINDOW_HOURS
    HOUR_FILE.write_text(
        json.dumps(data, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8")
    return {"written": written, "dropped": dropped,
            "hours": len(data["hours"]), "items": len(data["items"])}


if __name__ == "__main__":
    print(f"模块：{__doc__.splitlines()[0]}")
    print(f"目标文件：{HOUR_FILE}")