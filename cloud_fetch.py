# -*- coding: utf-8 -*-
"""云端抓取脚本：GitHub Actions 每 30 分钟跑一次，产出 data.json。

为什么需要它：
  本机抓取只在程序开着的时候才进行。电脑一关，24 小时窗口里就空出一段，
  倒货榜靠窗口内的 MIN/MAX 算波动空间，采样一少，峰谷抓不到，分数就失真。
  云端按时抓、存最近 48 小时，本机启动时把「自己没抓到的那段时间」补进来。

⚠️ 两个硬约束（都是踩过或算过的）：
  1. 只保留 48 小时：655 个通货 × 96 个时间点，列式存储后约 2 MB，
     配合「单 commit 强推」，仓库体积恒定，不会一天天涨上去。
  2. 抓取必须复用 app.py 的 take_snapshot：口径和本机完全一致，
     不然补进去的数据和本地数据混在一起，算出来的波动是两套口径的混合，更糟。

输出格式（列式：时间戳只存一份、通货 id 只存一份）：
  {
    "v": 1, "generated": <ts>, "league": "...", "window_hours": 48,
    "app_version": "1.27.5",          ← 生成这份数据的 app.py 版本，客户端会校验
    "ts": [1696000000, ...],
    "items": {"<currency_id>": {"cat": "...",
                                "d": [...], "e": [...], "c": [...],
                                "o": [...], "s": [...]}}
  }
  数组与 ts 一一对应；缺值用 null。d/e/c = 神圣/崇高/混沌计价，o = 挂出量，s = 求购量。
"""
import json
import os
import pathlib
import sys

_HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
import app  # noqa: E402  —— 复用本机那套抓取逻辑，别另写一份

WINDOW_HOURS = 48
DATA_FILE = pathlib.Path(os.environ.get("POE2_CLOUD_DATA") or _HERE / "data.json")
LEAGUE = os.environ.get("POE2_LEAGUE") or "Forbidden Rites"

FIELDS = ("d", "e", "c", "o", "s")


def normalize(data: dict) -> int:
    """把每个通货的数组强制对齐到时间轴长度。返回修过的数组条数。

    ⚠️ 为什么会错位：data.json 是 Actions 一轮轮追加出来的，上一轮可能用的还是
    旧版 app.py、可能跑到一半失败、也可能两次推送撞车——都会留下个别通货的
    数组比 ts 短（2026-09-28 实测：650 个通货长度 3，另有 2 个长度 1）。
    不修的话客户端按 ts 下标取值直接 IndexError，一整轮补数据白跑。
    短了补 null，长了截断（多出来的没有对应时间点，留着也是错位）。
    """
    n = len(data.get("ts") or [])
    fixed = 0
    for item in (data.get("items") or {}).values():
        for key in FIELDS:
            arr = item.get(key) or []
            if len(arr) == n:
                continue
            item[key] = (list(arr) + [None] * n)[:n]
            fixed += 1
    return fixed


def load_existing() -> dict:
    """读上一轮留下的 data.json；没有就从空架子开始。"""
    if DATA_FILE.exists():
        try:
            data = json.loads(DATA_FILE.read_text(encoding="utf-8"))
            if isinstance(data.get("ts"), list) and isinstance(data.get("items"), dict):
                fixed = normalize(data)
                if fixed:
                    print(f"  · 上一轮有 {fixed} 条数组与时间轴错位，已对齐")
                return data
        except (ValueError, OSError) as exc:
            print(f"  · 既有 data.json 读不出来（{exc}），重新开始")
    return {"v": 1, "ts": [], "items": {}}


def _pad(item: dict, length: int) -> None:
    """新出现的通货要把数组补齐到当前长度，前面的时间点用 null 占位。"""
    for key in FIELDS:
        arr = item.setdefault(key, [])
        while len(arr) < length:
            arr.append(None)


def append_snapshot(data: dict, rows, ts: int | None = None) -> int:
    """把这一轮快照写进列式结构。返回本轮写入的条目数。

    ⚠️ ts 最好显式传进来：开了 skip_unchanged 之后，取到的行可能是「沿用上一次
    的值」（时间戳早于本轮），拿 rows[0]['ts'] 当本轮时间会把整条时间轴写歪。
    不传时按行内时间戳兜底（老调用方 / 旧测试就是这么传的）。
    """
    if not rows:
        return 0
    if ts is None:
        ts = int(dict(rows[0]).get("ts") or 0)
    ts = int(ts)
    if ts in data["ts"]:
        idx = data["ts"].index(ts)
    else:
        data["ts"].append(ts)
        idx = len(data["ts"]) - 1
    length = len(data["ts"])

    for row in rows:
        cid = row["currency_id"]
        item = data["items"].setdefault(cid, {"cat": row["category"] or ""})
        if not item.get("cat"):
            item["cat"] = row["category"] or ""
        _pad(item, length)
        item["d"][idx] = row["value_divine"]
        item["e"][idx] = row["value_exalted"]
        item["c"][idx] = row["value_chaos"]
        item["o"][idx] = row["orders"]
        item["s"][idx] = row["stock"]
    return len(rows)


def prune(data: dict, window_hours: int = WINDOW_HOURS) -> int:
    """裁到最近 N 小时；ts 数组和每个通货的数组必须同步裁，否则整列错位。"""
    if not data["ts"]:
        return 0
    cutoff = max(data["ts"]) - window_hours * 3600
    keep = [i for i, t in enumerate(data["ts"]) if t >= cutoff]
    dropped = len(data["ts"]) - len(keep)
    if dropped <= 0:
        return 0

    data["ts"] = [data["ts"][i] for i in keep]
    for item in data["items"].values():
        for key in FIELDS:
            arr = item.get(key) or []
            item[key] = [arr[i] if i < len(arr) else None for i in keep]
    # 时间轴全被裁掉的通货直接删掉，免得留一堆全是 null 的壳
    data["items"] = {
        cid: it for cid, it in data["items"].items()
        if any(any(v is not None for v in (it.get(k) or [])) for k in FIELDS)
    }
    return dropped


def main() -> int:
    app.setup_dirs()
    app.init_db()
    with app.STATE_LOCK:
        app.STATE["league"] = LEAGUE

    print(f"抓取 {LEAGUE} …")
    ts, count = app.take_snapshot(LEAGUE)

    # 每个通货取「时间戳不超过本轮」的最新一行。
    # 为什么不能写 `WHERE ts = ?` 精确限定：v1.27.7 起本机只在值变化时才写库，
    # 价格没动的通货这一轮根本没有新行，精确限定会取不到它们，
    # 列式数组里就出现空洞（客户端补数据时那一格是 null，等于白补一个时间点）。
    # 改成「取最近一行」后，没变化的通货会自然沿用上次的值填进本轮，数组与 ts 等长。
    # 同时排除 synthetic：那是 7 天日线反推的历史点，不能当本轮实测值。
    rows = app.db().execute(
        "SELECT currency_id, category, MAX(ts) AS mts, value_divine, value_exalted,"
        " value_chaos, orders, stock"
        " FROM snapshot WHERE league = ? AND ts <= ?"
        " AND (source IS NULL OR source != 'synthetic')"
        " GROUP BY currency_id",
        (LEAGUE, ts),
    ).fetchall()
    if not rows:
        print("  × 本轮没有取到任何行，保留原文件不动")
        return 1

    data = load_existing()
    written = append_snapshot(data, rows, ts)
    dropped = prune(data)
    normalize(data)          # 写盘前再兜一次，保证落盘的数组一定与 ts 等长
    data["v"] = 1
    data["generated"] = int(ts)
    data["league"] = LEAGUE
    data["window_hours"] = WINDOW_HOURS
    # ⚠️ 记下「这份数据是哪个版本的 app.py 抓的」。客户端会拿它跟自己的版本比，
    #    前两段不一致就拒绝补入——免得哪次忘了把新 app.py 推到 GitHub，
    #    云端一直用旧口径抓，补进来和本机数据混成两套口径。
    data["app_version"] = app.VERSION

    DATA_FILE.write_text(
        json.dumps(data, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    size_mb = DATA_FILE.stat().st_size / 1024 / 1024
    print(f"  ✓ 写入 {written} 条，裁掉 {dropped} 个过期时间点")
    print(f"  ✓ {DATA_FILE.name} {size_mb:.2f} MB  "
          f"时间轴 {len(data['ts'])} 点 / 通货 {len(data['items'])} 个")
    return 0


if __name__ == "__main__":
    sys.exit(main())
