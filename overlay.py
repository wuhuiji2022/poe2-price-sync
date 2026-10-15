# -*- coding: utf-8 -*-
"""poeoverlay.com 取数模块（第四个数据源）。

★ 为什么不需要浏览器（2026-10-14 实测推翻了原先的 CDP 方案）
  poeoverlay 是 Next.js App Router，页面没有独立 JSON API，价全部服务端渲染进
  RSC flight payload。只要带 `RSC: 1` 头直接 GET 详情页，就能拿到一个 ~100KB 的
  flight 流，里面有：

      {"itemId":"divine","itemName":"Divine Orb","series":[
        {"currencyId":"chaos",  "entries":[{"price":..,"stock":..,"timestamp":..}, ...]},
        {"currencyId":"annul",  "entries":[...]},
        {"currencyId":"exalted","entries":[...]}]}

  也就是说：**一次请求 = 3~4 个基准的现价 + 40 天日线 + 挂出量**。
  单页 1.4s（CDP 方案要 4.6s 且要常驻一个 headless Edge）。

★ 三个关键实测结论
  1. series 的末点 == 页面显示的 Current price（divine 末点 10.4707，页面 10.47）。
  2. 基准集合是 divine/chaos/exalted/annul 四个，每个通货给出**除自己以外**的 3 个
     （自己是 1，不需要给）。所以用户要的「混沌/崇高/神圣都参与差价」天然满足。
  3. slug = 英文名 kebab 化（`Orb of Annulment` → `orb-of-annulment`），
     实测 16/16 命中。分类目录从分类列表页的 payload 里取，不用猜。
     不存在的 slug 返回 404，可以据此跳过。

★ 它跟另外三个源的本质区别（决定了它能做「跨基准套利」而别的源不行）
  doe / scout / ninja 每个通货只给**一个**价，value_chaos / value_divine 都是拿这一个价
  除汇率尺子折出来的 —— 同轮各通货的 divine/exalted 比值恒定，所以跨基准差价是伪信号
  （v1.28.4 做过又删掉）。Overlay 直接读游戏内 Currency Exchange 的多组挂单，
  每个 (通货, 基准) 配对是**独立市场**：
      1 divine = 10.4707 chaos   = 704.381 exalted
      1 chaos  = 71.4771 exalted
      → 10.4707 × 71.4771 = 748.4 ≠ 704.381，差 6.25%
  这是真实挂单差价，不是折算误差。

只读依赖：urllib（标准库）。不引入任何第三方包 —— 便携包有 100MiB 硬上限。
"""
from __future__ import annotations

import json
import re
import sqlite3
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

SITE = "https://www.poeoverlay.com/market-history/poe2"
_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36 Edg/155.0.4283.45")
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))

# 抓取范围（Overlay 的 URL 分类 slug）。实测条目数：
#   currency 38 / essences 82 / ritual 39 / breach 29 / fragments 23 → 211 项
#   （全量 14 个分类是 668 项，要扩直接往这个列表里加）
DEFAULT_CATEGORIES = ["currency", "essences", "ritual", "breach", "fragments"]

# Overlay 提供的计价基准。用户要的三个都在里面，annul 是白送的。
BASES = ("divine", "chaos", "exalted", "annul")

# ★ 但参与「跨基准差价」计算的只有这三个，annul 不参与。
#   原因（2026-10-14 实测）：annul 是个流动性很差的配对，报价经常离谱 ——
#   transmute 在 annul 上的挂出量直接是 0，greater-orb-of-augmentation 折出来
#   0.474 而 chaos/exalted 两路只有 0.03~0.06。把它算进来会把 vaal 的差价
#   从 2.7% 抬到 21.4%，整张榜被一个没人用的市场带偏。
ARB_BASES = ("divine", "chaos", "exalted")

# 日线桶会持续修订（今天那一格在 UTC 日切前一直在变），
# 所以每轮把「已存的最大 ts」那一格重写一遍，而不是只写严格更新的点。
REWRITE_LAST_POINT = True
RETENTION_DAYS = 45

DEFAULT_TIMEOUT = 30
DEFAULT_WORKERS = 4


def kebab(name: str) -> str:
    """英文名 → Overlay slug：`Orb of Annulment` → `orb-of-annulment`。"""
    s = (name or "").lower().replace("'", "").replace("\u2019", "")
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


def _get(url: str, timeout: int = DEFAULT_TIMEOUT, retries: int = 2):
    """带 RSC 头直接 GET。返回 (status, text)；失败返回 (0, 原因)。"""
    last: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": _UA, "Accept": "*/*", "RSC": "1"})
            with _OPENER.open(req, timeout=timeout) as r:
                return r.status, r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return 404, ""
            last = exc
        except Exception as exc:  # noqa: BLE001 - 网络层统一兜底
            last = exc
        time.sleep(1.0 * (attempt + 1))
    return 0, repr(last)


def json_at(text: str, key: str):
    """从 flight 流里按 key 做括号配对，切出完整的 JSON 片段。

    ★ 不能用正则：payload 里嵌套很深，`"categories":[...]` 内部还有数组。
    """
    i = text.find(key)
    if i < 0:
        return None
    i += len(key)
    while i < len(text) and text[i] in " \t\r\n":
        i += 1
    if i >= len(text) or text[i] not in "[{":
        return None
    op, cl = ("[", "]") if text[i] == "[" else ("{", "}")
    depth, in_str, esc, j = 0, False, False, i
    while j < len(text):
        ch = text[j]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == op:
                depth += 1
            elif ch == cl:
                depth -= 1
                if depth == 0:
                    return text[i:j + 1]
        j += 1
    return None


def fetch_catalog(league: str, category: str, timeout: int = DEFAULT_TIMEOUT):
    """取某分类的目录。返回 [(currency_id, name, slug)]，失败返回 []。

    ★ 列表页 payload 里「当前分类」是填满的，其它分类只给 1 项做导航 ——
      所以取项数最多的那个分类，就是本类目录。
    """
    status, text = _get(f"{SITE}/{league}/{category}", timeout=timeout)
    if status != 200:
        return []
    blob = json_at(text, '"categories":')
    if not blob:
        return []
    try:
        cats = json.loads(blob)
    except Exception:  # noqa: BLE001
        return []
    if not cats:
        return []
    cat = max(cats, key=lambda c: len(c.get("items") or []))
    out = []
    for it in cat.get("items") or []:
        cid, name = it.get("id"), it.get("name")
        if cid and name:
            out.append((cid, name, kebab(name)))
    return out


def fetch_series(league: str, category: str, slug: str,
                 timeout: int = DEFAULT_TIMEOUT):
    """取某物品的多基准序列。

    返回 {"item_id": str, "bases": {base: [(ts, value, stock), ...]}}，
    失败/无数据返回 None。序列按 ts 升序。
    """
    status, text = _get(f"{SITE}/{league}/{category}/{slug}", timeout=timeout)
    if status != 200:
        return None
    blob = json_at(text, '"series":')
    if not blob:
        return None
    try:
        series = json.loads(blob)
    except Exception:  # noqa: BLE001
        return None
    m = re.search(r'"itemId":"([^"]+)"', text)
    out: dict[str, list] = {}
    for s in series:
        base = s.get("currencyId")
        if not base:
            continue
        pts = []
        for e in s.get("entries") or []:
            ts, v = e.get("timestamp"), e.get("price")
            if ts is None or v is None:
                continue
            pts.append((int(ts), float(v), e.get("stock")))
        if pts:
            pts.sort(key=lambda p: p[0])
            out[base] = pts
    if not out:
        return None
    return {"item_id": m.group(1) if m else slug, "bases": out}


def fetch_round(league: str, categories=None, workers: int = DEFAULT_WORKERS,
                timeout: int = DEFAULT_TIMEOUT, log=None):
    """抓一轮：先取各分类目录，再并发取每个物品的序列。

    返回 {"points": [...], "catalog": [...], "stats": {...}}
    points 元素 = (currency_id, base, ts, value, stock)
    """
    cats = list(categories or DEFAULT_CATEGORIES)
    t0 = time.time()
    catalog_rows, jobs = [], []
    for cat in cats:
        items = fetch_catalog(league, cat, timeout=timeout)
        if log:
            log(f"overlay 目录 {cat}: {len(items)} 项")
        for cid, name, slug in items:
            catalog_rows.append((cat, cid, name, slug))
            jobs.append((cat, cid, slug))

    points: list[tuple] = []
    ok = fail = 0
    if jobs:
        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            futs = {pool.submit(fetch_series, league, c, s, timeout):
                    (c, cid, s) for c, cid, s in jobs}
            for fut, (c, cid, s) in futs.items():
                try:
                    got = fut.result()
                except Exception:  # noqa: BLE001
                    got = None
                if not got:
                    fail += 1
                    continue
                ok += 1
                for base, pts in got["bases"].items():
                    for ts, v, stock in pts:
                        points.append((cid, base, ts, v, stock))

    dur = time.time() - t0
    stats = {"categories": len(cats), "items": len(jobs), "ok": ok,
             "fail": fail, "points": len(points), "seconds": round(dur, 1)}
    if log:
        log(f"overlay 一轮：{stats}")
    return {"points": points, "catalog": catalog_rows, "stats": stats}


def ensure_schema(con: sqlite3.Connection) -> None:
    """建表。app.py 的 SCHEMA 里也有一份，这里单独一份是为了让本模块能独立跑。"""
    con.executescript("""
    CREATE TABLE IF NOT EXISTS overlay_point (
        league       TEXT    NOT NULL,
        currency_id  TEXT    NOT NULL,
        base         TEXT    NOT NULL,   -- divine / chaos / exalted / annul
        ts           INTEGER NOT NULL,   -- 日线桶，UTC 毫秒
        value        REAL,               -- 1 个该通货值多少基准货币
        stock        REAL,               -- 挂出量
        PRIMARY KEY (league, currency_id, base, ts)
    );
    CREATE INDEX IF NOT EXISTS idx_overlay_point_ts
        ON overlay_point (league, ts);
    CREATE TABLE IF NOT EXISTS overlay_catalog (
        league       TEXT    NOT NULL,
        category     TEXT    NOT NULL,
        currency_id  TEXT    NOT NULL,
        name         TEXT,
        slug         TEXT,
        PRIMARY KEY (league, category, currency_id)
    );
    """)


def store(con: sqlite3.Connection, league: str, result: dict,
          log=None) -> int:
    """落库。只写新点 + 重写每 (通货, 基准) 的最新一格，不整轮全量 REPLACE。"""
    ensure_schema(con)
    con.executemany(
        "INSERT OR REPLACE INTO overlay_catalog "
        "(league, category, currency_id, name, slug) VALUES (?,?,?,?,?)",
        [(league, c, cid, name, slug) for c, cid, name, slug in result["catalog"]])

    floors: dict[tuple[str, str], int] = {}
    for cid, base, mx in con.execute(
            "SELECT currency_id, base, MAX(ts) FROM overlay_point "
            "WHERE league=? GROUP BY currency_id, base", (league,)):
        floors[(cid, base)] = int(mx or 0)

    rows = []
    for cid, base, ts, v, stock in result["points"]:
        floor = floors.get((cid, base), 0)
        if ts >= floor:          # >= 是为了把今天那一格也重写
            rows.append((league, cid, base, ts, v, stock))
    if rows:
        con.executemany(
            "INSERT OR REPLACE INTO overlay_point "
            "(league, currency_id, base, ts, value, stock) VALUES (?,?,?,?,?,?)",
            rows)

    cut = int(time.time() * 1000) - RETENTION_DAYS * 86400_000
    con.execute("DELETE FROM overlay_point WHERE league=? AND ts < ?", (league, cut))
    con.commit()
    if log:
        log(f"overlay 落库 {len(rows)} 行（去重后）")
    return len(rows)


def latest_map(con: sqlite3.Connection, league: str,
               currency_id: str) -> dict[str, dict]:
    """某通货在各基准上的最新价。返回 {base: {"value":..,"stock":..,"ts":..}}。"""
    rows = con.execute(
        "SELECT p.base, p.value, p.stock, p.ts FROM overlay_point p "
        "WHERE p.league=? AND p.currency_id=? AND p.ts = ("
        "  SELECT MAX(q.ts) FROM overlay_point q WHERE q.league=p.league "
        "  AND q.currency_id=p.currency_id AND q.base=p.base)",
        (league, currency_id)).fetchall()
    return {r[0]: {"value": r[1], "stock": r[2], "ts": int(r[3])} for r in rows}


def latest_all(con: sqlite3.Connection, league: str) -> dict[str, dict[str, dict]]:
    """全库最新价：{currency_id: {base: {...}}}。"""
    rows = con.execute(
        "SELECT p.currency_id, p.base, p.value, p.stock, p.ts FROM overlay_point p "
        "WHERE p.league=? AND p.ts = ("
        "  SELECT MAX(q.ts) FROM overlay_point q WHERE q.league=p.league "
        "  AND q.currency_id=p.currency_id AND q.base=p.base)",
        (league,)).fetchall()
    out: dict[str, dict[str, dict]] = {}
    for cid, base, v, stock, ts in rows:
        out.setdefault(cid, {})[base] = {"value": v, "stock": stock, "ts": int(ts)}
    return out


def series(con: sqlite3.Connection, league: str, cid: str,
           base: str) -> list[tuple[int, float]]:
    """某通货在某基准上的完整日线 [(ts, value)]，升序。"""
    rows = con.execute(
        "SELECT ts, value FROM overlay_point "
        "WHERE league=? AND currency_id=? AND base=? ORDER BY ts",
        (league, cid, base)).fetchall()
    return [(int(r[0]), r[1]) for r in rows if r[1]]


def vol(con: sqlite3.Connection, league: str, cid: str, days: int = 7,
        a: str = "exalted", b: str = "chaos"):
    """「这个通货的各基准报价互相打架吗」——可信度。返回 None 表示算不出来。

    ★ 为什么必须有它（2026-10-14 实测）：
      各基准报价在非热门通货上会互相打架（transmute 的 chaos 路和 exalted 路
      折出来差 678%，greater-orb-of-transmutation 差 1823%）。听着像天赐套利，
      但拿 40 天日线逐日反推就能看出破绽 —— 它俩的隐含汇率在 0.23x~0.69x 之间
      乱跳（波动近 3 倍），是薄市场的噪音，不是能吃的价差。
      反过来 divine / vaal 反推出来的汇率天天贴着市场值（波动 1.04 / 1.21），
      它们的差价才可信。所以榜单必须按这个波动过滤，否则满屏都是垃圾数字。

    算法：用该通货自己的 (a 价 ÷ b 价) 反推「1 个 b 值多少 a」，
    再跟市场汇率（b 自己的 a 价）比，取最近 days 天这个比值的 max/min。
    越接近 1 越可信。
    """
    mkt = dict(series(con, league, b, a))
    if not mkt:
        return None
    sa = dict(series(con, league, cid, a))
    sb = dict(series(con, league, cid, b))
    if not sa or not sb:
        return None
    out = []
    for ts in sorted(set(sa) & set(sb))[-days:]:
        m = mkt.get(ts)
        if not m or not sa[ts] or not sb[ts]:
            continue
        out.append((sa[ts] / sb[ts]) / m)
    if len(out) < 2 or min(out) <= 0:
        return None
    return max(out) / min(out)


def arb_board(con: sqlite3.Connection, league: str, max_vol=None,
              ref: str = "chaos") -> list[dict]:
    """整张跨基准差价榜，按差价降序。

    max_vol=None 不过滤；给个数值（如 1.5）就只保留波动没超过它的。
    ★ 算不出波动的非基准通货一律不要（实测 mirror 就是这样混进榜首的：
      它只有 chaos / divine 两条序列，两条互相差 2190%，没有第三条就没法证伪）。
      基准货币自己（divine / chaos / exalted / annul）例外 —— 它们缺的那条序列
      是「自己兑自己」，恒等于 1，本来就不用给。
    """
    prices = latest_all(con, league)
    rows: list[dict] = []
    for cid in prices:
        row = arb_row(prices, cid, ref=ref)
        if not row:
            continue
        v = vol(con, league, cid)
        if v is None and cid not in BASES:
            continue
        if max_vol is not None and v is not None and v > max_vol:
            continue
        row["currency_id"] = cid
        row["vol"] = v
        rows.append(row)
    rows.sort(key=lambda r: -r["spread"])
    return rows


def arb_row(prices: dict[str, dict[str, dict]], cid: str,
            ref: str = "chaos"):
    """算一个通货的跨基准差价，全部折算到 ref 尺子。

    prices = latest_all() 的结果。
    返回 {"via": {base: 折算价}, "best": base, "worst": base, "spread": 比值-1}

    ★ 换算：X 用基准 b 计价是 R[X][b]，而 1 个 b 值 R[b][ref] 个 ref，
      所以 X 折算到 ref = R[X][b] × R[b][ref]，其中 R[ref][ref] = 1。
    """
    own = prices.get(cid) or {}
    via: dict[str, float] = {}
    for b in ARB_BASES:
        if b == ref:
            continue
        px = own.get(b)
        if not px or not px.get("value"):
            continue
        # R[b][ref]：1 个基准货币 b 值多少 ref
        rb = (prices.get(b, {}).get(ref) or {}).get("value")
        if not rb:
            continue
        via[b] = px["value"] * rb
    # 直接用 ref 自己计价的那一路
    if cid == ref:
        via[ref] = 1.0
    elif own.get(ref, {}).get("value"):
        via[ref] = own[ref]["value"]
    if len(via) < 2:
        return None
    best = min(via, key=lambda k: via[k])
    worst = max(via, key=lambda k: via[k])
    lo, hi = via[best], via[worst]
    if lo <= 0:
        return None
    return {"via": via, "best": best, "worst": worst,
            "spread": hi / lo - 1.0}


if __name__ == "__main__":
    import sys

    lg = sys.argv[1] if len(sys.argv) > 1 else "forbidden-rites"
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    print(f"联盟 {lg}，只探前 {n} 项（只抓 currency）")
    cat = fetch_catalog(lg, "currency")
    print(f"目录 {len(cat)} 项，前 {n} 项：")
    for cid, name, slug in cat[:n]:
        got = fetch_series(lg, "currency", slug)
        if not got:
            print(f"  {cid:34s} ✗")
            continue
        line = []
        for b, pts in got["bases"].items():
            ts, v, st = pts[-1]
            line.append(f"{b}={v:g}")
        print(f"  {cid:34s} itemId={got['item_id']:28s} {len(got['bases'])} 基准 "
              f"{' '.join(line)}")
