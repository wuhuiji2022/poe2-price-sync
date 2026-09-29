"""PoE2 通货汇率追踪器。

按 scout 自身的更新节奏抓取 PoE2 通货报价（默认 poe2scout，config 里 price_source
可切回 poe.ninja），把每种通货统一换算成 崇高石 / 混沌石 / 神圣石 三种基准后写入
SQLite，并通过本地网页提供「搜索 + 图表」看板。

数据源：
    汇率  https://api.poe2scout.com/...                    （默认，POE2 专用，单一聚合价）
          https://poe.ninja/poe2/api/economy/...           （备源；也提供条目清单/走势/成交量）
    名称  https://poe.game.qq.com/api/trade2/data/static   （中文）
          https://www.pathofexile.com/api/trade2/data/static （英文兜底）
    图标  https://poe.game.qq.com + image 路径              （首次请求后本地缓存）

注意：汇率源（无论 scout 还是 ninja）每种通货都只有一个聚合价，没有买卖价差。
官方交易接口 /exchange/poe2/ 虽然分买卖两侧，但实测挂单窗口极小且混着大量
低价求购单，算出来的价与真实成交价偏差 15%~23%，不可用——买卖报价改由用户在
计算器里按游戏内实际情况手工录入（SPREAD_ENABLED 默认关闭）。
"""

from __future__ import annotations

import io
import json
import math
import os
import re
import socket
import sqlite3
from concurrent.futures import ThreadPoolExecutor
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
import statistics
import datetime as dt
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import zhdict
from zhdict import ZH

APP_NAME = "poe2-currency-tracker"
VERSION = "1.27.10"
USER_AGENT = f"{APP_NAME}/{VERSION} (personal local tool)"

NINJA_API = "https://poe.ninja/poe2/api/economy"
NINJA_LEAGUES = f"{NINJA_API}/leagues"
CN_STATIC = "https://poe.game.qq.com/api/trade2/data/static"
EN_STATIC = "https://www.pathofexile.com/api/trade2/data/static"

# poe2scout：POE2 专用价格 API，价格以「崇高石 = 1」计价。
# 实测它给出的神圣石价格（约 507 崇高）比 poe.ninja（约 492 崇高）更接近游戏内交易所，
# 所以默认用它覆盖 ninja 的通货报价；想换回 ninja 就把 config.json 的 price_source 改成 "ninja"。
SCOUT_API = "https://api.poe2scout.com"
SCOUT_REALM = "poe2"
SCOUT_BASE_CURRENCY = "exalted"

# dadsofexile：直接扒游戏内货币交易所的订单簿，几分钟刷新一次。
# 8 个样本实测：平均绝对偏差 10.5%，对比 poe2scout 的 48.2%，
# 而且 scout 有系统性高估（7/8 条偏高，冷门通货能错到 +86%）。所以主源改用它。
# ⚠️ 它条目质量参差（约 62 条是占位/顶替的假价），必须用挂单数过滤，见 doe_prices()。
DOE_API = "https://dadsofexile.com/api/prices"

# ------------------------------------------------------------------ 可配置层
# 游戏会更新赛季、新增通货与类别，所以这些都不能写死在代码里。
# 配置放在 exe 同目录的 config.json，缺失时自动生成默认版本。

DEFAULT_CONFIG: dict = {
    "league": "Forbidden Rites",
    "auto_follow_latest_league": True,  # 新赛季开启后自动切到官方列表里的第一个联盟
    "interval_minutes": 30,   # 数据源约每 30~70 分钟成批刷新一次，30 分钟抓一次能把滞后压到最小
    "port": 8712,
    "retention_days": 30,
    # 分类顺序照抄 PoE Overlay II（market-history 侧栏）：
    # currency → essences → delirium → breach → abyss → fragments → runes →
    # ritual → soul-cores → idols → uncut-gems → expedition → gems。
    # 实测对照（2026-09-24）：Overlay 那 14 档里 12 档在 poe.ninja 上有数据；
    #   · atziri's-temple、gems 两档 ninja 不提供 —— gems 用 LineageSupportGems 顶上
    #   · 我们多一档 Verisium（赛季内容，Overlay 没单列），排在最后
    # 注意 id 必须是官方静态数据的分组名（会被拿去打 ninja），不能写中文或 Overlay 的 slug。
    "categories": [
        {"id": "Currency", "label": "通货", "enabled": True},
        {"id": "Essences", "label": "精髓", "enabled": True},
        {"id": "Delirium", "label": "液态情感", "enabled": True},
        {"id": "Breach", "label": "裂隙催化剂", "enabled": True},
        {"id": "Abyss", "label": "深渊之骨", "enabled": True},
        {"id": "Fragments", "label": "碎片", "enabled": True},
        {"id": "Runes", "label": "符文", "enabled": True},
        {"id": "Ritual", "label": "祭坛预兆", "enabled": True},
        {"id": "SoulCores", "label": "灵魂核心", "enabled": True},
        {"id": "Idols", "label": "神像", "enabled": True},
        {"id": "UncutGems", "label": "未切割宝石", "enabled": True},
        {"id": "Expedition", "label": "远征物品", "enabled": True},
        {"id": "LineageSupportGems", "label": "血统辅助宝石", "enabled": True},
        {"id": "Verisium", "label": "维里斯汞金", "enabled": True},
    ],
    "auto_discover_categories": True,  # 每次同步时探测官方新增的类别
    # 暗金榜：poe.ninja 的暗金分类（注意是复数，写单数会 404）
    "unique_categories": [
        {"id": "UniqueWeapons", "label": "暗金武器", "enabled": True},
        {"id": "UniqueArmours", "label": "暗金护甲", "enabled": True},
        {"id": "UniqueAccessories", "label": "暗金首饰", "enabled": True},
        {"id": "UniqueJewels", "label": "暗金珠宝", "enabled": True},
        {"id": "UniqueFlasks", "label": "暗金药剂", "enabled": True},
        {"id": "UniqueCharms", "label": "暗金护符", "enabled": True},
        {"id": "UniqueSanctumRelics", "label": "暗金遗物", "enabled": True},
        {"id": "UniqueTablets", "label": "暗金石板", "enabled": True},
    ],
    # 挂单数少于这个的档位不作为参考价：样本太少时价格容易被个别挂错的单带偏
    "unique_min_listing": 3,
    "unique_retention_days": 30,
    # 暗金「官方集市」查询的节奏。之前按差值扫描那套 8 秒一发的节奏走，
    # 一件要等近一分钟；实测官方对 search/fetch 的容忍度比想象中高，
    # 单独给它一套更快的节拍器（2.5s），一件 9 个请求约 25 秒出结果。
    # 真撞上 429 会自动按官方给的秒数罚等并翻倍放宽，不会硬闯。
    "unique_market_gap": 2.5,        # 集市查询两次请求之间的间隔（秒）
    "unique_market_sample": 20,      # 每种口径取回多少条挂单算中位数（10 的倍数）
    "unique_market_ttl_hours": 6,    # 一件查完后多久内直接读缓存
    "unique_market_cooldown": 300,   # 被限流后同一件至少隔多久才允许再查
    # 暗金没有官方中文名来源（官方静态数据里只有通货分组），
    # 想显示中文就在这里补：键写 poe.ninja 的英文物品名，值写中文。
    "unique_name_zh": {},
    # 买卖差价榜已被移除：它必须逐个通货去撞官方交易接口，限流非常凶
    # （实测动辄罚等 1~3 分钟，整晚只写进十几条数据），为了不影响正常使用默认关闭。
    # 想恢复：把这里改成 true，并把前端差价榜页面加回来即可（后端接口都还留着）。
    "spread_enabled": False,
    "spread_top_n": 12,          # 手动「立即扫描」时一轮最多扫多少个通货
    "spread_request_gap": 8.0,   # 两次交易接口请求之间的间隔（秒）
    "spread_pairs_per_round": 3, # 自动扫描每轮扫几个通货
    "spread_round_seconds": 600, # 自动扫描每轮之间的间隔（秒）——官方限流很严，别调太小
    "arb_min_value": 0.01,       # 倒货榜只显示现价 ≥ 这个值的通货（小于两位小数的不显示）
    # ⚠️ 下面三个阈值是照 v1.23 的新口径（挂出量/求购量）重新定的，别再拿旧口径的数。
    # 实测 655 项分位数：挂出量 P25=5 P50=53 P75=231 P90=914；
    # 求购量 P25=8 P50=49 P75=388 P90=3214。绝大多数是小通货（符文/精髓/灵魂核心），
    # 挂出量本来就只有几十个——新口径是把它们从 0 救回来，阈值必须跟着往下走，
    # 否则一件不误判地把六成以上都打成冷漠（旧值 300/100/500 实测判冷漠 73%）。
    "arb_min_stock": 30,         # 求购量下限：有多少通货挂着收它，太低＝卖不出去
    "arb_min_orders": 10,        # 挂出量下限：低于此值＝市场上没货，直接判冷漠
    "arb_active_orders": 1000,   # 活跃线：挂出量到此数即视为有人交易，单对深度少也不判冷漠
    # 冷漠通货默认**不**剔出，只排到榜单末尾（阈值有误判，剔太狠榜单会空）。
    "arb_hide_cold": False,
    # 勾了「剔出冷漠通货」时，最多剔掉榜单总数的这个比例（2/5），保证还剩 3/5
    "arb_max_cold_ratio": 0.4,
    # 窗口内采样点少于这个数，推荐分按比例打折（点数太少时振幅不可信）
    "arb_conf_samples": 8,
    # 云端补数据：本机只在程序开着的时候抓，电脑一关，24 小时窗口就空出一段，
    # 倒货榜靠窗口内的 MIN/MAX 算波动空间，采样一少峰谷就抓不到，分数跟着失真。
    # 云端（GitHub Actions）每 30 分钟抓一轮、只留最近 48 小时，
    # 本机启动时把「自己没抓到的时段」补进来。留空 = 不启用。
    # 填 jsDelivr 地址（国内能访问）：
    #   https://cdn.jsdelivr.net/gh/<用户名>/<仓库>@data/data.json
    "cloud_sync_url": "",
    "cloud_sync_interval_minutes": 30,
    # dadsofexile 是个人小站，fetched_at 可能长时间不动（数据僵住）。
    # 超过这个秒数没刷新就判定陈旧：取价整体改用 poe2scout，
    # 库存/挂单也让 scout 接管（doe 那份连兜底都不再用）。标称 30 分钟刷一次。
    "doe_stale_seconds": 5400,   # 90 分钟
    # 第二道「僵住」判据：价格指纹连续这么久完全没变，就当 doe 在返回僵数据
    # （2026-09-28 实测：时间戳每 15 分钟在动，价格却 7 小时不动，
    #   只比时间戳的检测抓不到这种情况）。超时后本轮回退 poe2scout。
    # ⚠️ 别设太短：doe 正常就是 40~100 分钟才整体刷新一次，
    #    设成 45 分钟会在它正常待着的时候误判、来回切源把价格抖出 10% 的台阶。
    "doe_frozen_seconds": 9000,  # 2.5 小时
    # ★ 三个源的请求间隔是分开的（秒），别合成一个：
    #   dadsofexile 约 10 分钟重算 → 4 分钟取一次，跟得上
    #   poe.ninja   实测 6~82 分钟刷一次 → 1 小时取一次（原来每轮每类别都打，太浪费）
    #   poe2scout   6 小时一聚 → 30 分钟取一次
    # 缓存时长必须短于源自己的刷新周期，否则会把「源还没刷」误当成「源不动」。
    "ninja_ttl_seconds": 3600,   # 1 小时
    # poe.ninja 的汇率指纹连续这么久没变，就判它也不动了（它响应里没有时间戳）
    "ninja_frozen_seconds": 21600,  # 6 小时
    "spread_refs": ["chaos"],    # 自动扫描用哪些基准货币（与页面差价榜默认基准保持一致）
    "spread_window_hours": 24,   # 榜单展示窗口：多久之内扫到的挂单仍然展示
    "spread_rescan_minutes": 360, # 同一个通货隔多久才重新扫一次（配额有限，别调太小）
    "spread_display_limit": 20,  # 榜单只保留点差最大的前 N 个
    # 不做差价的通货（游戏「通货」页签里的基础通货，交易量大但点差没有意义）
    # 可以写 id，也可以写英文名 / 中文名，启动时会自动对应
    "spread_exclude": [
        "aug", "greater-orb-of-augmentation", "perfect-orb-of-augmentation",
        "transmute", "greater-orb-of-transmutation", "perfect-orb-of-transmutation",
        "regal", "greater-regal-orb", "perfect-regal-orb",
        "exalted", "greater-exalted-orb", "perfect-exalted-orb",
        "chaos", "greater-chaos-orb", "perfect-chaos-orb",
        "vaal", "alch", "divine", "chance", "annul",
        "artificers", "fracturing-orb", "mirror",
        "hinekoras-lock", "cryptic-key",
    ],
    "trade_host": "https://www.pathofexile.com",
    # 官方静态数据里没有中文条目的通货，在这里手动补中文名（键写通货 id）。
    # 赛季更新出了新物品、官方还没给中文时，直接往这里加一行就能顶上，优先级高于内置兜底表。
    "name_zh_overrides": {},
    # 是否显示命令行黑框。程序是不带控制台打包的（双击只有一个程序窗口），
    # 想看实时日志就把这里改成 true，再来一次就会先弹出黑框。
    # 无论这里是 true 还是 false，日志都会写进 data/logs/当天日期.log。
    "console": False,
}


def config_path() -> Path:
    return app_dir() / "config.json"


def load_config() -> dict:
    """读取配置文件；不存在则生成默认文件，读取失败时回退到默认配置。"""
    path = config_path()
    if not path.exists():
        save_config(DEFAULT_CONFIG)
        return json.loads(json.dumps(DEFAULT_CONFIG))

    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001 - 配置损坏不应导致程序无法启动
        # 这里用 print 而非 log：配置加载发生在日志函数就绪之前
        print(f"[配置] config.json 读取失败，使用默认配置：{exc}")
        return json.loads(json.dumps(DEFAULT_CONFIG))

    merged = json.loads(json.dumps(DEFAULT_CONFIG))
    merged.update(loaded)
    # 类别列表是核心配置，缺失或被清空时补回默认
    if not merged.get("categories"):
        merged["categories"] = json.loads(json.dumps(DEFAULT_CONFIG["categories"]))
    return merged


def save_config(config: dict) -> None:
    path = config_path()
    path.write_text(
        json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def enabled_categories() -> list[tuple[str, str]]:
    return [
        (entry["id"], entry.get("label") or entry["id"])
        for entry in CONFIG.get("categories", [])
        if entry.get("enabled", True)
    ]


def enabled_unique_categories() -> list[tuple[str, str]]:
    return [
        (entry["id"], entry.get("label") or entry["id"])
        for entry in CONFIG.get("unique_categories", [])
        if entry.get("enabled", True)
    ]


# 国际服官方 CDN，按优先级尝试；全部失败才回落到国服
IMAGE_HOSTS = (
    "https://web.poecdn.com",
    "https://www.pathofexile.com",
    "https://poe.game.qq.com",
)

BASE_LABELS = {"exalted": "崇高石", "chaos": "混沌石", "divine": "神圣石"}

# 用户客户端可能是繁中，而物品库里的中文名是简体。
# 下面这张「繁→简」表由物品库里的全部通货名自动生成（只覆盖名字里真正出现过的字），
# 搜索时把输入转成简体再比对，避免打繁中搜不出东西。
_TRAD_SIMP_PAIRS = (
    "亂乱亞亚佔占俠侠倫伦傳传儀仪優优兇凶內内凱凯剝剥創创劑剂勁劲動动勝胜勳勋卻却啓启喚唤喪丧嚴严圍围圓圆圖图團团執执堅坚報报"
    "壘垒壯壮夢梦奧奥奪夺婭娅實实寶宝尋寻岡冈峯峰嶽岳巔巅師师帶带庫库廳厅強强彈弹復复恆恒惡恶愛爱態态憤愤憶忆應应懼惧戰战捲卷"
    "換换搖摇擁拥擊击擬拟擴扩敵敌數数斷断時时會会極极榮荣樞枢機机歐欧殘残殼壳毀毁氣气決决淵渊減减湊凑準准滅灭漢汉漣涟潰溃澤泽"
    "濃浓烏乌煉炼熱热熾炽燭烛燼烬爐炉爛烂爭争爾尔獨独獵猎獻献瑪玛環环瓊琼產产甦苏異异瘋疯癡痴礦矿礫砾祕秘禍祸禮礼稜棱積积穢秽"
    "穩稳築筑籃篮約约納纳級级絆绊結结絕绝統统絲丝經经維维綻绽緣缘縛缚縮缩繩绳繮缰罰罚羅罗羈羁聖圣聲声脫脱腦脑膚肤臟脏茲兹荊荆"
    "莊庄華华蒼苍薩萨藥药蘭兰虛虚蛻蜕蝕蚀術术衛卫衝冲裏里裝装襲袭覆复覓觅視视觸触託托記记許许詐诈詛诅試试詩诗詭诡誅诛誇夸誌志"
    "語语說说諂谄請请諭谕諾诺謁谒識识譫谵護护變变豐丰豬猪貓猫貪贪貫贯貴贵費费質质賽赛贈赠贊赞軀躯軍军軸轴輔辅輕轻輝辉輻辐辮辫"
    "迴回連连進进運运達达遠远適适遺遗釋释鉢钵銘铭鋼钢錯错鎖锁鎧铠鏡镜鐘钟鐮镰鐵铁鑰钥長长陰阴陽阳階阶際际雜杂離离難难電电靈灵"
    "靜静鞏巩韌韧響响頌颂預预頓顿領领頭头顎颚願愿顫颤顱颅風风颶飓飛飞養养餘余饒饶馬马騎骑驍骁驟骤體体髮发鬥斗魘魇魯鲁鳴鸣鴉鸦"
    "鵰雕麗丽麥麦點点"
)
_TRAD_TO_SIMP: dict[str, str] = {
    _TRAD_SIMP_PAIRS[i]: _TRAD_SIMP_PAIRS[i + 1] for i in range(0, len(_TRAD_SIMP_PAIRS), 2)
}


def normalize_zh(text: str) -> str:
    """把繁体输入转成简体，简体原样返回。"""
    return "".join(_TRAD_TO_SIMP.get(ch, ch) for ch in text)


def matches_query(raw_query: str, *fields: str) -> bool:
    """搜索匹配：同时用原样和简体化的关键词去比对，繁简输入都能命中。"""
    needle = raw_query.strip()
    if not needle:
        return True
    variants = {needle.lower(), normalize_zh(needle).lower()}
    for field in fields:
        if not field:
            continue
        low = field.lower()
        if any(v in low for v in variants):
            return True
        if any(v in normalize_zh(field).lower() for v in variants):
            return True
    return False
BASE_COLUMNS = {"exalted": "value_exalted", "chaos": "value_chaos", "divine": "value_divine"}
# 本机历史攒到这么多采样点后，走势曲线就用自己的记录（此前只能借用数据源的 7 天趋势）
LOCAL_POINTS_MIN = 2

SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshot (
    ts           INTEGER NOT NULL,
    league       TEXT    NOT NULL,
    category     TEXT    NOT NULL,
    currency_id  TEXT    NOT NULL,
    value_divine REAL,
    value_exalted REAL,
    value_chaos  REAL,
    volume       REAL,
    trend        REAL,
    spark        TEXT,
    stock        REAL,      -- 求购量：各交易对里对手挂出、等着收它的通货数量合计
    -- 注意：这两个都是「挂出的数量」，不是挂单笔数（数据源不提供笔数）。
    -- 交易所每个交易对是双向的，两侧数字完全独立
    --   （实测 Omen of the Hunt ↔ 崇高石：预兆侧 0，崇高石侧 20,095）。
    -- orders = 挂出量：该通货自己挂出去多少（你想买能买到多少）
    -- stock  = 求购量：对手挂了多少通货在收它（你想卖有多少人接）
    orders       INTEGER,   -- 挂出量（poe2scout SnapshotPairs，全交易对合计）
    source       TEXT DEFAULT 'real',   -- real=本機实际抓取, synthetic=由 7 天趋势还原
    PRIMARY KEY (ts, league, category, currency_id)
);
CREATE INDEX IF NOT EXISTS idx_snapshot_lookup
    ON snapshot (league, currency_id, ts DESC);
CREATE TABLE IF NOT EXISTS spread (
    ts           INTEGER NOT NULL,
    league       TEXT    NOT NULL,
    currency_id  TEXT    NOT NULL,
    ref          TEXT    NOT NULL,
    ask          REAL,   -- 买入价：买 1 单位该通货要付多少基准货币
    bid          REAL,   -- 卖出价：卖 1 单位该通货能收到多少基准货币
    spread       REAL,   -- 点差百分比（通常为负：ask 高于 bid）
    ask_offers   INTEGER,
    bid_offers   INTEGER,
    mid          REAL,   -- 中间价，便于跟聚合价对比
    PRIMARY KEY (ts, league, currency_id, ref)
);
CREATE INDEX IF NOT EXISTS idx_spread_lookup
    ON spread (league, currency_id, ref, ts DESC);
CREATE TABLE IF NOT EXISTS item_meta (
    currency_id TEXT PRIMARY KEY,
    name_en     TEXT,
    name_zh     TEXT,
    icon        TEXT
);
-- 暗金物品快照。同一件暗金会按 roll 档位拆成多行，
-- 所以主键是 (档位 key) 而不是物品名，归组展示时再按 name + base_type 合在一起。
CREATE TABLE IF NOT EXISTS unique_snapshot (
    ts            INTEGER NOT NULL,
    league        TEXT    NOT NULL,
    category      TEXT    NOT NULL,
    item_key      TEXT    NOT NULL,
    name          TEXT,
    base_type     TEXT,
    icon          TEXT,
    level_req     INTEGER,
    corrupted     INTEGER DEFAULT 0,
    value_divine  REAL,
    value_exalted REAL,
    value_chaos   REAL,
    listing_count INTEGER DEFAULT 0,
    trend         REAL,
    spark         TEXT,
    mods          TEXT,
    PRIMARY KEY (ts, league, category, item_key)
);
CREATE INDEX IF NOT EXISTS idx_unique_lookup
    ON unique_snapshot (league, item_key, ts DESC);
CREATE INDEX IF NOT EXISTS idx_unique_name
    ON unique_snapshot (league, name, ts DESC);
-- 暗金在官方集市上查出来的价格中位数（污染 / 未污染 / 未鉴定三种口径）
CREATE TABLE IF NOT EXISTS unique_market (
    league   TEXT    NOT NULL,
    name     TEXT    NOT NULL,
    variant  TEXT    NOT NULL,
    median   REAL,
    lo       REAL,
    hi       REAL,
    count    INTEGER DEFAULT 0,
    total    INTEGER DEFAULT 0,
    partial  INTEGER DEFAULT 0,
    ts       INTEGER,
    PRIMARY KEY (league, name, variant)
);
CREATE TABLE IF NOT EXISTS app_meta (k TEXT PRIMARY KEY, v TEXT);
"""


# ---------------------------------------------------------------- 路径与环境

def app_dir() -> Path:
    """可写目录：数据、图标缓存都放这里（打包后是 exe 所在目录）。"""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def web_dir() -> Path:
    """静态资源目录（打包后位于 PyInstaller 临时目录）。"""
    if getattr(sys, "frozen", False):
        return Path(sys._MEIPASS) / "web"  # type: ignore[attr-defined]
    return Path(__file__).resolve().parent / "web"


DATA_DIR = app_dir() / "data"
ICON_DIR = DATA_DIR / "icons"
DB_PATH = DATA_DIR / "tracker.db"
# 日志文件目录。程序不带控制台跑的时候，这里是唯一能追溯「刚才到底发生了什么」的地方，
# 所以 log() 一律同时写一份到这里（按天一个文件）。
LOG_DIR = DATA_DIR / "logs"
LOG_KEEP_DAYS = 7


def setup_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    ICON_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    # 暗金中文化词典：由 build_zh.py 生成，缺失时页面回落英文，不影响其它功能
    ZH.load(DATA_DIR / "unique_zh.json")


# 配置在路径确定之后才加载（config.json 与数据放在同一目录）
CONFIG: dict = load_config()

LEAGUE = CONFIG.get("league") or "Forbidden Rites"
# config 里写的是兜底值。开启 auto_interval 后会按 scout 实际的更新间隔重算：
# 源 6 小时才刷一次，程序却每 30 分钟抓一轮，抓到的全是同一个数，既浪费请求
# 又会把重复值写进历史，让倒货榜的波动看起来偏小。
INTERVAL_SECONDS = max(5, int(CONFIG.get("interval_minutes", 60))) * 60
AUTO_INTERVAL = bool(CONFIG.get("auto_interval", True))
# 自动间隔的上下限：再快也不低于 30 分钟（要给足采样密度），再慢不超过 6 小时
AUTO_INTERVAL_MIN = 30 * 60
AUTO_INTERVAL_MAX = 6 * 60 * 60
# ★ 只在「值真的变了」时才写库（v1.27.7）。
# 为什么：本机 5 分钟一采，而 dadsofexile 约 10 分钟才重算一次，
# 且它用上千万成交量算加权价，天然稳定——2026-09-29 实测最近 30 轮快照
# **98% 的行与上一轮完全相同**（18797 行里 18430 行是重复值）。
# 这些重复行既撑大数据库（5.7 小时就写进去 1.8 万行），又让「抓取完成，写入 N 行」
# 的 N 虚高，看不出到底有没有新东西。改成变化才写：值没动就不落行，
# 曲线和 MIN/MAX 算出来的波动完全不变（重复点本来也不提供新信息）。
# 关掉它就退回老行为（每轮全量写）。
SKIP_UNCHANGED = bool(CONFIG.get("skip_unchanged", True))
PORT = int(CONFIG.get("port", 8712))
# 监听地址：0.0.0.0 = 局域网内其他设备也能打开；改回 127.0.0.1 就只允许本机访问
HOST = str(CONFIG.get("host") or "0.0.0.0").strip()
RETENTION_DAYS = int(CONFIG.get("retention_days", 90))
def _clamp_int(raw, fallback: int, low: int, high: int) -> int:
    """把配置项夹到合法区间，避免手改 config.json 时写错导致崩溃。"""
    try:
        return max(low, min(high, int(raw)))
    except (TypeError, ValueError):
        return fallback


# 通货报价用哪个源。scout = poe2scout（更贴近游戏内交易所），ninja = poe.ninja。
# 实测两个源都有数据，只是 scout 的神圣石汇率更接近游戏里看到的数，所以默认 scout。
PRICE_SOURCE = str(CONFIG.get("price_source") or "scout").strip().lower()
if PRICE_SOURCE not in ("scout", "ninja"):
    PRICE_SOURCE = "scout"

# ★ 取价主源（可在界面上随时切换，不必改配置文件重启）。
#
# 为什么要它：2026-09-29 用户拿游戏内交易所的真值逐条核对过，三个源的准确度
# 和我们的默认假设**完全相反**：
#     真值             doe      ninja    scout
#     1d=535 exalted   +4%      -4%      +1%
#     1d=8.12 chaos    +36%     +0%      -4%
#     1c=60.5 exalted  -16%     +5%      +14%
#     1 annul=279 e    -61%     +20%     +32%
#   平均绝对偏差       29.2%    7.2%     12.8%
# → ninja 明显最准，而我们一直在拿最差的 doe 当主源。
#   但 ninja 只覆盖 52 种通货（scout 有 646），覆盖率差一大截，
#   所以不能一刀切改默认——**让它可选**，用户按自己在意的是「准」还是「全」来选。
#
#   auto  = doe 优先、拿不到再按 price_source 兜底（老行为，覆盖最全）
#   doe   = 只用 dadsofexile（不回退；它拿不到的通货就没价）
#   scout = 只用 poe2scout（跳过 doe）
#   ninja = 只用 poe.ninja（跳过 doe 和 scout，覆盖约 52 种）
PRIMARY_SOURCE_DEFAULT = str(CONFIG.get("primary_source") or "auto").strip().lower()
if PRIMARY_SOURCE_DEFAULT not in ("auto", "doe", "scout", "ninja"):
    PRIMARY_SOURCE_DEFAULT = "auto"
# 界面上切换后存在这里；None 表示沿用配置文件
PRIMARY_SOURCE_OVERRIDE: str | None = None


def primary_source() -> str:
    """当前生效的取价主源。"""
    return PRIMARY_SOURCE_OVERRIDE or PRIMARY_SOURCE_DEFAULT


def set_primary_source(value: str) -> str:
    """切换取价主源，返回实际生效的值（非法输入会被忽略）。"""
    global PRIMARY_SOURCE_OVERRIDE
    v = str(value or "").strip().lower()
    if v not in ("auto", "doe", "scout", "ninja"):
        return primary_source()
    PRIMARY_SOURCE_OVERRIDE = v if v != PRIMARY_SOURCE_DEFAULT else None
    return primary_source()

# 买卖差价榜总开关。关闭时后端不再启动扫描线程，也不会去碰官方交易接口。
# 官方 trade2 的挂单窗口很小且混着大量低价求购单，算出来的买/卖价和真实成交价对不上，
# 所以这个开关默认关闭，也不再有自动买卖价展示。
SPREAD_ENABLED = bool(CONFIG.get("spread_enabled", False))
SPREAD_TOP_N = int(CONFIG.get("spread_top_n", 20))
SPREAD_REQUEST_GAP = float(CONFIG.get("spread_request_gap", 8.0))
SPREAD_PAIRS_PER_ROUND = _clamp_int(CONFIG.get("spread_pairs_per_round", 2), 2, 1, 30)
SPREAD_ROUND_SECONDS = _clamp_int(CONFIG.get("spread_round_seconds", 600), 600, 60, 3600)
SPREAD_WINDOW_HOURS = _clamp_int(CONFIG.get("spread_window_hours", 24), 24, 1, 24 * 30)
SPREAD_RESCAN_SECONDS = _clamp_int(CONFIG.get("spread_rescan_minutes", 360), 360, 10, 1440) * 60
ARB_MIN_VALUE = float(CONFIG.get("arb_min_value", 0.01) or 0.0)
# 倒货榜推荐队列的求购量下限：没人挂通货收它，就等于卖不出去，
# 排进推荐榜只会误导。低于此值的不进推荐排序（仍可取回，标记 eligible=False）。
ARB_MIN_STOCK = _clamp_int(CONFIG.get("arb_min_stock", 30), 30, 0, 10_000_000)
# 挂出量下限：市场上根本没货可买时，报价没有可成交性。
# ⚠️ 口径已换（v1.23 起是全交易对合计，v1.24 重新标定）：
# 全市场挂出量实测 P50≈53、P75≈231。定 10 —— 货架上连 10 个都凑不出来的才算没货。
# 旧值 100 是按旧口径（只统计"对崇高石"那一个交易对）定的，沿用会把六成通货误杀。
ARB_MIN_ORDERS = _clamp_int(CONFIG.get("arb_min_orders", 10), 10, 0, 10_000_000)
# 「活跃线」：挂出量到了这个数就算有人在交易，此时即使求购量少也不该判冷漠。
# ⚠️ 踩过的坑：只按「求购量 < 300」一刀切，会把挂出 2000+、但分散在各交易对
# 的高价小件货（如狂猿雕像 orders=2671 / stock=130）全误杀成冷漠 —— 它明明很活跃。
# 655 项里一下砍掉 520 项，榜单失去意义。所以求购量门槛必须和挂出量联合判断。
# 定 1000 ≈ 挂出量 P90（实测 914）：能进前 10% 的挂出量，就认它是活跃品种。
ARB_ACTIVE_ORDERS = _clamp_int(CONFIG.get("arb_active_orders", 1000), 1000, 0, 10_000_000)
# 是否把「冷漠通货」整个剔出推荐队列（False 时只是排到后面，仍然显示）。
# 默认 False：冷漠判定本身有误差，一刀剔掉会让榜单突然少一大半，
# 用户更愿意自己看一眼再决定，所以默认只沉底不剔出。
ARB_HIDE_COLD = bool(CONFIG.get("arb_hide_cold", False))
# 就算用户勾了「剔出冷漠通货」，最多也只能剔掉这么多（占榜单总数的比例）。
# ⚠️ 踩过的坑：阈值定死时，升级到新口径的第一轮可能大半通货都「未知」或「偏薄」，
# 一刀切下去 655 项能剔掉 520 项，榜单直接空掉。留个上限保证任何时候都还剩 60%。
ARB_MAX_COLD_RATIO = min(
    max(float(CONFIG.get("arb_max_cold_ratio", 0.4) or 0.0), 0.0), 0.9
)
# 窗口内采样点少于这个数时，推荐分按 samples / 本值 打折。
# 只有两三个点算出来的「振幅」很可能是噪声，不该跟全天 48 个点的货抢榜首。
ARB_CONF_SAMPLES = _clamp_int(CONFIG.get("arb_conf_samples", 8), 8, 2, 200)

# ------------------------------------------------------------------ 云端补数据
# 本机只在程序开着的时候抓。电脑一关，24 小时窗口就空出一段，倒货榜的波动
# 空间（窗口内 MIN/MAX）会因采样变少而被系统性低估。云端每 30 分钟抓一轮、
# 只留 48 小时，本机把「自己没抓到的时段」补进来（source='cloud'）。
# ⚠️ 本机实测数据永远优先：已存在 real 快照的时间点，云端不覆盖。
CLOUD_SYNC_URL = str(CONFIG.get("cloud_sync_url") or "").strip()
CLOUD_SYNC_INTERVAL = _clamp_int(
    CONFIG.get("cloud_sync_interval_minutes", 30), 30, 5, 720) * 60
CLOUD_SYNC_TIMEOUT = 20

# jsDelivr 缓存的兜底源。jsDelivr 对「分支上的文件」最长缓存 12 小时：
# 实测同一时刻 jsDelivr 给的是 8.8 小时前的旧版，而仓库里早就是新版了；
# 加 ?t=<时间戳> 也绕不过（试过三次，三次都返回同一份旧数据）。
# 于是从 jsDelivr 链接反推出 raw 链接，发现 CDN 那份过期就换直连再取一次。
# 注意顺序不能倒：raw.githubusercontent 在国内经常被墙，jsDelivr 才是主源。
CLOUD_SYNC_URL_RAW = ""
_CDN_MATCH = re.match(
    r"https?://cdn\.jsdelivr\.net/gh/([^/]+)/([^@]+)@([^/]+)/(.+)$", CLOUD_SYNC_URL
)
if _CDN_MATCH:
    _user, _repo, _ref, _path = _CDN_MATCH.groups()
    CLOUD_SYNC_URL_RAW = f"https://raw.githubusercontent.com/{_user}/{_repo}/{_ref}/{_path}"

# 云端那份超过多少小时就算「缓存过期」，值得换源重试。
# 为什么是 2 小时而不是更短：GitHub 的 cron 是尽力而为，忙时会漏跑，
# 实测出现过 3 小时才跑一次的情况——那是云端真的没抓，不是 CDN 缓存旧。
# 阈值卡太紧就会每次同步都白跑一次直连请求，白等一个超时。
CLOUD_STALE_HOURS = 2.0

# 扫描哪些基准货币。页面差价榜默认看混沌石，所以默认只扫混沌石：
# 官方接口配额极其有限，扫得越杂、每个基准铺满的速度就越慢。
_SPREAD_REFS_RAW = CONFIG.get("spread_refs") or ["chaos"]
SPREAD_REFS: list[str] = [r for r in _SPREAD_REFS_RAW if r in ("chaos", "divine", "exalted")]
if not SPREAD_REFS:
    SPREAD_REFS = ["chaos"]
SPREAD_DISPLAY_LIMIT = _clamp_int(CONFIG.get("spread_display_limit", 20), 20, 5, 500)
SPREAD_EXCLUDE: list[str] = [
    str(x).strip() for x in (CONFIG.get("spread_exclude") or []) if str(x).strip()
]
# 每轮抓取后顺带缓存多少个图标（攒满后断网也能正常显示图标）
ICON_WARM_PER_ROUND = _clamp_int(CONFIG.get("icon_warm_per_round", 60), 60, 0, 500)

# ---------------------------------------------------------------- 自适应轮询
# poe.ninja 支持 ETag / If-None-Match：数据没变时返回 304，响应体是空的。
# 所以「有没有更新」这个问题的探测成本几乎为零，可以高频问而不撞限流；
# 只有真的变了才去拉全量数据。这样既跟得上源站节奏，又不会白烧配额。
ADAPTIVE_POLL = bool(CONFIG.get("adaptive_poll", True))
# 探测间隔的上下限：临近源站更新点时压到最小紧盯，离得远就放宽省请求
POLL_MIN_SECONDS = _clamp_int(CONFIG.get("poll_min_seconds", 60), 60, 15, 3600)
POLL_MAX_SECONDS = _clamp_int(
    CONFIG.get("poll_max_seconds", max(600, INTERVAL_SECONDS)), max(600, INTERVAL_SECONDS),
    POLL_MIN_SECONDS, 24 * 3600,
)
# 冷启动（还没摸清源站节奏）时，每轮没变化就把间隔乘这个系数，直到上限
POLL_BACKOFF = float(CONFIG.get("poll_backoff", 1.5) or 1.5)
# 兜底：万一 ETag 机制失效，超过这么久没抓过就强制抓一次（防止数据彻底停滞）
POLL_FALLBACK_SECONDS = _clamp_int(
    CONFIG.get("fallback_minutes", max(60, INTERVAL_SECONDS // 60)), max(60, INTERVAL_SECONDS // 60),
    5, 24 * 60,
) * 60
# 用哪些类别当"探针"（默认只探通货，它最活跃，ETag 变化能代表源站刷新）
_raw_probe = CONFIG.get("poll_probe_types") or ["Currency"]
POLL_PROBE_TYPES = [str(x).strip() for x in _raw_probe if str(x).strip()] or ["Currency"]
SPREAD_RETRIES = 3
TRADE_HOST = CONFIG.get("trade_host") or "https://www.pathofexile.com"
TRADE_BASE = f"{TRADE_HOST}/api/trade2"

CATEGORIES: list[tuple[str, str]] = enabled_categories()
UNIQUE_CATEGORIES: list[tuple[str, str]] = enabled_unique_categories()
UNIQUE_MIN_LISTING = _clamp_int(CONFIG.get("unique_min_listing", 3), 3, 0, 100)
UNIQUE_RETENTION_DAYS = _clamp_int(
    CONFIG.get("unique_retention_days", RETENTION_DAYS), int(CONFIG.get("retention_days", 30)),
    1, 365,
)


# ------------------------------------------------------------ 中文名兜底
# 官方静态数据里确实没有中文条目的通货（多数是赛季新物品），这里按官方命名习惯补一份中文。
# 想改直接编辑 config.json 里的 name_zh_overrides，那张表优先级更高。

ZH_FALLBACK: dict[str, str] = {
    "ravens-reflection": "渡鸦之映",
    "shattered-triskelion": "破碎三相环",
    "the-triskelion-reforged": "重铸三相环",
    "hawk-idol": "苍鹰雕像",
    "panther-idol": "黑豹雕像",
    "stoat-idol": "白鼬雕像",
    "eonyrs-thunder": "艾奥尼尔之雷",
    "helbryms-hide": "赫尔布林之皮",
}

_ZH_OVERRIDE_CACHE: dict[str, str] | None = None


def zh_overrides() -> dict[str, str]:
    """内置兜底表 + 用户在 config.json 里补充的中文名，后者优先。"""
    global _ZH_OVERRIDE_CACHE
    if _ZH_OVERRIDE_CACHE is None:
        merged: dict[str, str] = dict(ZH_FALLBACK)
        for key, value in (CONFIG.get("name_zh_overrides") or {}).items():
            if isinstance(value, str) and value.strip():
                merged[str(key).strip()] = value.strip()
        _ZH_OVERRIDE_CACHE = merged
    return _ZH_OVERRIDE_CACHE




def has_cjk(text: str) -> bool:
    """判断字符串里有没有汉字，用来识别「是不是真的拿到了中文名」。"""
    return any("\u4e00" <= ch <= "\u9fff" for ch in text or "")


def zh_display_name(currency_id: str, meta) -> str:
    """返回一定可展示的中文名；确实没有时返回空字符串，由调用方决定是否展示。

    优先级：用户/内置覆盖表 > 物品库中文名 > 空。
    绝不拿英文名或 id 冒充中文——宁可不显示，也不让列表里出现没有中文名的通货。
    """
    override = zh_overrides().get(currency_id)
    if override:
        return override
    if meta is not None:
        try:
            name_zh = meta["name_zh"] or ""
        except (KeyError, IndexError, TypeError):
            name_zh = ""
        if name_zh.strip() and has_cjk(name_zh):
            return name_zh.strip()
    return ""


# ------------------------------------------------------------------ 网络请求

# Windows 上 urlopen 会先做一次系统代理自动探测（WPAD）：读注册表里的
# Internet Settings 再去发现代理，探测超时后才回落直连，之后才有缓存。
# 实测第一个类别 34.19 秒、后面每个只要 2.7 秒——差的正是这一段，
# 这也是「每次打开程序都要等半天」的真凶（不是带宽、不是数据量）。
# 云端同步早就用 ProxyHandler({}) 绕开了（见 sync_from_cloud），主抓取路径漏了。
# ⚠️ 如果你确实在公司代理后面上网、必须走代理才能出去，
#    把 config.json 里的 bypass_system_proxy 改成 false 即可回落系统代理。
BYPASS_SYSTEM_PROXY = bool(CONFIG.get("bypass_system_proxy", True))
_HTTP_OPENER = urllib.request.build_opener(
    urllib.request.ProxyHandler({}) if BYPASS_SYSTEM_PROXY
    else urllib.request.ProxyHandler()
)


def http_get(url: str, *, timeout: int = 30, retries: int = 3) -> bytes:
    """带重试的 GET，失败返回最后一次异常。"""
    last: Exception | None = None
    for attempt in range(retries):
        try:
            request = urllib.request.Request(
                url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"}
            )
            with _HTTP_OPENER.open(request, timeout=timeout) as response:
                return response.read()
        except Exception as exc:  # noqa: BLE001 - 网络层统一兜底
            last = exc
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"请求失败 {url} -> {last}")


def http_json(url: str, *, retries: int = 3, timeout: int = 30):
    return json.loads(
        http_get(url, retries=retries, timeout=timeout).decode("utf-8", "ignore")
    )


# ------------------------------------------------------- poe2scout 通货报价
# poe2scout 是 POE2 专用价格站，价格以「崇高石 = 1」计价，一次能拿到 600+ 种通货，
# 覆盖面比 poe.ninja 的 51 种大得多，且汇率更接近游戏内交易所。

# 实测 poe2scout 的通货聚合价每 6 小时才更新一次（UTC 00/06/12/18 点），
# 短时间内反复请求拿到的完全是同一个数，所以缓存给到 30 分钟——
# 既避开 6 小时的盲区，也不至于白白去打第三方接口。
SCOUT_TTL_SECONDS = 30 * 60

# ★★ 三个源的请求间隔是**分开**的，别混成一个值
#
# 原先只有 dadsofexile / poe2scout 各带一个缓存时长，poe.ninja 是
# **每轮每个类别都现打一次**——一轮 14 个类别、5 分钟一轮，等于一天打它 4000 次，
# 而它自己其实是小时级刷新（实测 6~82 分钟），绝大多数请求拿回的是同一份数据。
#
#   dadsofexile  DOE_TTL_SECONDS   =   4 分钟  —— 它约 10 分钟重算一次，要跟得上
#   poe.ninja    NINJA_TTL_SECONDS =  60 分钟  —— 实测 6~82 分钟刷一次，按 1 小时取
#   poe2scout    SCOUT_TTL_SECONDS =  30 分钟  —— 它 6 小时一聚，30 分钟够避开盲区
#
# ⚠️ 缓存时长必须**短于**源自己的刷新周期，否则会把「源还没刷」误当成「源不动」，
#    一直拿上一份快照当新的用；但也不能远短于它，否则只是白白打接口。
NINJA_TTL_SECONDS = _clamp_int(
    CONFIG.get("ninja_ttl_seconds", 60 * 60), 60 * 60, 5 * 60, 6 * 3600
)

_NINJA_CACHE: dict[str, tuple[float, dict]] = {}
_NINJA_CACHE_LOCK = threading.RLock()


def ninja_cached(url: str, *, force: bool = False) -> dict:
    """带缓存地取一个 poe.ninja 端点（默认 1 小时才真的发一次请求）。

    ⚠️ 请求失败时**优先退回旧缓存**而不是抛出去：ninja 偶尔抖一下不该让整轮
       没数据，旧一份也比没有强（它本来就是小时级）。真的没缓存时才 raise。
    """
    now = time.time()
    with _NINJA_CACHE_LOCK:
        cached = _NINJA_CACHE.get(url)
        if cached and not force and now - cached[0] < NINJA_TTL_SECONDS:
            return cached[1]
    try:
        payload = http_json(url)
    except Exception:                                              # noqa: BLE001
        with _NINJA_CACHE_LOCK:
            old = _NINJA_CACHE.get(url)
        if old:
            log_once(f"ninja-cache-fallback:{url}",
                     f"  · poe.ninja 本次请求失败，沿用 {int(now - old[0]) // 60} "
                     f"分钟前的缓存")
            return old[1]
        raise
    if isinstance(payload, dict):
        with _NINJA_CACHE_LOCK:
            _NINJA_CACHE[url] = (now, payload)
    return payload


def source_intervals() -> dict[str, int]:
    """三个源各自的请求间隔（秒）。它们是分开配置的，调用方别当成一个值用。"""
    return {
        "dadsofexile": DOE_TTL_SECONDS,
        "poe.ninja": NINJA_TTL_SECONDS,
        "poe2scout": SCOUT_TTL_SECONDS,
    }

# ★★ scout 数据的最大可采信年龄。
#
# 2026-09-29 事故：poe2scout 整站停更（ExchangeSnapshot 卡在 09-28 08:00，22 小时没动），
# 但它照旧返回一份陈旧快照——价格、挂出量、求购量全是 22 小时前的数。
# 我们的逐条取价优先级是写死的 doe > scout > ninja，只要 scout 有这条数据，
# **哪怕它已经停更 22 小时，也轮不到更新的 poe.ninja**，于是这些通货整体冻结。
#
# scout 标称小时级刷新，取 3 小时当门槛（3 倍标称值，正常波动不会误伤）。
# 超过门槛就整份不参与逐条取价与挂出量取值，让位给 doe / ninja。
# 拿不到它的时间戳时不拦（保持老行为），免得缺字段反而把它一票否决。
SCOUT_MAX_AGE_SECONDS = _clamp_int(
    CONFIG.get("scout_max_age_seconds", 3 * 3600), 3 * 3600, 3600, 7 * 24 * 3600
)
_SCOUT_LOCK = threading.Lock()
_SCOUT_CACHE: dict[str, tuple[float, dict[str, float]]] = {}
# 各通货在 scout 里的数字 id，换汇明细接口只认这个 id
_SCOUT_ITEM_IDS: dict[str, dict[str, int]] = {}
# scout 源数据的更新时间与更新间隔（秒），由实际响应推断，供「上次更新」和抓取节奏使用
_SCOUT_SOURCE: dict[str, dict[str, int]] = {}
# scout 的库存（ByCategory 的 CurrentQuantity），随价格一起抓，不额外发请求。
# ⚠️ 语义不明：实测它与「全交易对挂出总量」对不上（transmute 111400 vs 11822），
#    只用来在拿不到 SnapshotPairs 时兜底，不作为主力口径。
_SCOUT_QUANTITY: dict[str, tuple[float, dict[str, float]]] = {}
# 交易所全量交易对快照：一次请求拿到全部 1648 个交易对（约 2.2MB），
# 按通货汇总出「挂出量（卖）」与「求购量（买）」。见 scout_pair_stocks()。
# 联盟 -> (抓取时刻, 挂出量, 求购量, 参与交易对数)
_SCOUT_PAIRS: dict[str, tuple[float, dict[str, float], dict[str, float], dict[str, int]]] = {}
SCOUT_PAIRS_TTL = 30 * 60


def _parse_scout_time(text: str) -> float:
    """解析 poe2scout 的时间戳。

    形如 2026-09-23T12:00:00.0000000Z，末尾是 7 位小数，
    fromisoformat 只认到 6 位，所以先截一刀；解析不了就返回 0。
    """
    raw = str(text or "").strip()
    if not raw:
        return 0.0
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    head, _, tail = raw.partition(".")
    if tail:
        digits = "".join(ch for ch in tail if ch.isdigit())
        non_tz = tail
        for tz in ("+", "-"):
            idx = non_tz.find(tz)
            if idx > 0:
                non_tz = non_tz[:idx]
                break
        digits = "".join(ch for ch in non_tz if ch.isdigit())[:6]
        tz_part = ""
        for tz in ("+", "-"):
            idx = tail.find(tz)
            if idx > 0:
                tz_part = tail[idx:]
                break
        raw = f"{head}.{digits}{tz_part}" if digits else f"{head}{tz_part}"
    try:
        return dt.datetime.fromisoformat(raw).timestamp()
    except ValueError:
        return 0.0


SCOUT_CATEGORIES = (
    "currency", "fragments", "runes", "essences", "ultimatum", "expedition",
    "ritual", "vaultkeys", "breach", "abyss", "uncutgems", "lineagesupportgems",
    "delirium", "incursion", "idol", "verisium", "vaal",
)


def scout_currency_prices(league: str, *, force: bool = False) -> dict[str, float]:
    """取「1 单位通货 = 多少崇高石」，失败返回空字典（调用方按 ninja 结果兜底）。

    结果按联盟缓存 30 分钟。⚠️ 这里踩过两个性能坑（2026-09-28），改之前先看懂：

    1. **必须单飞**。原来是「查缓存 → 释放锁 → 去抓」，并发抓 14 个类别时
       头 6 个线程会同时发现缓存是冷的，于是同一份**全联盟**数据被重复抓 6 遍，
       光这一项就让首轮卡 20 秒。现在整个抓取过程都在锁内完成，
       后到的线程等锁时缓存已经热了，直接拿走。
    2. **分页要并发**。17 个 scout 类别 + 逐页串行＝十几个请求排队，实测 13.75 秒；
       按类别并发（分页仍在各类别内部串行）后约 3 秒。
    """
    now = time.time()
    with _SCOUT_LOCK:
        cached = _SCOUT_CACHE.get(league)
        if cached and not force and now - cached[0] < SCOUT_TTL_SECONDS:
            return dict(cached[1])

        league_part = urllib.parse.quote(league)

        def fetch_one(
            category: str,
        ) -> tuple[dict[str, float], dict[str, float], dict[str, int], float]:
            """抓一个 scout 类别的全部页，返回 (价格, 库存, 数字id, 源更新时间)。"""
            sub_prices: dict[str, float] = {}
            sub_qty: dict[str, float] = {}
            sub_ids: dict[str, int] = {}
            sub_ts = 0.0
            page = 1
            while page <= 20:
                url = (
                    f"{SCOUT_API}/{SCOUT_REALM}/Leagues/{league_part}"
                    f"/Currencies/ByCategory?category={category}&perPage=100&page={page}"
                )
                try:
                    payload = http_json(url, retries=2)
                except Exception:  # noqa: BLE001 - 单个类别失败不影响其它类别
                    break
                items = payload.get("Items") or []
                for item in items:
                    api_id = item.get("ApiId")
                    price = item.get("CurrentPrice")
                    if not api_id:
                        continue
                    try:
                        raw_id = int(item.get("ItemId"))
                    except (TypeError, ValueError):
                        raw_id = 0
                    if raw_id:
                        sub_ids[str(api_id)] = raw_id

                    # 注意 PriceLogs 里偶尔夹 null，不能直接 .get
                    # 这里只取更新时间；间隔不从这里推断——
                    # ByCategory 的 PriceLogs 是每日一点（24h），但 CurrentPrice 实际 6 小时就变了，
                    # 用它算间隔会得到 24 小时，把抓取节奏拖得过长。间隔交给 scout_probe_interval。
                    for entry in (item.get("PriceLogs") or []):
                        if not isinstance(entry, dict):
                            continue
                        stamp = _parse_scout_time(entry.get("Time"))
                        if stamp > 0:
                            sub_ts = max(sub_ts, stamp)

                    if price is None:
                        continue
                    try:
                        value = float(price)
                    except (TypeError, ValueError):
                        continue
                    if value > 0:
                        sub_prices[str(api_id)] = value
                    # 顺手记下库存：倒货榜的「库存」列改用 scout 的 CurrentQuantity，
                    # 这里是唯一能批量拿到它的地方，别再单独发一轮请求。
                    try:
                        qty = float(item.get("CurrentQuantity") or 0.0)
                    except (TypeError, ValueError):
                        qty = 0.0
                    if qty > 0:
                        sub_qty[str(api_id)] = qty
                if len(items) < 100:
                    break
                page += 1
            return sub_prices, sub_qty, sub_ids, sub_ts

        prices: dict[str, float] = {}
        quantities: dict[str, float] = {}
        item_ids: dict[str, int] = {}
        # 这里只记录源数据的更新时间；更新间隔不在这里推断（见 scout_probe_interval）
        source_ts = 0.0
        with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as pool:
            for sub_prices, sub_qty, sub_ids, sub_ts in pool.map(
                fetch_one, SCOUT_CATEGORIES
            ):
                prices.update(sub_prices)
                quantities.update(sub_qty)
                item_ids.update(sub_ids)
                source_ts = max(source_ts, sub_ts)

        if source_ts:
            info = dict(_SCOUT_SOURCE.get(league) or {})
            info["aggregate_updated_at"] = int(source_ts)
            info["updated_at"] = max(int(info.get("updated_at") or 0), int(source_ts))
            _SCOUT_SOURCE[league] = info

        if prices:
            _SCOUT_CACHE[league] = (now, prices)
            _SCOUT_ITEM_IDS[league] = item_ids
            _SCOUT_QUANTITY[league] = (now, quantities)
        return dict(prices)


_SCOUT_PROBE_TTL = 6 * 3600
_SCOUT_PROBE: dict[str, tuple[float, dict[str, int]]] = {}


def scout_probe_interval(league: str) -> dict[str, int]:
    """探测 scout 两类数据各自的更新间隔（各一次请求，结果缓存 6 小时）。

    为什么要单独探测而不顺手用 ByCategory 的 PriceLogs：
    ByCategory 给的历史是「每日一点」，但它的 CurrentPrice 实际 6 小时就刷新，
    拿历史间隔当更新间隔会得出 24 小时，按它排的抓取节奏会慢得离谱。
    真正能反映刷新节奏的是 Currencies/{apiId} 的 PriceLogs（6 小时）
    和 Pairs 的 Epoch（1 小时）。
    """
    now = time.time()
    with _SCOUT_LOCK:
        cached = _SCOUT_PROBE.get(league)
        if cached and now - cached[0] < _SCOUT_PROBE_TTL:
            return dict(cached[1])

    league_part = urllib.parse.quote(league)
    result: dict[str, int] = {}

    def gaps_of(values: list[float], low: int, high: int) -> int:
        """相邻时间点差值的中位数；样本不足或异常一律返回 0。"""
        stamps = sorted((v for v in values if v > 0), reverse=True)
        gaps = [stamps[i - 1] - stamps[i] for i in range(1, len(stamps))]
        gaps = [g for g in gaps if low <= g <= high]
        if not gaps:
            return 0
        gaps.sort()
        return int(gaps[len(gaps) // 2])

    # 聚合价：Currencies/divine 的价格历史
    try:
        detail = http_json(
            f"{SCOUT_API}/{SCOUT_REALM}/Leagues/{league_part}/Currencies/divine", retries=2
        )
        stamps = [
            _parse_scout_time(e.get("Time"))
            for e in (detail.get("PriceLogs") or []) if isinstance(e, dict)
        ]
        aggregate = gaps_of(stamps, 60, 72 * 3600)
        if aggregate:
            result["interval_seconds"] = aggregate
    except Exception:  # noqa: BLE001
        pass

    # 换汇明细：Pairs 的 Epoch，严格 1 小时一个点
    try:
        pairs = http_json(
            f"{SCOUT_API}/{SCOUT_REALM}/Leagues/{league_part}"
            f"/Currencies/Pairs/291/290/History?limit=4", retries=2
        )
        epochs = [float(h.get("Epoch") or 0) for h in (pairs.get("History") or [])]
        detail_gap = gaps_of(epochs, 60, 72 * 3600)
        if detail_gap:
            result["detail_interval_seconds"] = detail_gap
    except Exception:  # noqa: BLE001
        pass

    if result:
        with _SCOUT_LOCK:
            _SCOUT_PROBE[league] = (now, result)
            info = dict(_SCOUT_SOURCE.get(league) or {})
            info.update(result)
            _SCOUT_SOURCE[league] = info
    return dict(result)


def scout_source_info(league: str) -> dict[str, int]:
    """scout 源数据的更新时间（Unix 秒）与更新间隔（秒）。

    拿不到就返回全 0，调用方据此判断要不要显示；不要拿本地快照时间冒充源时间。
    """
    with _SCOUT_LOCK:
        return dict(_SCOUT_SOURCE.get(league) or {"updated_at": 0, "interval_seconds": 0})


def scout_qty_status(league: str) -> dict:
    """挂出量 / 求购量来源的健康状况。

    这两个数**只有 poe2scout 一家给**（poe.ninja 只有成交量、doe 只有桥接对的量），
    所以它一停更就没有替代源，界面上会整片变成「—」。
    ⚠️ 那就必须把「源停更了」这件事明确说出来——否则用户看到一片「—」，
    只会以为程序坏了，而实际是上游挂了、程序正在如实报空。
    """
    try:
        ts = int(scout_source_info(league).get("updated_at") or 0)
    except Exception:  # noqa: BLE001 - 状态查询不该影响主流程
        ts = 0
    if not ts:
        return {"ok": False, "stale_hours": 0.0, "reason": "拿不到 poe2scout 的数据时间"}
    age = max(int(time.time()) - ts, 0)
    fresh = age <= SCOUT_MAX_AGE_SECONDS
    return {
        "ok": fresh,
        "stale_hours": round(age / 3600, 1),
        "reason": "" if fresh else f"挂出量来源 poe2scout 已 {age // 3600} 小时没更新，暂时无法提供实时挂单量",
    }


# 本轮实际生效的取价基准源（在 fetch_category 里写，接口读）。
# 为什么要记：界面上得能说清「现在这个价是谁给的、它多久没动了」——
# 否则用户看到一条直线，分不清是市场没动、还是源僵了、还是程序坏了。
_LAST_BASIS: dict[str, str] = {}


def price_status(league: str) -> dict:
    """当前取价基准源的健康状况：它是谁、它的价多久没变过。

    跟 `scout_qty_status()` 一个套路——把「上游不动了」明确说出来。
    ⚠️ 判据一律用**数据本身变没变**（价格指纹 / 汇率指纹），
       不看源自报的时间戳：dadsofexile 会「时间戳在动、价格不动」，
       poe.ninja 干脆不带时间戳。
    """
    basis = str(_LAST_BASIS.get(league) or "")
    if basis == "doe":
        _frozen, held = doe_frozen(league)
        label = "dadsofexile"
    elif basis == "ninja":
        _frozen, held = ninja_frozen(league)
        label = "poe.ninja"
    elif basis == "scout":
        try:
            ts = int(scout_source_info(league).get("updated_at") or 0)
        except Exception:  # noqa: BLE001
            ts = 0
        held = max(int(time.time()) - ts, 0) if ts else 0
        label = "poe2scout"
    else:
        return {"ok": True, "basis": "", "held_minutes": 0, "reason": ""}

    held_min = held // 60
    if held_min < 60:
        return {"ok": True, "basis": basis, "held_minutes": held_min, "reason": ""}
    hours = held_min // 60
    return {
        "ok": False,
        "basis": basis,
        "held_minutes": held_min,
        "reason": f"当前取价源 {label} 的价格已 {hours} 小时没有变化，"
                  f"显示的可能是 {hours} 小时前的行情",
    }


def scout_quantities(league: str) -> dict[str, float]:
    """scout 给的库存（ByCategory 的 CurrentQuantity）。

    跟价格走同一份缓存：价格是 30 分钟一刷，库存跟着刷就够了。
    取不到就返回空字典，调用方拿 doe 的库存兜底。
    """
    with _SCOUT_LOCK:
        cached = _SCOUT_QUANTITY.get(league)
    if not cached:
        return {}
    return dict(cached[1])


def scout_pair_stocks(
    league: str, *, force: bool = False
) -> tuple[dict[str, float], dict[str, float], dict[str, int]]:
    """一次拿全交易所的两个方向，返回 (挂出量, 求购量, 参与交易对数)。

    ⚠️ 关键：交易所的每个「交易对」是**双向**的，两侧各有一个挂出量，
    而玩家在游戏里看到的数字取决于他选的方向——这是之前一直对不上的根因：

        Exalted Orb <-> Omen of the Hunt（实测 2026-09-24 10:00 快照）
            Omen of the Hunt 侧挂出 = 0        「用崇高石买它」→ 没人挂
            Exalted Orb     侧挂出 = 20,095   「用它换崇高石」→ 约两万

    所以：
      · 挂出量 ask  = 该通货自己在所有交易对里挂出的数量之和
                     = 你想**买**它时，市场上能买到多少
      · 求购量 bid  = 所有交易对里，对手挂出来等着收它的通货数量之和
                     = 你想**卖**它时，有多少通货在那儿接着
      · 交易对数    = 它参与了多少个交易对

    为什么不再只查「对崇高石」那一个交易对：
      Pairs/{a}/{b} 是 a 与 b 这一对的市场快照（BaseCurrencyApiId 恒为 exalted，
      它只是计价单位）。只查那一对等于只看了市场的一小块：
        神圣石旧口径 3030，实际跨 268 个交易对合计 119 万。
      更糟的是崇高石自己：`Pairs/{exalted}/{exalted}` 不存在，旧代码直接跳过 → 恒 0。
    （HighestStock 是「挂出的物品数量」，不是挂单笔数——笔数这些源根本不给。）
    """
    now = time.time()
    with _SCOUT_LOCK:
        cached = _SCOUT_PAIRS.get(league)
    if cached and not force and now - cached[0] < SCOUT_PAIRS_TTL:
        return dict(cached[1]), dict(cached[2]), dict(cached[3])

    league_part = urllib.parse.quote(league)
    try:
        payload = http_json(
            f"{SCOUT_API}/{SCOUT_REALM}/Leagues/{league_part}/SnapshotPairs",
            retries=2,
            timeout=60,
        )
    except Exception:  # noqa: BLE001 - 拿不到就返回空，调用方回退 doe
        return {}, {}, {}
    if not isinstance(payload, list) or not payload:
        return {}, {}, {}

    ask: dict[str, float] = {}
    bid: dict[str, float] = {}
    count: dict[str, int] = {}

    def qty(src: dict) -> float:
        try:
            return float(src.get("HighestStock") or 0.0)
        except (TypeError, ValueError):
            return 0.0

    for pair in payload:
        if not isinstance(pair, dict):
            continue
        sides = (
            ((pair.get("CurrencyOne") or {}).get("ApiId"), pair.get("CurrencyOneData") or {}),
            ((pair.get("CurrencyTwo") or {}).get("ApiId"), pair.get("CurrencyTwoData") or {}),
        )
        for index, (api_id, data) in enumerate(sides):
            if not api_id:
                continue
            _, other_data = sides[1 - index]
            stock = qty(data)
            other_stock = qty(other_data)
            key = str(api_id)
            # 挂出量：自己挂了多少出去（想买它能买到多少）
            if stock > 0:
                ask[key] = ask.get(key, 0.0) + stock
            # 求购量：对手挂了多少通货等着收它（想卖它有多少人接）。
            # 注意这里不要求自己那一侧有货——实测 Omen of the Hunt 本方 0、
            # 对方 20095，正是用户能在游戏里看到却在我们这儿显示为 0 的那个数。
            if other_stock > 0:
                bid[key] = bid.get(key, 0.0) + other_stock
            count[key] = count.get(key, 0) + 1

    if ask or bid:
        with _SCOUT_LOCK:
            _SCOUT_PAIRS[league] = (now, ask, bid, count)
    return dict(ask), dict(bid), dict(count)


# 基准货币小时级成交均价的缓存（只用于同轮去重，见 scout_refresh_bases）
_SCOUT_BASES: dict[str, tuple[float, dict[str, float]]] = {}
SCOUT_BASES_TTL = 60


def scout_refresh_bases(league: str, prices: dict[str, float]) -> dict[str, float]:
    """用换汇明细把基准货币的价格刷新到小时级。

    Currencies/ByCategory 是 6 小时粒度，而神圣石这种主币一天能波动几个百分点，
    6 小时的滞后会直接算错整张表。Currencies/Pairs 是 1 小时粒度，
    拿「该货币 ↔ 崇高石」最近一小时的成交额 / 成交数量，就是这一小时的实际成交均价。
    只对基准货币做，两个请求就够，其余通货沿用 6 小时的聚合价。
    """
    if not prices:
        return prices

    # ⚠️ 同轮去重：这个函数每个类别都会调一次，一轮 14 个类别就是 28 个请求，
    #    而它取的「基准货币成交均价」跟类别毫无关系。
    #    TTL 只给 60 秒——刚好覆盖一轮抓取（约 6 秒），下一轮必定重新取，
    #    所以它仍然保持小时级的时效，只是不再把同一份数据重复取 14 遍。
    with _SCOUT_LOCK:
        cached = _SCOUT_BASES.get(league)
        if cached and time.time() - cached[0] < SCOUT_BASES_TTL:
            merged = dict(prices)
            merged.update(cached[1])
            return merged
        item_ids = dict(_SCOUT_ITEM_IDS.get(league) or {})
    base_id = item_ids.get(SCOUT_BASE_CURRENCY)
    if not base_id:
        return prices

    league_part = urllib.parse.quote(league)
    updated: dict[str, float] = {}
    for name in ("divine", "chaos"):
        target = item_ids.get(name)
        if not target or target == base_id:
            continue
        url = (
            f"{SCOUT_API}/{SCOUT_REALM}/Leagues/{league_part}"
            f"/Currencies/Pairs/{target}/{base_id}/History?limit=1"
        )
        try:
            payload = http_json(url, retries=1)
        except Exception:  # noqa: BLE001 - 拿不到就沿用 6 小时价
            continue
        history = payload.get("History") or []
        if not history:
            continue
        # 换汇明细是 1 小时粒度，比 6 小时的聚合价新，源时间以它为准
        epoch = int(history[0].get("Epoch") or 0)
        if epoch > 0:
            with _SCOUT_LOCK:
                info = dict(_SCOUT_SOURCE.get(league) or {})
                info["updated_at"] = max(int(info.get("updated_at") or 0), epoch)
                _SCOUT_SOURCE[league] = info
        data = (history[0].get("Data") or {})
        # 明细里两条分别是两个货币，按 CurrencyItemId 认准自己那一条
        for side in (data.get("CurrencyOneData") or {}, data.get("CurrencyTwoData") or {}):
            if int(side.get("CurrencyItemId") or 0) != target:
                continue
            traded = float(side.get("ValueTraded") or 0.0)
            volume = float(side.get("VolumeTraded") or 0.0)
            if traded > 0 and volume > 0:
                updated[name] = traded / volume

    with _SCOUT_LOCK:
        _SCOUT_BASES[league] = (time.time(), updated)
    merged = dict(prices)
    merged.update(updated)
    return merged


# ------------------------------------------------------- dadsofexile 通货报价
# dadsofexile 直接扒游戏内货币交易所的订单簿（`price_source=exchange-direct`），
# 几分钟刷新一次，比 poe2scout 的 6 小时聚合快得多，也更贴实际成交价。
#
# ⚠️ 但它的条目必须过滤，实测有两类假价：
#   1. 约 44 条价格恰好等于 1.00 —— 拿不到价时的占位
#   2. 约 18 条价格恰好等于 chaos 或 divine 的价 —— 拿主币顶替
# 这两类照样标着 exchange-direct / exchange-bridged，从 price_source 看不出来，
# 所以判定可信度只能看 order_book（挂单条数）。
DOE_TTL_SECONDS = 4 * 60      # 它刷新很快，缓存别开太久，否则白瞎了它的优势
DOE_MIN_ORDERS = 100          # 挂单少于这个数，价格不采信（实测 p50 只有 57）
DOE_TIMEOUT = 15              # 个人小站，别让它卡住整轮抓取

# ★★ 桥接价（exchange-bridged）：交易所里没有直接挂单，但 doe 用桥接汇率给了一个实时价。
#
# 2026-09-29 事故的根子就在这里：原先的采信条件只有 `order_book >= 100`，
# 而 bridged 条目**按定义** order_book 就是 0（它压根不在通货交易所直接挂单），
# 于是这 154 条全被判不可用 → 逐条取价降级到 poe2scout → 而 scout 已停更 22 小时
# → 这些通货的价格、挂出量、求购量整体冻结（用户截图里那条从 09-28 15:12 起
#   一动不动的直线就是这个）。
#
# 拿 poe.ninja 当基准交叉验证过（只取 ninja 以神圣石计价、量级可信的那批）：
#   · 59 个 bridged 条目里，与 ninja 的中位偏差 4.4%，52.5% 落在 5% 以内；
#   · 贵重物品尤其准：mirror 0.46%、hinekoras-lock 0.62%、uul-netols-embrace 0.28%。
#   · 对照：现行采信档（order_book>=100）同一口径下中位偏差 14.0%。
# 所以 bridged 价是**可以采信**的，而且它是「有价可用」和「拿停更源的旧价」之间的分水岭。
# 仍保留占位假价过滤（价格恰好=1 / =chaos / =divine 的一律不采）。
DOE_BRIDGED_SRC = "exchange-bridged"
# 数据来源：https://dadsofexile.com/api/prices 的 price_source 字段

DOE_INTERVAL_SECONDS = 30 * 60   # 实测几分钟到二十分钟刷一次，取 30 分钟当保守标称
# 标记间隔是 30 分钟，超过这个时长还没刷新就认为它「僵住了」：
# 它是个人小站，抽风时会一直返回同一份陈旧数据（价格看着正常但其实不动了）。
# 一旦判定陈旧，取价和库存/挂单都整体回退 poe2scout，不等它自己恢复。
DOE_STALE_SECONDS = _clamp_int(
    CONFIG.get("doe_stale_seconds", 90 * 60), 90 * 60, 10 * 60, 24 * 3600
)
# 「价格指纹」连续不变的容忍时长：超过它就判定 doe 在返回僵数据。
# ⚠️ 阈值必须明显大于 doe 自己的刷新间隔。实测它正常时 40~100 分钟整体刷一次，
#    凌晨最久一次隔了 398 分钟；而它僵住时是**整整 8 小时一个小数位都不动**。
#    取 2.5 小时：正常刷新不会误伤，真僵了也能在两个半小时内切走。
#    （2026-09-28 第一次设成 45 分钟，结果每次 doe 正常待着就被误判成僵，
#     反复在 doe / scout 之间横跳，神圣石汇率跟着抖出 10% 的台阶。）
#   下限压在 2 小时：早先写过 45 分钟的那批 config.json 会被夹上来，
#   否则老用户一升级反而更容易被误判。
DOE_FROZEN_SECONDS = _clamp_int(
    CONFIG.get("doe_frozen_seconds", 150 * 60), 150 * 60, 120 * 60, 24 * 3600
)
# 一轮里至少有多大比例的「可用条目」价格变了，才算 doe 真的刷新过。
# 它一次正常刷新会让几百项一起变（实测 500~650 项）；僵住时只有零星几项在动。
DOE_REFRESH_MIN_RATIO = 0.02

_DOE_LOCK = threading.RLock()
_DOE_CACHE: dict[str, tuple[float, dict[str, dict]]] = {}
_DOE_SOURCE: dict[str, int] = {}   # 联盟 -> 源采集时间（Unix 秒）


def doe_source_info(league: str) -> dict[str, int]:
    """dadsofexile 的采集时间。它不给刷新间隔，用实测的标称值补上。"""
    with _DOE_LOCK:
        ts = int(_DOE_SOURCE.get(league) or 0)
    return {"updated_at": ts, "interval_seconds": DOE_INTERVAL_SECONDS if ts else 0}


def doe_stale(league: str) -> tuple[bool, int]:
    """dadsofexile 是不是「僵住了」：返回 (是否陈旧, 数据已经多久没动)。

    拿不到 fetched_at 也算陈旧——连时间都没有的数据没法信。
    判定陈旧后调用方应当整体回退 scout，而不是继续用一份不动的旧数据。
    """
    info = doe_source_info(league)
    ts = int(info.get("updated_at") or 0)
    if not ts:
        return True, 0
    age = int(time.time()) - ts
    return age > DOE_STALE_SECONDS, max(age, 0)


# --------------------------------------------------------------------------
# 「僵住」的第二道判据：价格指纹
#
# ⚠️ 2026-09-28 实测踩出来的：doe 抽风时会返回**时间戳在动、价格不动**的数据——
#    fetched_at 每 15 分钟照常往前走，神圣石却 7 小时一个小数位都没变
#    （15:27 到 22:06 共 54 个快照全是 482.9223…），期间 629 项数据纹丝不动。
#    上面 doe_stale() 只看时间戳，这种情况永远判不出"僵"，
#    程序就一直把这份复印件当新数据用。
#
# 教训：**数据源自报的时间戳不可信，要判断它是否僵住只能看数据本身变没变。**
# 所以这里对每次拿到的价格算一个指纹，记住"这份指纹第一次是什么时候见到的"；
# 只要指纹长时间不变，不管 fetched_at 多新鲜，都判僵住并回退 scout。
# --------------------------------------------------------------------------
_DOE_FP: dict[str, tuple[str, float]] = {}    # 联盟 -> (价格指纹, 首次见到该指纹的时刻)
_DOE_LAST: dict[str, dict[str, float]] = {}  # 联盟 -> 上一轮各可用条目的价格
_DOE_FP_LOCK = threading.Lock()
_DOE_STATE_FILE = DATA_DIR / "doe_freshness.json"
_DOE_STATE_LOADED = False
_DOE_NOISE_TS: dict[str, float] = {}


def _doe_usable(flat: dict[str, dict]) -> dict[str, float]:
    """只挑出「真的会被拿来取价」的条目（ok=True）来算指纹。

    ⚠️ 2026-09-28 实测踩的坑（v1.27.5 修了却没修好的那个）：
       原来对 doe 返回的**全部**条目算指纹，结果它「交易所」那部分价格
       从 15:27 起整整 8 小时一个小数位都没动，但每轮总有 20 来个条目在变——
       那些是 doe 压根没有（灵魂核心、三维宝珠之类）或只标 scout-blend 的，
       走的是 scout，跟着 scout 十几分钟动一次。
       于是全局指纹每隔十几分钟就被重置，「僵住」判定永远差一点点、
       一次都没触发过，程序就这么把一份 8 小时前的复印件当实时价用了 8 小时。
       所以指纹只能覆盖**我们真的从 doe 取价**的那部分（实测 635 条里 232 条）。
    """
    # 没带 ok 标记的（调用方直接喂的一份价格表）一律当可用。
    return {
        key: float(val.get("price") or 0.0)
        for key, val in flat.items()
        if isinstance(val, dict) and val.get("ok", True)
    }


def _doe_hash_prices(prices: dict[str, float]) -> str:
    """对一份「通货 -> 价格」算指纹（只看价格，不看时间戳）。"""
    try:
        import hashlib

        digest = hashlib.sha1()
        for key in sorted(prices):
            digest.update(f"{key}:{prices[key]!r};".encode("utf-8"))
        return digest.hexdigest()
    except Exception:  # noqa: BLE001
        return ""


def _doe_fingerprint(flat: dict[str, dict]) -> str:
    """对一批报价算指纹：只看价格、不看时间戳，且只看会被采用的那部分。"""
    return _doe_hash_prices(_doe_usable(flat))


def _doe_state_load() -> None:
    """把上次记下的指纹读回来，免得重启后要白等一个判定周期。

    doe 一僵就是好几个小时，而判据是「这份指纹多久没变」；不落盘的话
    每次启动都从零开始计时，偏偏刚打开程序这段时间最容易被僵数据糊脸。
    只有在指纹还对得上时才认这份记录——doe 若已经刷过，指纹不同会自动重置。
    """
    global _DOE_STATE_LOADED
    if _DOE_STATE_LOADED:
        return
    _DOE_STATE_LOADED = True
    try:
        with open(_DOE_STATE_FILE, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, ValueError):
        return
    if not isinstance(data, dict):
        return
    now = time.time()
    for league, entry in data.items():
        if not isinstance(entry, dict):
            continue
        fp = str(entry.get("fp") or "")
        seen = float(entry.get("seen") or 0)
        if fp and 0 < seen <= now:
            _DOE_FP[str(league)] = (fp, seen)


def _doe_state_save() -> None:
    """把指纹写盘。写失败无所谓，最坏是下次启动多等一个周期。"""
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        payload = {lg: {"fp": fp, "seen": seen} for lg, (fp, seen) in _DOE_FP.items()}
        # 用字符串拼而不是 with_suffix：调用方（自检）可能把路径换成 str
        tmp = Path(f"{_DOE_STATE_FILE}.tmp")
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        os.replace(tmp, _DOE_STATE_FILE)
    except OSError:
        pass


def doe_note_prices(league: str, flat: dict[str, dict]) -> None:
    """每次真正取到 doe 数据后记一笔指纹：价格**整体**变过才重置计时。

    ⚠️ 「变过」必须看变动比例，不能只看指纹相不相同：
       僵住时照样有零星几条在动（走 scout 的那些），指纹一变计时就被重置。
       doe 一次真刷新是几百项一起动（实测 500~650 项），
       个位数变化只能算噪音，不算刷新。
    """
    usable = _doe_usable(flat)
    if not usable:
        return
    fp = _doe_hash_prices(usable)
    if not fp:
        return
    now = time.time()
    with _DOE_FP_LOCK:
        _doe_state_load()
        prev_prices = _DOE_LAST.get(league) or {}
        changed = sum(
            1
            for key, value in usable.items()
            if key not in prev_prices or abs(prev_prices[key] - value) > 1e-9
        )
        _DOE_LAST[league] = usable
        prev = _DOE_FP.get(league)
        if prev is None:
            _DOE_FP[league] = (fp, now)
            _doe_state_save()
            return
        if prev[0] == fp:
            return  # 一点没变，继续累计僵住时长
        ratio = changed / max(len(usable), 1)
        if ratio < DOE_REFRESH_MIN_RATIO:
            if now - float(_DOE_NOISE_TS.get(league) or 0) > 30 * 60:
                _DOE_NOISE_TS[league] = now
                log(f"  · dadsofexile 本轮只有 {changed}/{len(usable)} 项价格变动"
                    f"（{ratio:.1%}），达不到有效刷新门槛，不计入刷新")
            return
        _DOE_FP[league] = (fp, now)
        _doe_state_save()


def doe_frozen(league: str) -> tuple[bool, int]:
    """dadsofexile 是不是「价格僵住了」：返回 (是否僵住, 已僵多久/秒)。

    与 doe_stale() 互补：那个看时间戳新旧，这个看**价格本身**有没有变。
    两者任一成立都应回退 scout —— 只有时间戳在动、价格不动这种情况，
    光看时间戳是抓不到的：它自报的 fetched_at 一直很新，
    实测僵了 8 小时也照样写着「1 分钟前才抓的」，完全不能信。
    """
    with _DOE_FP_LOCK:
        _doe_state_load()
        entry = _DOE_FP.get(league)
    if not entry:
        return False, 0
    _fp, first_seen = entry
    held = int(time.time() - first_seen)
    return held >= DOE_FROZEN_SECONDS, max(held, 0)


# --------------------------------------------------------------------------
# poe.ninja 的「活跃度」判据
#
# 2026-09-29 的死结逼出这套东西：
#   · poe2scout 全站停更 24.7 小时（三个活跃联盟的 Epoch 全卡在同一时刻）
#   · dadsofexile 价格僵住 23 小时（divine 汇率死在 482.92，
#     比用户游戏内核对过的真值 535 低 9.7%）
#   · 唯一还可能活着的是 poe.ninja —— 但「回退守卫」fallback_ok 是拿
#     **scout 的时间戳**去比 doe 的，scout 停更后它永远更旧
#     → 永远判「备用源更旧」→ 永不回退 → 程序被锁死在那份差 9.7% 的僵数据上。
#
# 所以 scout 停更时，守卫该问的是「ninja 活着吗」，不是「scout 新不新」。
# ninja 的响应**不带时间戳**（实测确认，core 里只有 rates/primary），
# 只能跟 doe 一样看数据本身变没变：对 core.rates 算指纹，长时间不变就判它也不动了。
# --------------------------------------------------------------------------
_NINJA_FP: dict[str, tuple[str, float]] = {}   # 联盟 -> (汇率指纹, 首次见到该指纹的时刻)
_NINJA_FP_LOCK = threading.Lock()
_NINJA_STATE_FILE = DATA_DIR / "ninja_freshness.json"
_NINJA_STATE_LOADED = False

# ninja 是小时级刷新（实测 6~82 分钟），阈值必须明显大于它，
# 否则它正常待着就被误判成不动。默认 6 小时。
NINJA_FROZEN_SECONDS = _clamp_int(
    CONFIG.get("ninja_frozen_seconds", 6 * 3600), 6 * 3600, 3600, 48 * 3600
)


def _ninja_state_load() -> None:
    global _NINJA_STATE_LOADED
    if _NINJA_STATE_LOADED:
        return
    _NINJA_STATE_LOADED = True
    try:
        with open(_NINJA_STATE_FILE, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, ValueError):
        return
    if not isinstance(data, dict):
        return
    now = time.time()
    for league, entry in data.items():
        if not isinstance(entry, dict):
            continue
        fp = str(entry.get("fp") or "")
        seen = float(entry.get("seen") or 0)
        if fp and 0 < seen <= now:
            _NINJA_FP[str(league)] = (fp, seen)


def _ninja_state_save() -> None:
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        payload = {lg: {"fp": fp, "seen": seen} for lg, (fp, seen) in _NINJA_FP.items()}
        tmp = Path(f"{_NINJA_STATE_FILE}.tmp")
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        os.replace(tmp, _NINJA_STATE_FILE)
    except OSError:
        pass


def ninja_note_rates(league: str, rates: dict) -> None:
    """每次拿到 poe.ninja 的汇率就记一笔指纹：汇率变了才算它刷新过。"""
    if not rates:
        return
    try:
        fp = _doe_hash_prices({str(k): float(v) for k, v in rates.items()})
    except (TypeError, ValueError):
        return
    if not fp:
        return
    now = time.time()
    with _NINJA_FP_LOCK:
        _ninja_state_load()
        prev = _NINJA_FP.get(league)
        if prev is None or prev[0] != fp:
            _NINJA_FP[league] = (fp, now)
            _ninja_state_save()


def ninja_frozen(league: str) -> tuple[bool, int]:
    """poe.ninja 的汇率是不是也僵住了：返回 (是否僵住, 已僵多久/秒)。

    没有历史时返回 False —— 刚启动不该用它去拦回退，
    那会把「还不知道 ninja 死没死」误当成「ninja 也没动」。
    """
    with _NINJA_FP_LOCK:
        _ninja_state_load()
        entry = _NINJA_FP.get(league)
    if not entry:
        return False, 0
    held = int(time.time() - entry[1])
    return held >= NINJA_FROZEN_SECONDS, max(held, 0)


def pick_ask_bid(
    cid: str,
    pair_ask: dict[str, float],
    pair_bid: dict[str, float],
    scout_qty: dict[str, float],
    doe_row: dict | None,
    doe_is_stale: bool = False,
) -> tuple[float, float]:
    """买卖两侧的挂出量，返回 (挂出量 ask, 求购量 bid)。

    两个数都来自 SnapshotPairs，是「物品/通货的数量」不是「挂单笔数」：
      · ask 挂出量 ← 该通货自己在所有交易对里挂出的总数 = 你想买能买到多少
      · bid 求购量 ← 所有交易对里对手挂出来收它的通货总数 = 你想卖有多少人接

    为什么必须两个都留（这是用户实测踩出来的坑）：
      交易所的每一对是双向的，两侧数字完全独立。Omen of the Hunt ↔ 崇高石
      这一对里，预兆侧是 0、崇高石侧是 20,095。只留一个方向，
      玩家在游戏里换一边看就对不上了——之前就是这样被判成「数据不准」。

    降级顺序：scout SnapshotPairs → scout ByCategory 库存 → doe → 0。
    doe 僵住时不拿它兜底，避免旧数据冒充实时量；都没数就如实存 0（前端显示「—」）。

    ⚠️ doe 的桥接价条目（qty_ok=False）也不许兜底：它的 quantity / order_book
       是**桥接交易对**的量，不是本物品挂了多少，拿它当挂出量就是又混了一次口径。
    """
    src = None if doe_is_stale else doe_row
    if src is not None and not src.get("qty_ok", True):
        src = None

    def first(*values: float) -> float:
        for value in values:
            if value and value > 0:
                return float(value)
        return 0.0

    # 挂出量有兜底：ByCategory 的 CurrentQuantity 虽然语义不完全一样，
    # 但量级对得上「市场上有多少货」，scout 全盘拿不到时还能顶一下。
    ask = first(
        pair_ask.get(cid, 0.0),
        scout_qty.get(cid, 0.0),
        float((src or {}).get("stock") or 0.0),
        float((src or {}).get("orders") or 0.0),
    )
    # 求购量**没有兜底**：doe 只给「库存」和「挂单笔数」，这两个都不是
    # 「对手挂了多少通货在收它」，拿它当求购量就等于又混了一次口径 ——
    # 这正是之前数字对不上的根源。拿不到就如实存 0（前端显示「—」）。
    bid = pair_bid.get(cid, 0.0)
    return ask, max(bid, 0.0)


def _parse_doe_time(text: str) -> int:
    """把 doe 的 fetched_at（如 2026-09-23T17:23:38.377Z）转成 Unix 秒。"""
    if not text:
        return 0
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
    except (TypeError, ValueError):
        return 0


def doe_placeholder(price: float, chaos: float, divine: float) -> bool:
    """价格是不是「拿不到真实价」的占位/顶替值。"""
    if abs(price - 1.0) < 0.02:
        return True
    for ref in (chaos, divine):
        if ref and abs(price - ref) < 0.01:
            return True
    return False


def doe_prices(league: str, *, force: bool = False) -> dict[str, dict]:
    """取 dadsofexile 全量报价，返回 {api_id: {price, stock, orders, src, ok}}。

    `ok` 已经把「挂单太少」和「占位假价」两种情况排除掉了，调用方直接用即可。
    任何环节出错都返回空 dict，让上层回落到 scout —— 它是个人小站，可能随时挂。
    """
    now = time.time()
    with _DOE_LOCK:
        cached = _DOE_CACHE.get(league)
        if cached and not force and now - cached[0] < DOE_TTL_SECONDS:
            return {k: dict(v) for k, v in cached[1].items()}

    try:
        payload = http_json(
            f"{DOE_API}?league={urllib.parse.quote(league)}",
            retries=2,
            timeout=DOE_TIMEOUT,
        )
    except Exception:  # noqa: BLE001 - 小站不可用时静默回落
        return {}

    raw = payload.get("prices") or {}
    entries = list(raw.values()) if isinstance(raw, dict) else list(raw)

    flat: dict[str, dict] = {}
    for item in entries:
        if not isinstance(item, dict):
            continue
        api_id = str(item.get("api_id") or "")
        price = float(item.get("price_exalted") or 0.0)
        if not api_id or price <= 0:
            continue
        flat[api_id] = {
            "price": price,
            "stock": float(item.get("quantity") or 0.0),
            "orders": int(item.get("order_book") or 0),
            "src": str(item.get("price_source") or ""),
            # 桥接价的成交量（桥接交易对的量，不是本物品的量）——
            # 只用来判断「这条桥接价有没有真实市场支撑」，不对外展示。
            "exchange_volume": float(item.get("exchange_volume") or 0.0),
        }

    if not flat:
        return {}

    # 兜底值判定要用 chaos / divine 的价当参照。
    # ⚠️ 基准货币必须豁免这个检测：exalted 的价本来就是 1.0，
    # divine / chaos 的价也必然等于它们自己，照常套检测会把三把尺子全判成假价
    # （踩过：于是它们回落到 scout 价，却仍在用 doe 的系数换算，两套基准直接打架，
    #   神圣石算出 559 而系数是 512）。
    chaos = float((flat.get("chaos") or {}).get("price") or 0.0)
    divine = float((flat.get("divine") or {}).get("price") or 0.0)
    for key, entry in flat.items():
        if key in ("divine", "chaos", "exalted"):
            entry["ok"] = entry["price"] > 0
            entry["qty_ok"] = True
            continue
        # 占位假价（恰为 1.00 / chaos / divine）一律不采信，跟挂单多少无关
        if doe_placeholder(entry["price"], chaos, divine):
            entry["ok"] = False
            entry["qty_ok"] = False
            continue
        # 挂单够多 → 直接采信，且它的 stock/orders 是可信的「本物品」数量
        if entry["orders"] >= DOE_MIN_ORDERS:
            entry["ok"] = True
            entry["qty_ok"] = True
            continue
        # 桥接价：order_book 恒为 0，但 doe 用桥接汇率给了实时价（见 DOE_BRIDGED_SRC 注释）。
        # ⚠️ 这种条目的 quantity / order_book 是**桥接交易对**的量，不是本物品的，
        #    所以只采信价格，qty_ok=False —— 挂出量/求购量不能让它们冒充。
        if entry["src"] == DOE_BRIDGED_SRC and entry["exchange_volume"] > 0:
            entry["ok"] = True
            entry["qty_ok"] = False
            continue
        entry["ok"] = False
        entry["qty_ok"] = False

    fetched = _parse_doe_time(str(payload.get("fetched_at") or ""))
    with _DOE_LOCK:
        _DOE_CACHE[league] = (now, flat)
        if fetched:
            _DOE_SOURCE[league] = fetched
    # 记下这批价格的指纹：下次比对就知道 doe 到底有没有真的刷新过
    doe_note_prices(league, flat)
    return {k: dict(v) for k, v in flat.items()}


class TradeRateLimited(RuntimeError):
    """官方接口明确要求等待时抛出，调用方应当直接放弃本轮而不是继续打接口。"""


class TradeLimiter:
    """官方交易接口的全局节拍器。

    429 之后必须按官方给的秒数老实等着。之前一边罚等一边继续请求，
    结果罚时越滚越长（实测 60s → 170s），整晚只写进十几条数据。
    现在所有请求串行通过这里，罚等期间谁都不许发。
    """

    def __init__(self, gap: float) -> None:
        self.gap = gap
        self.lock = threading.RLock()
        self.next_ok = 0.0          # 下一次允许发请求的时刻
        self.penalty_until = 0.0
        self.last_error = ""

    def wait_turn(self) -> None:
        with self.lock:
            delay = self.next_ok - time.time()
        if delay > 0:
            time.sleep(delay)

    def note_ok(self) -> None:
        with self.lock:
            self.next_ok = time.time() + self.gap
            self.last_error = ""

    def note_penalty(self, seconds: float, message: str) -> None:
        with self.lock:
            self.penalty_until = time.time() + max(seconds, self.gap)
            self.next_ok = max(self.next_ok, self.penalty_until)
            self.last_error = message

    def penalized(self) -> bool:
        with self.lock:
            return self.penalty_until > time.time()

    def snapshot(self) -> dict:
        with self.lock:
            now = time.time()
            waiting = max(0.0, self.next_ok - now)
            return {
                "waiting": round(waiting, 1),
                "penalty_until": int(self.penalty_until),
                "last_error": self.last_error if self.penalty_until > now else "",
            }


TRADE_LIMITER = TradeLimiter(SPREAD_REQUEST_GAP)
_TRADE_GATE = threading.RLock()  # 同一时刻只允许一个请求打官方接口


def trade_post(path: str, payload: dict, *, timeout: int = 25, limiter: "TradeLimiter | None" = None):
    """向官方交易接口 POST 请求，遇到速率限制时按官方要求的等待时间老实等。

    `limiter` 用来换节拍器：暗金集市查询走自己的 MARKET_LIMITER（间隔更短），
    差值扫描走全局 TRADE_LIMITER。不传就用全局的。
    """
    gate = limiter or TRADE_LIMITER
    url = f"{TRADE_BASE}{path}"
    last: Exception | None = None
    with _TRADE_GATE:  # 手动扫描和自动扫描共用一个出口，避免互相踩
        for _attempt in range(SPREAD_RETRIES):
            gate.wait_turn()
            try:
                request = urllib.request.Request(
                    url,
                    data=json.dumps(payload).encode("utf-8"),
                    headers={
                        "User-Agent": USER_AGENT,
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                    },
                    method="POST",
                )
                with _HTTP_OPENER.open(request, timeout=timeout) as response:
                    data = json.loads(response.read().decode("utf-8", "ignore"))
                gate.note_ok()
                return data
            except urllib.error.HTTPError as exc:
                last = exc
                if exc.code == 429:
                    # 不重试：官方罚等动辄一两分钟，连着撞只会把罚时越滚越长。
                    # 记下解禁时间、立刻收工，等下一轮再来看看。
                    hold = _retry_after_seconds(exc)
                    gate.note_penalty(hold, f"官方限流 {hold:.0f}s")
                    log(f"  · 交易接口限流，按官方要求等待 {hold:.0f}s 再继续")
                    raise TradeRateLimited(f"官方限流 {hold:.0f}s") from exc
                break
            except Exception as exc:  # noqa: BLE001
                last = exc
                gate.note_penalty(max(gate.gap, SPREAD_REQUEST_GAP) * 2, str(exc))
    if gate.penalized():
        raise TradeRateLimited(gate.snapshot().get("last_error") or "交易接口限流中")
    raise RuntimeError(f"交易接口请求失败 {url} -> {last}")


class SpreadJob:
    """后台扫描真实挂单的任务，避免请求被长时间阻塞。"""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.state = {
            "running": False,
            "started_at": 0,
            "finished_at": 0,
            "written": 0,
            "error": "",
            "rate_limited": False,
        }

    def start_scan(self, limit: str) -> dict:
        top_n = _safe_int(limit, SPREAD_TOP_N, 5, 120)
        limiter = TRADE_LIMITER.snapshot()
        with self.lock:
            if self.state["running"]:
                return {**self.state, **limiter, "ok": False, "reason": "扫描正在进行中"}
            if limiter["waiting"] > 0:
                return {
                    **self.state,
                    **limiter,
                    "ok": False,
                    "reason": f"官方接口限流中，还需等待 {limiter['waiting']:.0f} 秒",
                }
            self.state.update(
                {
                    "running": True,
                    "started_at": int(time.time()),
                    "error": "",
                    "written": 0,
                    "rate_limited": False,
                }
            )
        threading.Thread(
            target=self._run, args=(top_n,), name="spread-scan", daemon=True
        ).start()
        return {**self.state, "ok": True, "limit": top_n}

    def _run(self, top_n: int) -> None:
        try:
            written = build_spread_snapshot(STATE["league"], top_n, force=True)
            with self.lock:
                self.state.update(
                    {"running": False, "finished_at": int(time.time()), "written": written}
                )
            log(f"  ✓ 手动扫描完成，写入 {written} 组挂单")
        except Exception as exc:  # noqa: BLE001
            with self.lock:
                self.state.update(
                    {"running": False, "finished_at": int(time.time()), "error": str(exc)}
                )
            log(f"  × 手动扫描失败：{exc}")

    def status(self) -> dict:
        with self.lock:
            return {**self.state, **TRADE_LIMITER.snapshot()}


SPREAD_JOB = SpreadJob()


def _retry_after_seconds(exc: urllib.error.HTTPError) -> float:
    """从 429 响应里解析官方要求等待的秒数。

    官方同时给了两种提示：响应头 Retry-After，以及正文里的
    "Rate limit exceeded; Please wait 74 seconds before trying again."
    两者取较大的那个，宁可多等也别再撞一次。
    """
    candidates: list[float] = []

    raw = exc.headers.get("Retry-After")
    if raw:
        try:
            candidates.append(float(raw))
        except ValueError:
            pass

    try:
        body = exc.read().decode("utf-8", "ignore")
    except Exception:  # noqa: BLE001 - 读不到正文就用头信息
        body = ""
    if body:
        match = re.search(r"wait\s+(\d+(?:\.\d+)?)\s*seconds", body, re.IGNORECASE)
        if match:
            candidates.append(float(match.group(1)))

    if candidates:
        return max(candidates)
    return 30.0  # 兜底：没有明确提示时保守等半分钟


# --------------------------------------------------------- 真实挂单（买卖价差）

def exchange_offers(league: str, want: str, have: str) -> list[dict]:
    """查询「用手里的 have 换想要的 want」时，市场上有哪些挂单。

    返回的每条挂单：
        rate  = 每付出 1 单位 have 能收到多少 want
        price = 反向换算，每收到 1 单位 want 需要付出多少 have
    """
    payload = {"exchange": {"want": [want], "have": [have], "status": {"option": "online"}}}
    data = trade_post(f"/exchange/poe2/{urllib.parse.quote(league)}", payload)
    offers: list[dict] = []
    for entry in (data.get("result") or {}).values():
        listing = entry.get("listing") or {}
        for offer in listing.get("offers") or []:
            item = offer.get("item") or {}
            pay = offer.get("exchange") or {}
            give_amount = float(item.get("amount") or 0)   # 对方提供（你收到）的数量
            pay_amount = float(pay.get("amount") or 0)     # 对方收取（你付出）的数量
            if item.get("currency") != want or pay.get("currency") != have:
                continue
            if give_amount <= 0 or pay_amount <= 0:
                continue
            offers.append(
                {
                    "rate": give_amount / pay_amount,      # 1 have -> ? want
                    "price": pay_amount / give_amount,     # 1 want -> ? have
                    "stock": int(item.get("stock") or 0),
                }
            )
    return offers


def best_quotes(league: str, currency_id: str, ref: str, fair_price: float | None) -> dict:
    """算出某个通货相对基准货币的买入价 / 卖出价。

    - 买入价 ask：想买到该通货，最便宜要付多少 ref  →  取 want=通货 / have=ref 的最低报价
    - 卖出价 bid：想卖掉该通货，最高能收多少 ref    →  取 want=ref / have=通货 的最高报价
    """
    buy_side = [o["price"] for o in exchange_offers(league, currency_id, ref)]
    sell_side = [o["rate"] for o in exchange_offers(league, ref, currency_id)]

    ask, ask_n = _best_price(buy_side, "ask", fair_price)
    bid, bid_n = _best_price(sell_side, "bid", fair_price)

    if ask and bid:
        spread = (bid - ask) / ask * 100.0
        mid = (ask + bid) / 2.0
    else:
        spread, mid = None, ask or bid
    return {
        "ask": ask,
        "bid": bid,
        "spread": spread,
        "mid": mid,
        "ask_offers": ask_n,
        "bid_offers": bid_n,
    }


def _best_price(values: list[float], side: str, fair_price: float | None) -> tuple[float | None, int]:
    """从一堆挂单里挑出代表价。

    极值会被「5 混沌卖 1 神圣」这种挂错的垃圾单带偏，所以先按聚合价粗筛，
    再按挂单自身的中位数收一圈，最后才取极值。
    筛完若为空说明这一侧确实没人挂单（稀有物品很常见），如实返回 None。
    """
    pool = [v for v in values if v > 0]
    if not pool:
        return None, 0

    if fair_price and fair_price > 0:
        near = [v for v in pool if fair_price * 0.5 <= v <= fair_price * 2.0]
        if near:
            pool = near

    if len(pool) >= 3:
        ordered = sorted(pool)
        median = ordered[len(ordered) // 2]
        tighter = [v for v in pool if median * 0.6 <= v <= median * 1.6]
        if tighter:
            pool = tighter

    return (min(pool) if side == "ask" else max(pool)), len(pool)


_EXCLUDED_CACHE: set[str] | None = None


def excluded_currency_ids() -> set[str]:
    """把 config 里写的排除项（id / 英文名 / 中文名）解析成通货 id 集合。

    完全匹配优先，匹配不到再退化成包含匹配，方便直接写中文名。
    结果缓存起来，物品库刷新后调用 invalidate_exclusions() 重新解析。
    """
    global _EXCLUDED_CACHE
    if _EXCLUDED_CACHE is not None:
        return _EXCLUDED_CACHE

    resolved: set[str] = set()
    unresolved: list[str] = []
    try:
        metas = db().execute("SELECT currency_id, name_en, name_zh FROM item_meta").fetchall()
    except Exception:  # noqa: BLE001 - 物品库还没建好时不该让扫描失败
        metas = []

    for entry in SPREAD_EXCLUDE:
        needle = entry.lower()
        hit = None
        for row in metas:
            if (row["currency_id"] or "").lower() == needle:
                hit = row["currency_id"]
                break
        if hit is None:
            for row in metas:
                if (row["name_en"] or "").strip().lower() == needle:
                    hit = row["currency_id"]
                    break
        if hit is None:
            for row in metas:
                if (row["name_zh"] or "").strip() == entry:
                    hit = row["currency_id"]
                    break
        if hit is None:
            # 兜底做包含匹配，中文名少一个字也能对上
            for row in metas:
                name_en = (row["name_en"] or "").lower()
                name_zh = row["name_zh"] or ""
                if needle in name_en or (entry and entry in name_zh):
                    hit = row["currency_id"]
                    break
        if hit:
            resolved.add(hit)
        else:
            unresolved.append(entry)

    if not metas:
        # 物品库还没同步好，配置里写的 id 本身就是可直接使用的 id
        resolved = {x for x in SPREAD_EXCLUDE if x.isascii() and " " not in x}
        unresolved = []

    _EXCLUDED_CACHE = resolved
    if unresolved:
        log(f"  · 排除名单里这些没对上，已跳过：{unresolved}")
    log(f"  · 差价榜排除 {len(resolved)} 种基础通货")
    return resolved


def invalidate_exclusions() -> None:
    global _EXCLUDED_CACHE
    _EXCLUDED_CACHE = None


def tracked_item_count(league: str) -> int:
    """当前联盟在追踪的通货数量。

    ★ 按通货数（各自最近一行）而不是「最新一轮的行数」——开了
    skip_unchanged 后最新一轮是稀疏的，按轮数会只剩二三十（v1.27.8 修复）。
    """
    row = db().execute(
        "SELECT COUNT(DISTINCT currency_id) AS c FROM snapshot"
        " WHERE league = ? AND (source IS NULL OR source != 'synthetic')",
        (league,),
    ).fetchone()
    return int(row["c"]) if row else 0


def build_spread_snapshot(
    league: str, top_n: int = SPREAD_TOP_N, force: bool = False
) -> int:
    """扫描若干通货的真实买卖挂单，写入 spread 表。

    官方交易接口限流很严，一轮扫不完所有通货，所以按「最久没扫过」优先排序，
    多轮之后榜单会自然铺满；榜单读取时也取每个通货最近一次的结果。
    """
    latest_ts_row = db().execute(
        "SELECT MAX(ts) AS ts FROM snapshot WHERE league = ?", (league,)
    ).fetchone()
    if not latest_ts_row or latest_ts_row["ts"] is None:
        return 0
    latest_ts = int(latest_ts_row["ts"])

    excluded = excluded_currency_ids()
    ts = int(time.time())
    # 配额极其有限：刚扫过的通货隔很久才重扫，把请求留给还没扫过的
    fresh_cutoff = ts - SPREAD_RESCAN_SECONDS

    # 自动扫描也让界面能看到进度
    with SPREAD_JOB.lock:
        SPREAD_JOB.state.update(
            {"running": True, "started_at": ts, "error": "", "written": 0}
        )

    written = 0
    rate_limited = False
    for ref in SPREAD_REFS:
        column = BASE_COLUMNS[ref]
        # last_scan 必须按基准分开算。之前不分 ref，一个通货只要被扫过神圣石
        # 就被当成「扫过了」，结果页面默认看的混沌石几乎扫不到数据。
        # 候选 = 每个通货各自最近一行（不能按最新一轮整轮取：那是稀疏的）。
        # last_scan 单独查再拼回去——嵌在一条 SQL 里会让 ? 参数的绑定顺序
        # 变得难以推断，两步走虽然多一次查询，但正确性一目了然。
        candidates = latest_snapshot_rows(
            league,
            ["s.currency_id", "s.value_exalted", "s.value_chaos",
             "s.value_divine", "s.volume"],
        )
        last_scan = {
            r["currency_id"]: int(r["ts"])
            for r in db().execute(
                "SELECT currency_id, MAX(ts) AS ts FROM spread"
                " WHERE league = ? AND ref = ? GROUP BY currency_id",
                (league, ref),
            ).fetchall()
        }
        # last_scan 必须按基准分开算。之前不分 ref，一个通货只要被扫过神圣石
        # 就被当成「扫过了」，结果页面默认看的混沌石几乎扫不到数据。
        targets = [
            {**dict(row), "last_scan": last_scan.get(row["currency_id"])}
            for row in candidates
            if row["currency_id"] != ref and row["currency_id"] not in excluded
        ]
        targets.sort(key=lambda r: (
            0 if r["last_scan"] is None else 1,
            r["last_scan"] or 0,
            -(float(r["volume"] or 0.0)),
        ))
        targets = [
            r for r in targets
            if force  # 手动「立即扫描」无视重扫间隔
            or r["last_scan"] is None
            or r["last_scan"] < fresh_cutoff
        ][:top_n]

        for index, row in enumerate(targets, start=1):
            cid = row["currency_id"]
            fair = row[column]
            try:
                quote = best_quotes(league, cid, ref, fair if fair == fair else None)
            except TradeRateLimited as exc:
                # 已经吃到官方罚等了，再打下去罚时只会越滚越长，直接收工等下一轮
                log(f"  · {exc}，本轮提前结束（已写入 {written} 组）")
                rate_limited = True
                break
            except Exception as exc:  # noqa: BLE001 - 单个通货失败不影响整体
                log(f"  · {cid}（{ref}）挂单读取失败：{exc}")
                continue

            with db() as connection:
                connection.execute(
                    "INSERT OR REPLACE INTO spread"
                    " (ts, league, currency_id, ref, ask, bid, spread,"
                    "  ask_offers, bid_offers, mid) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (
                        ts, league, cid, ref,
                        quote["ask"], quote["bid"], quote["spread"],
                        quote["ask_offers"], quote["bid_offers"], quote["mid"],
                    ),
                )
            # 两侧都没人挂单也照样记一笔：这样它这轮就算「扫过了」，
            # 不会每轮都来白撞一次，把本就不多的配额挤掉
            written += 1
            if index % 5 == 0:
                log(f"    挂单扫描进度 {ref} {index}/{len(targets)}（已写入 {written}）")
        if rate_limited:
            break

    with db() as connection:
        cutoff = int(time.time()) - RETENTION_DAYS * 86400
        connection.execute("DELETE FROM spread WHERE ts < ?", (cutoff,))
        # 清理脏数据：自己换自己，以及买入价低于卖出价的交叉单（多为挂错的异常报价）
        connection.execute("DELETE FROM spread WHERE currency_id = ref")
        connection.execute("DELETE FROM spread WHERE spread > 20.0")

    with SPREAD_JOB.lock:
        SPREAD_JOB.state.update(
            {
                "running": False,
                "finished_at": int(time.time()),
                "written": written,
                "rate_limited": rate_limited,
            }
        )
    return written


def spread_extra_meta(ref: str, covered: int, shown: int) -> dict:
    """差价榜的状态说明：扫过多少、还剩多少、现在是不是被官方限流卡着。"""
    snap = db().execute(
        "SELECT MAX(ts) AS ts FROM snapshot WHERE league = ?", (STATE["league"],)
    ).fetchone()
    tracked = 0
    if snap and snap["ts"] is not None:
        # 同 tracked_item_count：按通货数取，不能按最新一轮的行数（那是稀疏的）
        tracked = tracked_item_count(STATE["league"])
    excluded = excluded_currency_ids()
    return {
        "window_hours": SPREAD_WINDOW_HOURS,
        "covered": covered,                # 窗口内扫到过挂单的通货数
        "shown": shown,                    # 实际展示条数
        "limit": SPREAD_DISPLAY_LIMIT,     # 点差前 N
        "excluded": len(excluded),         # 被排除的基础通货数
        "tracked": max(tracked - len(excluded), 0),  # 等待扫描的通货总数
        "refs": list(SPREAD_REFS),         # 后台正在扫哪些基准
        "scanning_ref": ref in SPREAD_REFS,
        "limiter": TRADE_LIMITER.snapshot(),
    }


def spread_rows(ref: str, hours: int, query: str) -> dict:
    """买卖差价榜。

    官方接口配额很紧，一轮扫不完所有通货，所以展示窗口放宽到 24 小时：
    多轮扫描的结果一直累积，不会因为「一小时没扫到」就整榜清空。
    基础通货（config.spread_exclude）不参与，最后按点差取前 N 个。
    """
    latest_ts_row = db().execute(
        "SELECT MAX(ts) AS ts FROM spread WHERE league = ? AND ref = ?",
        (STATE["league"], ref),
    ).fetchone()
    if not latest_ts_row or latest_ts_row["ts"] is None:
        meta = build_meta(ref, 0, 0)
        meta["no_zh"] = 0
        meta["spread"] = spread_extra_meta(ref, 0, 0)
        return {"meta": meta, "items": []}

    latest_ts = int(latest_ts_row["ts"])
    # 展示窗口：默认 24 小时，前端选更长就按前端的来
    window_hours = _clamp_int(hours, SPREAD_WINDOW_HOURS, 1, 24 * RETENTION_DAYS)
    cutoff = int(time.time()) - window_hours * 3600

    excluded = excluded_currency_ids()

    # 一轮扫描只能覆盖部分通货，所以取每个通货在窗口内最近一次的结果，让榜单逐步累积
    rows = [
        row
        for row in db().execute(
            "SELECT s.currency_id, s.ask, s.bid, s.spread, s.ask_offers, s.bid_offers,"
            "       s.mid, s.ts"
            " FROM spread s"
            " JOIN (SELECT currency_id, MAX(ts) AS mts FROM spread"
            "       WHERE league = ? AND ref = ? AND ts >= ?"
            "       GROUP BY currency_id) m"
            "   ON m.currency_id = s.currency_id AND m.mts = s.ts"
            " WHERE s.league = ? AND s.ref = ?"
            "   AND (s.ask IS NOT NULL OR s.bid IS NOT NULL)",
            (STATE["league"], ref, cutoff, STATE["league"], ref),
        ).fetchall()
        if row["currency_id"] not in excluded
    ]
    total_in_window = len(rows)
    # 「点差前 N」：按点差绝对值从大到小（空间越大越值得做）
    if SPREAD_DISPLAY_LIMIT and len(rows) > SPREAD_DISPLAY_LIMIT:
        rows.sort(key=lambda r: abs(r["spread"] or 0.0), reverse=True)
        rows = rows[:SPREAD_DISPLAY_LIMIT]

    history: dict[str, list[tuple[int, float]]] = {}
    for row in db().execute(
        "SELECT currency_id, ts, spread FROM spread"
        " WHERE league = ? AND ref = ? AND ts >= ? AND spread IS NOT NULL"
        " ORDER BY ts ASC",
        (STATE["league"], ref, cutoff),
    ).fetchall():
        history.setdefault(row["currency_id"], []).append((row["ts"], row["spread"]))

    meta_rows = {
        row["currency_id"]: row
        for row in db().execute("SELECT * FROM item_meta").fetchall()
    }

    items: list[dict] = []
    no_zh = 0
    for row in rows:
        cid = row["currency_id"]
        meta = meta_rows.get(cid)
        name_en = meta["name_en"] if meta and meta["name_en"] else cid
        name_zh = zh_display_name(cid, meta)
        if not matches_query(query, cid, name_en, name_zh):
            continue
        if not name_zh:
            no_zh += 1
            continue

        points = history.get(cid) or []
        change = None
        if len(points) >= 2 and points[0][1] is not None:
            first = points[0][1]
            change = points[-1][1] - first

        items.append(
            {
                "id": cid,
                "name": name_en,
                "name_zh": name_zh,
                "icon": f"/icon?id={urllib.parse.quote(cid)}" if meta and meta["icon"] else "",
                "ask": row["ask"],
                "bid": row["bid"],
                "spread": row["spread"],
                "mid": row["mid"],
                "ask_offers": row["ask_offers"] or 0,
                "bid_offers": row["bid_offers"] or 0,
                "change": change,
                "samples": len(points),
                "ts": row["ts"],
            }
        )
    meta = build_meta(ref, latest_ts, len(items))
    meta["no_zh"] = no_zh
    meta["spread"] = spread_extra_meta(ref, total_in_window, len(items))
    return {"meta": meta, "items": items}


def spread_history(currency_id: str, ref: str, hours: int) -> dict:
    cutoff = int(time.time()) - hours * 3600
    rows = db().execute(
        "SELECT ts, ask, bid, spread FROM spread"
        " WHERE league = ? AND currency_id = ? AND ref = ? AND ts >= ? ORDER BY ts ASC",
        (STATE["league"], currency_id, ref, cutoff),
    ).fetchall()
    meta = db().execute(
        "SELECT * FROM item_meta WHERE currency_id = ?", (currency_id,)
    ).fetchone()

    def series(column: str) -> list[list]:
        return [
            [row["ts"], row[column]]
            for row in rows
            if row[column] is not None
        ]

    return {
        "id": currency_id,
        "name": zh_display_name(currency_id, meta) or (
            meta["name_en"] if meta and meta["name_en"] else currency_id
        ),
        "name_zh": zh_display_name(currency_id, meta),
        "name_en": meta["name_en"] if meta and meta["name_en"] else currency_id,
        "ref": ref,
        "ask": series("ask"),
        "bid": series("bid"),
        "spread": series("spread"),
    }


# ------------------------------------------------------------- 通货元数据

def load_item_catalog() -> int:
    """拉取中英文名称与图标路径，写入 item_meta。返回条目数。"""
    catalog: dict[str, dict[str, str]] = {}

    def collect(url: str, lang_key: str) -> None:
        try:
            payload = http_json(url, retries=2)
        except Exception as exc:  # noqa: BLE001
            log(f"  · 静态数据不可用（{lang_key}）：{exc}")
            return
        try:
            for section in payload["result"]:
                for entry in section["entries"]:
                    record = catalog.setdefault(
                        entry["id"], {"name_en": "", "name_zh": "", "icon": ""}
                    )
                    record[lang_key] = entry.get("text", "")
                    if entry.get("image"):
                        record["icon"] = entry["image"]
        except Exception as exc:  # noqa: BLE001
            log(f"  · 静态数据解析失败（{lang_key}）：{exc}")

    collect(EN_STATIC, "name_en")
    collect(CN_STATIC, "name_zh")

    if not catalog:
        return 0

    # 官方静态数据没给中文的，用兜底表补上
    overrides = zh_overrides()
    for cid, record in catalog.items():
        if not has_cjk(record.get("name_zh", "")):
            record["name_zh"] = overrides.get(cid, "")

    # 已经抓进快照、但静态数据里根本没有的通货（赛季新物品）也要建一条记录，
    # 否则它们没有中文名只能显示 id。英文名先由 id 反推，中文名靠兜底表。
    seen = set(catalog)
    orphans = [
        row["currency_id"]
        for row in db().execute("SELECT DISTINCT currency_id FROM snapshot").fetchall()
    ]
    for cid in orphans:
        if cid in seen:
            continue
        catalog[cid] = {
            "name_en": cid.replace("-", " ").title(),
            "name_zh": overrides.get(cid, ""),
            "icon": "",
        }

    with db() as connection:
        connection.executemany(
            "INSERT OR REPLACE INTO item_meta (currency_id, name_en, name_zh, icon)"
            " VALUES (?, ?, ?, ?)",
            [
                (cid, rec["name_en"], rec["name_zh"], rec["icon"])
                for cid, rec in catalog.items()
            ],
        )
    return len(catalog)


# ----------------------------------------------------------------- 抓取汇率

def fetch_category(league: str, category: str) -> tuple[list[dict], float, float, float]:
    """抓取单个类别，返回 (行列表, 崇高系数, 混沌系数, 神圣系数)。"""
    url = (
        f"{NINJA_API}/exchange/current/overview"
        f"?league={urllib.parse.quote(league)}&type={urllib.parse.quote(category)}"
    )
    # ★ poe.ninja 是小时级刷新，按 1 小时缓存一次（见 NINJA_TTL_SECONDS 注释）
    payload = ninja_cached(url)

    core = payload.get("core") or {}
    rates: dict[str, float] = core.get("rates") or {}
    primary: str = core.get("primary", "divine")

    # rates[X] 表示「1 单位基准货币 = X 的多少单位」，
    # 所以 primary_value(以基准货币计价) * rates[X] = 以 X 计价的价值。
    def factor(name: str) -> float:
        if name in rates:
            return float(rates[name])
        return 1.0 if primary == name else float("nan")

    f_exalted, f_chaos, f_divine = factor("exalted"), factor("chaos"), factor("divine")
    # 记一笔 ninja 汇率指纹：它响应里没有时间戳，判断它活不活跃只能看数据变没变
    ninja_note_rates(league, rates)
    # ⚠️ ninja 没给汇率时 factor() 会返回 nan（自检里用假联盟就复现了），
    #    而 nan 会顺着「价格 × 系数」把每一行全污染成 nan。
    #    所以先判一次：系数不可用 == 这份 ninja 数据不可用，不许拿它当回退目标。
    ninja_rates_ok = all(
        math.isfinite(x) and x > 0 for x in (f_exalted, f_chaos, f_divine)
    )

    # poe2scout 覆盖时用它的汇率换算。它以「崇高石 = 1」计价，
    # 折算成 divine 基准的系数：1 divine = (divine价/exalted价) 崇高 = (divine价/chaos价) 混沌。
    # 拿不到 scout 就原样用 ninja 的系数，不影响主流程。
    scout: dict[str, float] = {}
    doe: dict[str, dict] = {}
    if PRICE_SOURCE == "scout":
        try:
            scout = scout_currency_prices(league)
            scout_probe_interval(league)
            scout = scout_refresh_bases(league, scout)
        except Exception:  # noqa: BLE001 - scout 挂了就退回 ninja
            scout = {}
        try:
            doe = doe_prices(league)
        except Exception:  # noqa: BLE001 - doe 是个人小站，挂了不影响主流程
            doe = {}

    # ★ 用户选定的主源：把不该用的那份清掉即可——下面取价时取不到就会自然
    #   落到 ninja 自带的系数上（见 rows 循环里的 price=None 分支）。
    #   两份数据仍然照抓（都有缓存，不额外发请求），这样切回 auto 时立刻可用。
    _primary = primary_source()
    if _primary == "scout":
        doe = {}
    elif _primary == "ninja":
        doe = {}
        scout = {}

    s_ex = scout.get("exalted") or 1.0
    s_ch = scout.get("chaos") or 0.0
    s_div = scout.get("divine") or 0.0

    # dadsofexile 更贴游戏内成交价（实测平均偏差 10.5% vs scout 48.2%），
    # 能用就用它的基准货币当尺子；拿不到再退回 scout，最后才是 ninja 自带系数。
    d_div = float((doe.get("divine") or {}).get("price") or 0.0)
    d_ch = float((doe.get("chaos") or {}).get("price") or 0.0)
    d_ex = float((doe.get("exalted") or {}).get("price") or 1.0)
    use_doe_base = d_div > 0 and d_ch > 0 and d_ex > 0
    # 僵住检测：它是个人小站，抽风时会一直返回同一份不动的数据。
    # 数据本身看着正常，只有 fetched_at 会暴露问题——一旦超时就整体回退 scout。
    stale, stale_age = doe_stale(league) if doe else (True, 0)
    # ⚠️ 两道判据都要查，缺一不可：
    #   stale  = 时间戳太旧（小站干脆不刷新了）
    #   frozen = 时间戳还在动、但价格长时间一个不变（2026-09-28 实测踩到的那种）
    frozen, frozen_age = doe_frozen(league) if doe else (False, 0)

    # ★★ 回退前必须先确认「回退目标确实比现在这个新」——不然那不叫回退，叫倒退。
    #
    # 2026-09-29 事故：poe2scout 整条链路停更（ExchangeSnapshot.Epoch 卡在 18 小时前，
    # 四个端点时间戳完全一致），而 dadsofexile 是实时的（fetched_at 落后 0 分钟）。
    # 偏偏 doe 用 1287 万成交量算出来的加权价**天然就很稳定**，几小时不动是正常现象，
    # 于是被「价格指纹僵住」判成僵数据，一纸判决就整体切到停更 18 小时的 scout——
    # 神圣石汇率当场从 557 掉到 538，然后彻底不动。检测生效了，结果反而更糟。
    #
    # 所以：只有 scout 那份数据确实不比 doe 旧，才允许回退。
    # 拿不到任一方时间戳时不拦（保持老行为），免得缺字段反而把 doe 锁死。
    #
    # ★★ 但这条守卫有个前提：**回退目标是 scout**。
    #    2026-09-29 实测踩到的死结：scout 全站停更 24.7 小时后被上面摘掉，
    #    回退目标实际变成了 poe.ninja，守卫却还在拿 scout 的时间戳跟 doe 比——
    #    scout 停更 24h，比谁都旧，于是永远判「备用源更旧」→ 永不回退。
    #    后果是 doe 的 divine 汇率卡在 482.92 整整 23 小时（比真值 535 低 9.7%），
    #    程序却一直把它当实时价用。
    #    → scout 已被摘掉时，该问的是「ninja 活着吗」，不是「scout 新不新」。
    doe_ts = int(doe_source_info(league).get("updated_at") or 0)
    scout_ts = int(scout_source_info(league).get("updated_at") or 0)

    # ★★ scout 太旧就整份停用（见 SCOUT_MAX_AGE_SECONDS 注释）。
    # 这是「回退守卫」之外的第二道闸：守卫只管 doe→scout 的整体回退，
    # 而逐条取价那一步是写死的 doe > scout > ninja —— 不在这里把 scout 摘掉，
    # 停更 22 小时的 scout 照样会把新鲜的 ninja 挡在门外。
    scout_fresh = True
    if scout_ts:
        scout_age = int(time.time()) - scout_ts
        if scout_age > SCOUT_MAX_AGE_SECONDS:
            scout_fresh = False
            log_once(
                f"scout-stale:{league}",
                f"  · poe2scout 的数据已 {scout_age // 3600} 小时没更新"
                f"（超过 {SCOUT_MAX_AGE_SECONDS // 3600} 小时门槛），"
                f"本轮不用它取价和挂出量，改用更新鲜的源",
            )
    if not scout_fresh:
        scout = {}
        s_div = s_ch = s_ex = 0.0

    fallback_ok = True
    if _primary == "doe":
        # 用户明确指定只用 dadsofexile：即便被判僵也不换源——
        # 换源是他自己能在界面上做的决定，程序不该替他改。
        fallback_ok = False
    elif not scout_fresh:
        # ★ scout 已停更被摘掉 → 回退目标是 poe.ninja，守卫必须换成问 ninja。
        #   这里**不会**来回横跳：判据是「doe 的价格指纹多久没变」，
        #   只要 doe 一直不动就一直回退，只有 doe 真的刷新了才会切回去。
        n_frozen, n_held = ninja_frozen(league)
        if n_frozen:
            fallback_ok = False
            log_once(
                f"ninja-frozen:{league}",
                f"  · poe.ninja 的汇率也已 {n_held // 60} 分钟没变过，"
                f"回退也拿不到更新的数据，本轮仍沿用 dadsofexile",
            )
        elif not ninja_rates_ok:
            fallback_ok = False
            log_once(
                f"ninja-norates:{league}",
                "  · poe.ninja 本轮没给汇率，无法用它的价，本轮仍沿用 dadsofexile",
            )
    elif doe_ts and scout_ts and scout_ts < doe_ts:
        fallback_ok = False
        older = (doe_ts - scout_ts) // 60
        log_once(
            f"scout-older:{league}",
            f"  · poe2scout 那份比 dadsofexile 旧 {older} 分钟，"
            f"本轮不回退（回退只会拿到更老的数据）",
        )

    if frozen and use_doe_base:
        if fallback_ok:
            _to = "poe.ninja（poe2scout 已停更，本轮不可用）" if not scout_fresh else "poe2scout"
            log_once(
                f"doe-frozen:{league}",
                f"  · dadsofexile 价格已 {frozen_age // 60} 分钟没变过"
                f"（时间戳还在动，判定为僵数据），本轮改用 {_to}",
            )
            use_doe_base = False
        else:
            log_once(
                f"doe-frozen-keep:{league}",
                f"  · dadsofexile 价格已 {frozen_age // 60} 分钟没变过，"
                f"但备用源也不动，仍沿用 dadsofexile",
            )
    if stale and use_doe_base:
        if fallback_ok:
            # ⚠️ scout 停更被摘掉后，这里实际落到的是 poe.ninja 的系数 ——
            #    日志必须说 ninja，不然排查时会被"改用 poe2scout"骗去查一个
            #    本轮根本没参与的数据源（2026-09-29 踩到）。
            _to = "poe.ninja（poe2scout 已停更，本轮不可用）" if not scout_fresh else "poe2scout"
            log_once(
                f"doe-stale:{league}",
                f"  · dadsofexile 数据已 {stale_age // 60} 分钟没刷新，本轮改用 {_to}",
            )
            use_doe_base = False
        else:
            log_once(
                f"doe-stale-keep:{league}",
                f"  · dadsofexile 数据已 {stale_age // 60} 分钟没刷新，"
                f"但备用源更旧，仍沿用 dadsofexile",
            )
    stale = stale or frozen

    # 记下本轮真正用上的基准源，供 /api/meta 告诉界面「现在这个价是谁给的」
    _LAST_BASIS[league] = "doe" if use_doe_base else ("scout" if scout else "ninja")

    # 挂出量 / 求购量改用 poe2scout 的「全交易所交易对快照」，doe 只做兜底。
    # ⚠️ 旧口径只查「对崇高石」那一个交易对、且只取自己那一侧，
    #    贵重物品恒为 0、崇高石自己也恒为 0（详见 scout_pair_stocks）。
    #    SnapshotPairs 一个请求就覆盖全部 1648 个交易对，顺带把逐条补查那套废掉了。
    pair_ask: dict[str, float] = {}
    pair_bid: dict[str, float] = {}
    pair_count: dict[str, int] = {}
    if PRICE_SOURCE == "scout" and scout_fresh:
        pair_ask, pair_bid, pair_count = scout_pair_stocks(league)
        # 「立刻填补」：doe 空着或僵住时，缓存里没有就当场强刷一次，
        # 否则这一轮的挂出量会整片落空。
        if not pair_ask and (stale or not doe):
            pair_ask, pair_bid, pair_count = scout_pair_stocks(league, force=True)
            if pair_ask:
                log(f"  · scout 交易对快照已当场强刷（{len(pair_ask)} 个通货）")
    # ByCategory 的 CurrentQuantity 语义不明（与全交易对合计对不上），
    # 只在上面全盘拿不到时当最后的兜底用。
    # ⚠️ 同样受 scout_fresh 约束：停更 22 小时的 scout 库存不是「本轮的兜底」，
    #    是旧数据冒充实时量（用户截图里那个一动不动的挂出量就是这么来的）。
    scout_qty = scout_quantities(league) if (not pair_ask and scout_fresh) else {}
    if use_doe_base:
        f_divine = 1.0
        f_exalted = d_div / d_ex
        f_chaos = d_div / d_ch
    elif scout and s_div > 0 and s_ch > 0 and s_ex > 0:
        f_divine = 1.0
        f_exalted = s_div / s_ex
        f_chaos = s_div / s_ch

    rows: list[dict] = []
    for line in payload.get("lines") or []:
        primary_value = float(line.get("primaryValue") or 0.0)
        if primary_value <= 0:
            continue
        cid = line.get("id", "")
        sparkline = line.get("sparkline") or {}

        # 取价优先级：dadsofexile（挂单够 / 桥接价，且非假价）> poe2scout（新鲜时）> poe.ninja。
        # 用谁的价就必须用谁的基准货币，混着算会自相矛盾（这是踩过的坑）。
        # ⚠️ scout 那一档必须带 scout_fresh：它停更时还留着数据，
        #    不摘掉就永远挡住更新的 ninja（2026-09-29 事故）。
        doe_row = doe.get(cid)
        price = None
        base_div = base_ch = base_ex = 0.0
        if doe_row and doe_row.get("ok") and use_doe_base:
            price = doe_row["price"]
            base_div, base_ch, base_ex = d_div, d_ch, d_ex
        elif scout_fresh and scout.get(cid) and s_div > 0 and s_ch > 0 and s_ex > 0:
            price = scout[cid]
            base_div, base_ch, base_ex = s_div, s_ch, s_ex

        if price and base_div > 0 and base_ch > 0 and base_ex > 0:
            divine = price / base_div
            exalted = price / base_ex
            chaos = price / base_ch
        else:
            divine = primary_value * f_divine
            exalted = primary_value * f_exalted
            chaos = primary_value * f_chaos

        ask_v, bid_v = pick_ask_bid(
            cid, pair_ask, pair_bid, scout_qty, doe_row, doe_is_stale=stale
        )

        rows.append(
            {
                "id": cid,
                "divine": divine,
                "exalted": exalted,
                "chaos": chaos,
                "volume": float(line.get("volumePrimaryValue") or 0.0),
                "trend": float(sparkline.get("totalChange") or 0.0),
                # ninja 自带的近 7 天趋势（百分比序列），本地历史不足时用作兜底
                "spark": sparkline.get("data") or [],
                # 买卖两侧的挂出量（都是「数量」，不是挂单笔数）：
                #   orders = 挂出量：该通货自己挂出去多少 → 你想买能买到多少
                #   stock  = 求购量：对手挂了多少通货在收它 → 你想卖有多少人接
                # 两者完全独立，实测 Omen of the Hunt 挂出 0 / 求购 27,397。
                # 取自 scout 的 SnapshotPairs，拿不到才逐级退回 ByCategory / doe。
                # 都没有就存 0，前端显示「—」，绝不拿 0 冒充「真的没有」。
                "stock": bid_v,
                "orders": ask_v,
            }
        )
    return rows, f_exalted, f_chaos, f_divine


# ----------------------------------------------------------------- 抓取暗金

# 词缀文本里有两种标记：[显示文本|术语] 和 [术语]，展示时都还原成纯文本
_MOD_LINK = re.compile(r"\[([^\]\|]*)(?:\|([^\]]*))?\]")
_MOD_RANGE = re.compile(r"(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)")
_MOD_SINGLE = re.compile(r"(\d+(?:\.\d+)?)")


def clean_ninja_text(text: str) -> str:
    """poe.ninja 的词缀文本带 [显示文本|术语] 链接标记，展示时只取前半段。"""
    if not text:
        return ""
    def pick(match: re.Match) -> str:
        # 有 | 时后半段是给人看的完整写法（"Energy Shield"），否则就用方括号里的内容
        return (match.group(2) or match.group(1) or "").strip()
    return _MOD_LINK.sub(pick, text).strip()


def parse_mod_range(text: str) -> tuple[float | None, float | None]:
    """从词缀文本里抠出数值区间，用来比较同一件暗金不同档位的差别。

        "+(100-137) to maximum Energy Shield" -> (100.0, 137.0)
        "Energy Shield: (164-298)"            -> (164.0, 298.0)
        "Gain 3 Life per enemy killed"        -> (3.0, 3.0)

    抠不出来就返回 (None, None)。
    """
    if not text:
        return None, None
    match = _MOD_RANGE.search(text)
    if match:
        lo, hi = float(match.group(1)), float(match.group(2))
        return (min(lo, hi), max(lo, hi))
    single = _MOD_SINGLE.search(text)
    if single:
        value = float(single.group(1))
        return value, value
    return None, None


def unique_rate_factors(league: str) -> dict[str, float] | None:
    """用 dadsofexile 的交易所实时价推算「1 神圣石 = ? 崇高石 / ? 混沌石」。

    poe.ninja 自带的汇率明显偏离线内交易所（实测它给 1 divine = 490.3 exalted /
    7.76 chaos，而交易所订单簿实际是 512.48 exalted / 6.905 chaos，
    exalted 低了 4.3%、chaos 高了 12.4%）。暗金是按神圣石报价的，
    汇率一错，exalted / chaos 两列会整体偏掉，所以用交易所实时汇率覆盖。
    """
    flat = doe_prices(league)
    divine = flat.get("divine")
    chaos = flat.get("chaos")
    if not divine or not chaos:
        return None
    d_price = float(divine.get("price") or 0.0)   # doe 一切以「崇高石 = 1」计价
    c_price = float(chaos.get("price") or 0.0)
    if d_price <= 0 or c_price <= 0:
        return None
    return {"divine": 1.0, "exalted": d_price, "chaos": d_price / c_price}


def fetch_unique_category(league: str, category: str) -> list[dict]:
    """抓取一个暗金分类，返回档位行。

    poe.ninja 会把同一件暗金按 roll 档位拆成多行（护甲 127-164 一档、164-298 一档），
    每行只给数值区间不给实际值，所以档位只能粗粒度比较，精确归因要靠官方挂单接口。
    """
    url = (
        f"{NINJA_API}/stash/current/item/overview"
        f"?league={urllib.parse.quote(league)}&type={urllib.parse.quote(category)}"
    )
    # 同上：poe.ninja 按 1 小时缓存一次，别每轮每个分类都去打它
    payload = ninja_cached(url)

    core = payload.get("core") or {}
    rates: dict[str, float] = core.get("rates") or {}
    primary: str = core.get("primary", "divine")

    def factor(name: str) -> float:
        if name in rates:
            return float(rates[name])
        return 1.0 if primary == name else float("nan")

    f_exalted, f_chaos, f_divine = factor("exalted"), factor("chaos"), factor("divine")

    # 汇率换成交易所实时值（理由见 unique_rate_factors 的注释）。
    # 只在 ninja 本来就用神圣石计价时覆盖，免得把别的 primary 币种算歪。
    doe_factors = unique_rate_factors(league)
    if doe_factors and primary == "divine":
        f_exalted = doe_factors["exalted"]
        f_chaos = doe_factors["chaos"]
        f_divine = 1.0

    rows: list[dict] = []
    for line in payload.get("lines") or []:
        primary_value = float(line.get("primaryValue") or 0.0)
        if primary_value <= 0:
            continue
        name: str = (line.get("name") or "").strip()
        base_type: str = (line.get("baseType") or "").strip()
        if not name:
            continue
        sparkline: dict = line.get("sparkLine") or {}
        rows.append(
            {
                # detailsId 在同一件物品的多档位下可能重复，补上行 id 保证唯一
                "key": f"{line.get('detailsId') or name}#{line.get('id')}",
                "name": name,
                "base_type": base_type,
                "category": (line.get("category") or "").strip(),
                "icon": line.get("icon") or "",
                "level_req": int(line.get("levelRequired") or 0),
                "corrupted": 1 if line.get("corrupted") else 0,
                "divine": primary_value * f_divine,
                "exalted": primary_value * f_exalted,
                "chaos": primary_value * f_chaos,
                "listing_count": int(line.get("listingCount") or 0),
                "trend": float(sparkline.get("totalChange") or 0.0),
                "spark": sparkline.get("data") or [],
                "mods": {
                    "p": [clean_ninja_text(m.get("text"))
                          for m in (line.get("propertyModifiers") or [])],
                    "e": [clean_ninja_text(m.get("text"))
                          for m in (line.get("explicitModifiers") or [])],
                },
            }
        )
    return rows


def take_unique_snapshot(league: str, ts: int | None = None) -> int:
    """抓取全部暗金分类并写入一次快照，返回写入行数。"""
    ts = int(ts or time.time())
    records: list[tuple] = []
    for category, _label in UNIQUE_CATEGORIES:
        try:
            rows = fetch_unique_category(league, category)
        except Exception as exc:  # noqa: BLE001 - 单个分类失败不影响其他分类
            log(f"  × 暗金 {category} 抓取失败：{exc}")
            continue
        for row in rows:
            records.append(
                (
                    ts, league, category, row["key"], row["name"], row["base_type"],
                    row["icon"], row["level_req"], row["corrupted"], row["divine"],
                    row["exalted"], row["chaos"], row["listing_count"], row["trend"],
                    json.dumps(row["spark"]),
                    json.dumps(row["mods"], ensure_ascii=False),
                )
            )
        time.sleep(0.4)  # 对社区接口保持礼貌

    if not records:
        return 0

    with db() as connection:
        connection.executemany(
            "INSERT OR REPLACE INTO unique_snapshot (ts, league, category, item_key, name,"
            " base_type, icon, level_req, corrupted, value_divine, value_exalted,"
            " value_chaos, listing_count, trend, spark, mods)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            records,
        )
        cutoff = ts - UNIQUE_RETENTION_DAYS * 86400
        connection.execute("DELETE FROM unique_snapshot WHERE ts < ?", (cutoff,))
    log(f"  ✓ 已记录 {len(records)} 条暗金报价（{len(UNIQUE_CATEGORIES)} 个分类）")
    return len(records)


# 7 天趋势回填是**派生数据**：同一条 sparkline 反推出来的结果每轮都一样，
# 但原来每一轮抓取都重算一遍（先 DELETE 3887 行、再 INSERT 3887 行），
# 5 分钟一轮的话一小时白烧 12 次全量重写。它本来就是日线颗粒度，
# 隔几小时重算一次完全够用。
BACKFILL_MIN_SECONDS = 6 * 3600


def backfill_history(league: str, ts: int) -> int:
    """用 poe.ninja 自带的近 7 天累计涨跌幅，还原出历史价格填入数据库。

    poe.ninja 每小时才更新一次报价，光靠本机抓取要攒很久才有转折区间。
    它的 sparkline 给的是「相对窗口起点的累计涨跌幅 %」，
    因此 price(t) = 当前价 × (1 + p_t) / (1 + p_末)，可以反推出真实价格序列。
    这类数据标记 source='synthetic'，只用于画图与统计，不和实测数据混淆。
    """
    last = _meta_value("backfill_at")
    if last and int(time.time()) - last < BACKFILL_MIN_SECONDS:
        return 0

    rows = db().execute(
        "SELECT currency_id, category, spark, value_divine, value_exalted, value_chaos"
        " FROM snapshot WHERE league = ? AND ts = ?"
        " AND (source IS NULL OR source != 'synthetic')",
        (league, ts),
    ).fetchall()

    records: list[tuple] = []
    for row in rows:
        try:
            raw = json.loads(row["spark"] or "[]")
        except (TypeError, ValueError):
            continue
        pct: list[float] = []
        for sample in raw if isinstance(raw, list) else []:
            try:
                pct.append(float(sample))
            except (TypeError, ValueError):
                continue
        if len(pct) < 3:
            continue

        last_pct = pct[-1]
        denominator = 1.0 + last_pct / 100.0
        if abs(denominator) < 1e-6:
            continue

        values = {
            "divine": row["value_divine"],
            "exalted": row["value_exalted"],
            "chaos": row["value_chaos"],
        }
        if any(v is None or v != v for v in values.values()):
            continue

        for index, change in enumerate(pct[:-1]):  # 最后一点就是「现在」，跳过
            # 7 个采样点覆盖近 7 天，按每天一个点回推时间戳
            point_ts = ts - (len(pct) - 1 - index) * 86400
            factor = (1.0 + change / 100.0) / denominator
            records.append(
                (
                    point_ts,
                    league,
                    row["category"],
                    row["currency_id"],
                    values["divine"] * factor,
                    values["exalted"] * factor,
                    values["chaos"] * factor,
                    0.0,
                    change,
                    "[]",
                    "synthetic",
                )
            )

    if not records:
        return 0

    with db() as connection:
        # 回填数据是派生的，每次先清掉旧的再写入
        connection.execute(
            "DELETE FROM snapshot WHERE league = ? AND source = 'synthetic'", (league,)
        )
        connection.executemany(
            "INSERT OR REPLACE INTO snapshot"
            " (ts, league, category, currency_id, value_divine, value_exalted,"
            "  value_chaos, volume, trend, spark, source) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            records,
        )
        connection.execute(
            "INSERT OR REPLACE INTO app_meta (k, v) VALUES ('backfill_at', ?)",
            (str(int(time.time())),),
        )
    return len(records)


def prune_snapshot(league: str) -> None:
    """清理过期数据，并把历史快照里的 spark 字段清空。

    spark 是数据源给的 7 天趋势数组（每行几百字节），只有最新一次快照的界面展示会用到，
    历史行保留它会让数据库快速膨胀（半天一小就涨十几 MB）。
    """
    with db() as connection:
        cutoff = int(time.time()) - RETENTION_DAYS * 86400
        connection.execute("DELETE FROM snapshot WHERE ts < ?", (cutoff,))
        connection.execute(
            "UPDATE snapshot SET spark = NULL"
            " WHERE spark IS NOT NULL"
            "   AND ts < (SELECT MAX(ts) FROM snapshot WHERE league = ?)",
            (league,),
        )


# ------------------------------------------------------- 源站更新探测（ETag）
# 记录每个探针 URL 最近一次拿到的 ETag，作为"数据有没有变"的基线
_ETAG_CACHE: dict[str, str] = {}
_ETAG_LOCK = threading.Lock()


def _overview_url(league: str, category: str) -> str:
    return (
        f"{NINJA_API}/exchange/current/overview"
        f"?league={urllib.parse.quote(league)}&type={urllib.parse.quote(category)}"
    )


def probe_updates(league: str) -> tuple[bool, int]:
    """低成本地问一句「源站数据变了吗」。

    带 If-None-Match 发条件请求：没变就是 304、响应体为空，几乎不占配额；
    变了才是 200，这时才值得去拉全量。返回 (是否有更新, 状态码)。
    """
    changed = False
    last_code = 200
    for category in POLL_PROBE_TYPES:
        url = _overview_url(league, category)
        headers = {"User-Agent": USER_AGENT, "Accept": "*/*"}
        with _ETAG_LOCK:
            known = _ETAG_CACHE.get(url)
        if known:
            headers["If-None-Match"] = known
        try:
            request = urllib.request.Request(url, headers=headers)
            with _HTTP_OPENER.open(request, timeout=20) as response:
                last_code = response.status
                etag = response.headers.get("ETag") or ""
                response.read()  # 只是探测，内容不解析；真要抓会再请求一次
        except urllib.error.HTTPError as exc:
            last_code = exc.code
            etag = exc.headers.get("ETag") or ""
        except Exception:  # noqa: BLE001 - 探测失败按"没变"处理，下一轮再来
            return changed, -1
        if last_code == 429:
            return False, 429
        if last_code == 200 and etag:
            with _ETAG_LOCK:
                previous = _ETAG_CACHE.get(url)
                _ETAG_CACHE[url] = etag
            # 第一次取样只建立基线，不算"更新"
            if previous and etag != previous:
                changed = True
    return changed, last_code


class UpdateTracker:
    """记住源站多久更新一次，据此决定下一次探测该等多久。

    刚启动时不知道节奏，就从最短间隔开始、没变化就逐步放宽；
    摸清节奏后，在「预计要更新了」的窗口里压到最短间隔紧盯，其余时间放宽。
    """

    def __init__(self) -> None:
        self.last_change = 0          # 最近一次观测到更新的时刻
        self.samples: list[int] = []  # 观测到的更新间隔（秒）
        self.wait = POLL_MIN_SECONDS  # 当前探测间隔
        self.probes = 0
        self.changes = 0

    def note_change(self, now: int) -> None:
        if self.last_change:
            gap = now - self.last_change
            if 60 <= gap <= 12 * 3600:  # 只收合理样本，异常长/短的不采纳
                self.samples.append(gap)
                self.samples = self.samples[-20:]
        self.last_change = now
        self.changes += 1
        self.wait = POLL_MIN_SECONDS  # 刚变过，说明源站活跃，继续紧盯
        save_tracker_state(self)  # 学到的节奏存下来，重启后不用重新摸索

    def note_no_change(self) -> None:
        self.probes += 1
        if not self.samples:
            # 冷启动：逐步退避，别一上来就死等
            self.wait = min(POLL_MAX_SECONDS, max(POLL_MIN_SECONDS, int(self.wait * POLL_BACKOFF)))
        # 摸清节奏后，间隔由 predicted_wait 决定，这里不动

    def typical_interval(self) -> int | None:
        """源站典型更新间隔（中位数，秒）；样本不足返回 None。"""
        if len(self.samples) < 2:
            return None
        ordered = sorted(self.samples)
        return ordered[len(ordered) // 2]

    def next_wait(self, now: int) -> int:
        """下一次探测该等多久。"""
        typical = self.typical_interval()
        if not typical or not self.last_change:
            return int(self.wait)  # 还没摸清节奏，用退避出来的间隔
        elapsed = now - self.last_change
        remain = typical - elapsed
        if remain <= typical * 0.15:
            return POLL_MIN_SECONDS          # 已到/临近预期更新点，最短间隔紧盯
        # 离得还远，但也别一口气睡到头，留一半余量避免错过提前更新
        return max(POLL_MIN_SECONDS, min(int(remain * 0.5), POLL_MAX_SECONDS))

    def snapshot(self, now: int) -> dict:
        typical = self.typical_interval()
        return {
            "enabled": ADAPTIVE_POLL,
            "min_seconds": POLL_MIN_SECONDS,
            "max_seconds": POLL_MAX_SECONDS,
            "current_wait": int(self.next_wait(now)),
            "probe_types": list(POLL_PROBE_TYPES),
            "probes": self.probes,
            "changes": self.changes,
            "last_change": self.last_change,
            "typical_minutes": round(typical / 60, 1) if typical else None,
            "samples": len(self.samples),
            "fallback_minutes": POLL_FALLBACK_SECONDS // 60,
        }


TRACKER = UpdateTracker()


def load_tracker_state(tracker: UpdateTracker) -> None:
    """重启后把上次摸清的源站节奏读回来，别每次都从零学起。"""
    try:
        row = db().execute("SELECT v FROM app_meta WHERE k = 'poll_samples'").fetchone()
        if row and row["v"]:
            tracker.samples = [int(x) for x in str(row["v"]).split(",") if x.strip()][-20:]
        row = db().execute("SELECT v FROM app_meta WHERE k = 'poll_last_change'").fetchone()
        if row and row["v"]:
            tracker.last_change = int(row["v"])
    except Exception:  # noqa: BLE001 - 读不回来就当没学过，重新摸
        pass


def save_tracker_state(tracker: UpdateTracker) -> None:
    try:
        with db() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO app_meta (k, v) VALUES ('poll_samples', ?)",
                (",".join(str(x) for x in tracker.samples),),
            )
            connection.execute(
                "INSERT OR REPLACE INTO app_meta (k, v) VALUES ('poll_last_change', ?)",
                (str(tracker.last_change),),
            )
    except Exception:  # noqa: BLE001 - 存不下不影响主流程
        pass


# 并发抓类别的路数。14 个类别串行实测 46 秒，6 路并发 8.3 秒（快 5.5 倍），
# 这是「每次打开程序都要等半天」的主因。不再往上加的原因：源站是社区接口，
# 并发再高也没有明显收益，反而容易撞限流被惩罚；6 路是实测的甜点。
FETCH_WORKERS = _clamp_int(CONFIG.get("fetch_workers", 6), 6, 1, 12)


def warm_shared_sources(league: str) -> None:
    """并发抓类别之前，先把「跟类别无关」的那几份联盟级数据取一遍。

    为什么必须单独预热：fetch_category 每个类别都要 scout 汇率、doe 基准价、
    全交易对挂出量这些**全联盟**数据。它们本身有缓存，但并发一起跑时
    头几个线程会同时发现缓存是冷的，于是同一份数据被重复抓好几遍——
    这是实测里首轮比后续轮慢 18 秒的真正原因（不是带宽、不是数据量）。
    开局串行取一次把缓存喂热，后面 14 个类别就全是缓存命中。
    """
    if PRICE_SOURCE != "scout":
        return
    steps = (
        ("scout 汇率", lambda: scout_currency_prices(league)),
        ("scout 更新间隔", lambda: scout_probe_interval(league)),
        ("dadsofexile 基准价", lambda: doe_prices(league)),
        ("scout 交易对挂出量", lambda: scout_pair_stocks(league)),
    )
    for name, fn in steps:
        try:
            fn()
        except Exception as exc:                               # noqa: BLE001
            # 预热失败不致命： fetch_category 里还有各自的兜底与重试
            log(f"  · {name}预热失败（不阻断抓取）：{exc}")


def last_snapshot_values(league: str, before_ts: int) -> dict[str, tuple]:
    """每个通货「最后一次写进库」的值（只取 ts 早于 before_ts 的）。

    用来判断本轮抓到的值有没有变。用「最后一次写入」而不是「上一轮」——
    开了 skip_unchanged 之后两者可能隔了好几轮，比对了才有意义。
    ⚠️ 排除 synthetic：那是 poe.ninja 日线反推的历史点，拿它当基线会把
    真实值误判成「没变化」而漏写。
    """
    try:
        rows = db().execute(
            "SELECT currency_id, MAX(ts) AS mts, value_divine, value_exalted,"
            " value_chaos, stock, orders"
            " FROM snapshot WHERE league = ? AND ts < ?"
            " AND (source IS NULL OR source != 'synthetic')"
            " GROUP BY currency_id",
            (league, before_ts),
        ).fetchall()
    except Exception:  # noqa: BLE001 - 读不到就当没有基线，本轮照写
        return {}
    return {
        r["currency_id"]: (
            r["value_divine"], r["value_exalted"], r["value_chaos"],
            r["stock"], r["orders"],
        )
        for r in rows
        if r["currency_id"]
    }


def latest_snapshot_rows(league: str, fields: list[str],
                         category: str | None = None) -> list:
    """每个通货「最近一次写进库」的那一行——**跨轮次**取最新，不是「最新一轮」。

    ★ 为什么必须有它（v1.27.8 事故修复）：开了 skip_unchanged 之后，
    take_snapshot 只写「值变了」的通货，最新一轮天然是稀疏的
    （实测常只有二三十行，而全量是 649）。所有「当前价」语义的读取
    （看板 / 倒货榜 / 换算器 / 差价扫描候选）如果还按老约定
    `ts = MAX(ts)` 整轮取，就只能看到那二三十个通货——
    2026-09-29 用户实测看板只剩 26 个通货、倒货榜空白，就是这个原因。
    正确语义：每个通货各自取最近一行（值没变的那部分，最近一行就是当前值）。
    ⚠️ 排除 synthetic（日线反推点，只用于画图，不能当现价）；
    cloud 点是云端真实抓取，正常计入。
    """
    cols = ", ".join(fields)
    sql = (
        f"SELECT {cols} FROM snapshot s"
        " JOIN (SELECT currency_id, MAX(ts) AS mts FROM snapshot"
        "       WHERE league = ? AND (source IS NULL OR source != 'synthetic')"
        "       GROUP BY currency_id) m"
        "   ON s.currency_id = m.currency_id AND s.ts = m.mts"
        " WHERE s.league = ?"
    )
    params: list = [league, league]
    if category and category != "all":
        sql += " AND s.category = ?"
        params.append(category)
    return db().execute(sql, params).fetchall()


def take_snapshot(league: str) -> tuple[int, int]:
    """抓取全部类别并写入一次快照，返回 (ts, 行数)。

    类别之间并发抓取：它们彼此独立，串行只是白白把单请求的等待时间叠起来。
    行数可能是 0：开了 skip_unchanged 且本轮所有价格都没变化时就是这样，
    属于正常情况，不是抓取失败（失败仍然会抛异常）。
    """
    ts = int(time.time())
    records: list[tuple] = []
    ok_categories: set[str] = set()

    def grab(item: tuple[str, str]) -> tuple[str, list | None, str | None]:
        category, _label = item
        try:
            rows, _fe, _fc, _fd = fetch_category(league, category)
        except Exception as exc:                               # noqa: BLE001
            return category, None, str(exc)[:120]
        return category, rows, None

    warm_shared_sources(league)

    # pool.map 保序：结果顺序跟 CATEGORIES 一致，写库顺序稳定，便于对照日志
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as pool:
        results = list(pool.map(grab, CATEGORIES))

    for category, rows, error in results:
        if error is not None:
            log(f"  × {category} 抓取失败：{error}")
            continue
        for row in rows:
            if not row["id"]:
                continue
            records.append(
                (
                    ts,
                    league,
                    category,
                    row["id"],
                    row["divine"],
                    row["exalted"],
                    row["chaos"],
                    row["volume"],
                    row["trend"],
                    json.dumps(row["spark"]),
                    row.get("stock") or 0.0,
                    int(row.get("orders") or 0),
                )
            )
        ok_categories.add(category)

    fetched = len(records)
    if not fetched:
        raise RuntimeError("本次抓取没有拿到任何数据")

    # ★ 去重：与上次写进库的值完全一样的行不再写一遍。
    #   5 分钟一采 × 源 10 分钟一刷 × 加权价本就稳定 = 绝大多数轮次是重复值。
    if SKIP_UNCHANGED:
        previous = last_snapshot_values(league, ts)
        if previous:
            kept = []
            for rec in records:
                current = (rec[4], rec[5], rec[6], rec[10], rec[11])
                if previous.get(rec[3]) == current:
                    continue
                kept.append(rec)
            skipped = len(records) - len(kept)
            if skipped:
                records = kept
                log(f"  · {skipped} 个通货价格与上次记录完全相同，跳过写入（共 {fetched} 个）")

    if not records:
        # 抓到了、但一个都没变。这不是失败，只是这段时间行情没动。
        log(f"  ✓ 本轮 {fetched} 个通货价格均无变化，未写入新行")
        return ts, 0

    with db() as connection:
        connection.executemany(
            "INSERT OR REPLACE INTO snapshot"
            " (ts, league, category, currency_id, value_divine, value_exalted,"
            "  value_chaos, volume, trend, spark, stock, orders)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            records,
        )

    log(f"  ✓ 已记录 {len(records)} 条报价（{len(ok_categories)} 个类别）")
    backfilled = backfill_history(league, ts)
    if backfilled:
        log(f"  ✓ 由 7 天趋势回填了 {backfilled} 条历史价格")

    # 回填也要走完再清理，否则刚回填进来的历史行会带着 spark 留到下一轮
    prune_snapshot(league)
    return ts, len(records)


def sync_from_cloud() -> dict:
    """把云端存的历史快照补进本地库，**只填补本机没抓到的时间点**。

    为什么只补不覆盖：本机和云端抓的是同一个源、同一套口径，但毕竟是两个
    时间点、两条网络路径。本机实测优先，既避免覆盖掉自己更准的数据，也让
    两边的 source 标记互不混淆（real / cloud 在库里分得清）。
    """
    if not CLOUD_SYNC_URL:
        return {"ok": False, "reason": "未配置 cloud_sync_url（留空即不启用）"}

    import urllib.request

    def grab(url: str) -> dict:
        # 本机请求绕开系统代理，否则会被网关挡成 502
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(
            urllib.request.Request(url, headers={"User-Agent": USER_AGENT}),
            timeout=CLOUD_SYNC_TIMEOUT,
        ) as resp:
            return json.loads(resp.read().decode("utf-8", "replace"))

    try:
        payload = grab(CLOUD_SYNC_URL)
    except Exception as exc:                                   # noqa: BLE001
        if not CLOUD_SYNC_URL_RAW:
            log(f"  · 云端同步失败（不影响本机抓取）：{exc}")
            return {"ok": False, "reason": str(exc)[:120]}
        try:
            payload = grab(CLOUD_SYNC_URL_RAW)
            log(f"  · jsDelivr 取不到（{exc}），已改走 raw 源")
        except Exception as exc2:                               # noqa: BLE001
            log(f"  · 云端同步失败（不影响本机抓取）：{exc2}")
            return {"ok": False, "reason": str(exc2)[:120]}

    # CDN 那份可能是几小时前的旧版：看 generated 判断，过期就换直连源再拿一次，
    # 两边都有就取更新的那份。补历史数据时旧版也能用，但最新的点只有新版才有。
    generated = int(payload.get("generated") or 0)
    if CLOUD_SYNC_URL_RAW and generated:
        age_hours = (time.time() - generated) / 3600.0
        if age_hours > CLOUD_STALE_HOURS:
            try:
                alt = grab(CLOUD_SYNC_URL_RAW)
            except Exception:                                   # noqa: BLE001
                alt = {}
            if int(alt.get("generated") or 0) > generated:
                log(f"  · jsDelivr 这份是 {age_hours:.1f} 小时前的旧版，改用 raw 源的新版")
                payload = alt

    # ★ 云端跑的是仓库里那份 app.py（cloud_fetch.py 是 import app 的）。
    #   万一哪次封包后忘了把新 app.py 推上去，云端就会一直用旧口径抓；
    #   那种数据补进来会和本机数据混成两套口径，算出来的涨跌更不可信。
    #   所以先比对生成它的版本号：前两段（大版本.次版本）不一致就拒绝，
    #   只差补丁号（1.27.2 / 1.27.3）放行——那种通常只是修 bug，不改抓取口径。
    #   老数据里没有这个字段时按“未知”处理并放行，否则加了字段反而全补不进来。
    def _compat(v: str) -> str:
        parts = str(v or "").split(".")
        return ".".join(parts[:2]) if len(parts) >= 2 else str(v or "")

    cloud_ver = str(payload.get("app_version") or "")
    if cloud_ver and _compat(cloud_ver) != _compat(VERSION):
        log(f"  · 云端数据由 v{cloud_ver} 生成，本机 v{VERSION}，抓取口径可能不一致，本次不补")
        log("  · 处理办法：把新的 app.py 推到 GitHub（python _push_github.py）")
        return {"ok": False,
                "reason": f"云端版本 v{cloud_ver} 与本机 v{VERSION} 不一致"}

    stamps = payload.get("ts") or []
    items = payload.get("items") or {}
    league = str(payload.get("league") or STATE.get("league") or "")
    if not stamps or not items or not league:
        return {"ok": False, "reason": "云端数据为空"}
    _gen = int(payload.get("generated") or 0)
    _age = f"{(time.time() - _gen) / 3600.0:.1f} 小时前" if _gen else "未知时间"
    log(f"  · 云端数据：{len(stamps)} 个时间点 / {len(items)} 个通货（生成于 {_age}）")

    import bisect

    with db() as connection:
        # ⚠️ 这里必须把已补入的 cloud 点也算「已有」：
        #    只看 real 的话，云端补过的时间点每轮都会被判定为缺失，
        #    于是每次同步把同样的几万行 INSERT OR REPLACE 一遍——纯浪费，
        #    还会和 WAL、清理逻辑反复打架。synthetic 是日线还原点，不算数。
        have = sorted(
            int(row["ts"]) for row in connection.execute(
                "SELECT DISTINCT ts FROM snapshot WHERE league = ?"
                " AND (source IS NULL OR source != 'synthetic')", (league,)
            ).fetchall()
        )

    # ★ 本机高频、云端稀疏时，判定「本机有没有采到这个时段」不能只看时间戳相等：
    #   本机 5 分钟一采、云端 20 分钟一采，两边时间戳几乎不可能重合，
    #   按精确比对会把云端点全部当成「本机缺失」灌进来，和本机点挤在一起，
    #   等于让稀疏的云端数据掺进本已很密的本机序列里。
    #   所以改成「邻近覆盖」判断：本机在该时刻前后 grace 秒内已有采样，
    #   就认为这个时段本机已经覆盖，云端不再补。
    #   grace 取本机采集间隔——本机在线时云端几乎一个点都补不进来（正是要的效果），
    #   本机关机的那段时间本机一个点都没有，云端照样全补上。
    grace = max(_clamp_int(CONFIG.get("interval_minutes", 30), 30, 1, 1440) * 60, 300)

    targets: list[int] = []
    for i, t in enumerate(stamps):
        ts = int(t)
        pos = bisect.bisect_left(have, ts)
        covered = any(
            0 <= j < len(have) and abs(have[j] - ts) <= grace
            for j in (pos - 1, pos)
        )
        if not covered:
            targets.append(i)

    if not targets:
        return {"ok": True, "added": 0, "points": 0,
                "note": "本机已覆盖云端这些时段（本机高频优先）"}

    records: list[tuple] = []
    for idx in targets:
        ts = int(stamps[idx])
        for cid, item in items.items():
            # ⚠️ 别直接 `item["e"][idx]`：云端那份是 Actions 一轮轮推出来的，
            #    并发推送 / 漏跑 / 中途换 app.py 都会让个别通货的数组比时间轴短
            #    （2026-09-28 实测 data.json 里 650 个通货长度 3、2 个长度 1）。
            #    直接取下标会 IndexError，而外层线程只打一行日志——
            #    表现就是「云端补数据看着在跑，其实一轮都没补上」。
            def pick(key: str) -> object:
                arr = item.get(key) or []
                return arr[idx] if idx < len(arr) else None

            exalted = pick("e")
            if exalted is None:          # 该时间点没价格，跳过
                continue

            records.append(
                (
                    ts, league, str(item.get("cat") or ""), str(cid),
                    pick("d"), exalted, pick("c"),
                    0.0, None, "[]",                 # volume / trend / spark：云端不存
                    pick("s") or 0.0, int(pick("o") or 0),
                    "cloud",
                )
            )

    if records:
        with db() as connection:
            connection.executemany(
                "INSERT OR REPLACE INTO snapshot"
                " (ts, league, category, currency_id, value_divine, value_exalted,"
                "  value_chaos, volume, trend, spark, stock, orders, source)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                records,
            )
    log(f"  ✓ 云端补入 {len(records)} 条（本机缺 {len(targets)} 个时间点）")
    return {"ok": True, "added": len(records), "points": len(targets)}


# ------------------------------------------------- 离线空档：用 scout 历史自补
# 为什么要它：云端补数据靠 GitHub Actions 的 schedule，实测成功率只有 ~23%
# （14.5 小时该跑 43 次只成 10 次，最大空档 7 小时），用户关机 10 小时回来
# 云端那头往往也是空的。scout 的通货明细接口自带 36 小时 × 6 小时粒度的历史，
# 一次请求一个通货就能拿到，用它补空档不受 GitHub 漏跑影响。
#
# ⚠️ 粒度只有 6 小时（scout 的快照节奏），补不出 5 分钟粒度；
#    它的价值是「离线回来一定有东西」，不是「补得更密」。
# ⚠️ 别拿它的点去跟 doe 的实时价混着算涨跌：6 小时一个点插在本机序列里，
#    24 小时窗口内会多出几个采样点，这本来就是我们想要的（否则那段是空白），
#    但它们在库里标着 source='scout'，需要区分时查这个字段即可。
SCOUT_BACKFILL = bool(CONFIG.get("scout_backfill", True))
# 本机相邻采样间隔超过这么久才算「有空档需要补」。默认 90 分钟：
# 比 6 小时粒度小得多，免得每回启动都去打几百个请求。
SCOUT_BACKFILL_GAP = _clamp_int(
    CONFIG.get("scout_backfill_gap_minutes", 90), 90, 30, 48 * 60
) * 60
# 两次自补之间的最小间隔，默认 6 小时。补一次要打几百个请求，别太勤快。
SCOUT_BACKFILL_EVERY = _clamp_int(
    CONFIG.get("scout_backfill_every_minutes", 360), 360, 60, 48 * 60
) * 60
SCOUT_BACKFILL_WORKERS = 4


def _scout_bulk_history(league: str) -> dict[str, dict[int, float]]:
    """一次拿全部物品的小时级历史，返回 {api_id: {时间点: 以崇高石计价的价格}}。

    ★ 这个端点（/Leagues/{league}/Items/PriceHistory）比逐个通货去问好太多：
      · 1 个请求 vs 六百多个请求
      · **小时级** vs 6 小时级（scout 的 PriceLogs 只有 6 小时一点）
      · 代价是只给最近 24 小时（逐个问能给 36 小时，但粒度粗 6 倍）

    实测 2026-09-29：返回 829 个物品、每个 24 条小时级记录。
    拿不到就返回空 dict，调用方退回逐个问的老办法。
    """
    # 先把 apiId → ItemId 的映射喂热（scout_currency_prices 顺手就填了，有缓存）
    try:
        scout_currency_prices(league)
    except Exception:                                           # noqa: BLE001
        pass
    with _SCOUT_LOCK:
        id_map = {int(v): k for k, v in (_SCOUT_ITEM_IDS.get(league) or {}).items()
                  if str(v or "").isdigit()}
    if not id_map:
        return {}
    league_part = urllib.parse.quote(league)
    try:
        payload = http_json(
            f"{SCOUT_API}/{SCOUT_REALM}/Leagues/{league_part}/Items/PriceHistory",
            retries=2, timeout=60,
        )
    except Exception:                                           # noqa: BLE001
        return {}
    if not isinstance(payload, dict):
        return {}
    out: dict[str, dict[int, float]] = {}
    for entry in (payload.get("ItemHistories") or []):
        if not isinstance(entry, dict):
            continue
        api_id = id_map.get(int(entry.get("ItemId") or 0))
        if not api_id:
            continue
        hist: dict[int, float] = {}
        for e in (entry.get("History") or []):
            if not isinstance(e, dict):
                continue
            stamp = _parse_scout_time(e.get("Time"))
            try:
                price = float(e.get("Price"))
            except (TypeError, ValueError):
                continue
            if stamp > 0 and price > 0:
                hist[int(stamp)] = price
        if hist:
            out[api_id] = hist
    return out


def _scout_price_logs(league: str, api_id: str) -> dict[int, float]:
    """拉一个通货的历史价，返回 {时间点(Unix秒): 以崇高石计价的价格}。

    scout 的 /Currencies/{apiId} 给 7 个点、6 小时一个，价格以联盟基准货币
    （崇高石）计价——跟 ByCategory 的 CurrentPrice 同一口径，可以直接混用。
    拿不到就返回空 dict，调用方跳过即可。
    """
    league_part = urllib.parse.quote(league)
    try:
        payload = http_json(
            f"{SCOUT_API}/{SCOUT_REALM}/Leagues/{league_part}"
            f"/Currencies/{urllib.parse.quote(str(api_id))}",
            retries=1, timeout=20,
        )
    except Exception:                                           # noqa: BLE001
        return {}
    if not isinstance(payload, dict):
        return {}
    out: dict[int, float] = {}
    for entry in (payload.get("PriceLogs") or []):
        if not isinstance(entry, dict):
            continue
        stamp = _parse_scout_time(entry.get("Time"))
        try:
            price = float(entry.get("Price"))
        except (TypeError, ValueError):
            continue
        # scout 的时间戳末尾是 7 位小数，_parse_scout_time 已经截过；解析不了就跳过
        if stamp > 0 and price > 0:
            out[int(stamp)] = price
    return out


def backfill_from_scout(league: str | None = None) -> dict:
    """用 scout 自带的历史把本机采样空档补上。

    优先走批量端点（1 个请求拿到 829 个物品 × 24 个小时级点）；
    它挂了才退回逐个查询（6 小时粒度、覆盖 36 小时）。

    只在「本机确实有空档」时才动手，且受 SCOUT_BACKFILL_EVERY 节流。
    返回 {"ok", "added", "points", "reason"}——别只打日志，
    失败和「无事发生」在日志里长得一样，调用方要能断言。
    """
    if not SCOUT_BACKFILL:
        return {"ok": False, "added": 0, "points": 0, "reason": "scout_backfill 已关闭"}
    league = league or str(STATE.get("league") or CONFIG.get("league") or "")
    if not league:
        return {"ok": False, "added": 0, "points": 0, "reason": "未确定联盟"}

    # 节流：补一次要打几百个请求，别每次启动都来一遍
    row = db().execute("SELECT v FROM app_meta WHERE k = 'scout_backfill_at'").fetchone()
    last = int(row["v"]) if row and str(row["v"] or "").isdigit() else 0
    if last and time.time() - last < SCOUT_BACKFILL_EVERY:
        return {"ok": False, "added": 0, "points": 0,
                "reason": f"距上次自补仅 {(time.time() - last) / 60:.0f} 分钟，跳过"}

    # 本机已有的时间点（synthetic 是日线还原点，不算覆盖）
    have = sorted(
        int(r["ts"]) for r in db().execute(
            "SELECT DISTINCT ts FROM snapshot WHERE league = ?"
            " AND (source IS NULL OR source != 'synthetic')", (league,)
        ).fetchall()
    )
    # 只在最近 48 小时里找空档：更早的数据本来就会被清理
    floor = int(time.time()) - 48 * 3600
    recent = [t for t in have if t >= floor]
    gaps = [
        (recent[i - 1], recent[i]) for i in range(1, len(recent))
        if recent[i] - recent[i - 1] > SCOUT_BACKFILL_GAP
    ]
    # ⚠️ 这两条「没事可做」的分支**不写**节流时间戳：
    #    此时只查了本机库和 scout 的两个基准货币（各 1 个请求），代价可以忽略；
    #    真写了的话，用户联网跑一次（判定无空档）后再关机两小时回来，
    #    反而会被 6 小时节流挡住——正好是它该起作用的时候。
    #    节流只用来挡「几百个请求」那次真正的补数据。
    if not gaps:
        return {"ok": True, "added": 0, "points": 0, "note": "最近 48 小时没有明显空档"}

    # 基准货币的历史：既是时间网格，也提供每个点的汇率。
    # 优先批量端点（小时级、1 个请求）；拿不到再退回逐个问（6 小时级）。
    bulk = _scout_bulk_history(league)
    if bulk:
        grid = bulk.get("divine") or {}
        chaos = bulk.get("chaos") or {}
        gran = "小时级"
    else:
        grid = _scout_price_logs(league, "divine")
        chaos = _scout_price_logs(league, "chaos")
        gran = "6 小时级（批量端点不可用）"
    if not grid:
        return {"ok": False, "added": 0, "points": 0, "reason": "拿不到 scout 的基准货币历史"}

    # 只补落在空档里、且本机附近确实没有采样的时间点
    grace = max(_clamp_int(CONFIG.get("interval_minutes", 30), 30, 1, 1440) * 60, 300)
    import bisect

    targets: list[int] = []
    for ts in sorted(grid):
        if not any(a + grace <= ts <= b - grace for a, b in gaps):
            continue
        pos = bisect.bisect_left(have, ts)
        if any(0 <= j < len(have) and abs(have[j] - ts) <= grace for j in (pos - 1, pos)):
            continue
        targets.append(ts)
    if not targets:
        return {"ok": True, "added": 0, "points": 0,
                "note": f"{len(gaps)} 段空档里没有可补的 scout 时间点"}

    # 补哪些通货：库里出现过的都补，类别沿用最近一次记录的
    rows = db().execute(
        "SELECT currency_id, MAX(category) AS cat FROM snapshot"
        " WHERE league = ? GROUP BY currency_id", (league,)
    ).fetchall()
    wanted = [(r["currency_id"], r["cat"] or "") for r in rows if r["currency_id"]]
    if not wanted:
        return {"ok": False, "added": 0, "points": 0, "reason": "库里没有通货可补"}

    # ⚠️ 基准货币必须自己先有价，否则换算无从谈起；拿不到就用 0，下面会跳过该点
    rates: list[tuple[int, float, float]] = []   # (ts, divine 的 exalted 价, chaos 的 exalted 价)
    for ts in targets:
        dex = float(grid.get(ts) or 0.0)
        cex = float(chaos.get(ts) or 0.0)
        if dex > 0:
            rates.append((ts, dex, cex))

    if not rates:
        return {"ok": False, "added": 0, "points": 0, "reason": "scout 没给基准货币的汇率"}

    def one(item: tuple[str, str]) -> list[tuple]:
        cid, cat = item
        logs = bulk.get(cid) if bulk else _scout_price_logs(league, cid)
        if not logs:
            return []
        out: list[tuple] = []
        for ts, dex, cex in rates:
            ex = logs.get(ts)
            if not ex or ex <= 0:
                continue
            out.append(
                (
                    ts, league, cat, cid,
                    ex / dex if dex else None,
                    ex,
                    ex / cex if cex else None,
                    0.0, None, "[]", 0.0, 0,
                    "scout",
                )
            )
        return out

    with ThreadPoolExecutor(max_workers=SCOUT_BACKFILL_WORKERS) as pool:
        chunks = list(pool.map(one, wanted))

    records = [rec for chunk in chunks for rec in chunk]
    if records:
        with db() as connection:
            connection.executemany(
                "INSERT OR REPLACE INTO snapshot"
                " (ts, league, category, currency_id, value_divine, value_exalted,"
                "  value_chaos, volume, trend, spark, stock, orders, source)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                records,
            )
    with db() as connection:
        connection.execute(
            "INSERT OR REPLACE INTO app_meta (k, v) VALUES ('scout_backfill_at', ?)",
            (int(time.time()),),
        )
    log(f"  ✓ scout 历史补入 {len(records)} 条（{len(rates)} 个{gran}点，"
        f"覆盖 {len(gaps)} 段空档）")
    return {"ok": True, "added": len(records), "points": len(rates),
            "gaps": len(gaps), "granularity": gran}


class ScoutBackfillWorker(threading.Thread):
    """启动后跑一次 scout 历史自补。补不动就算了，绝不影响本机抓取。"""

    def __init__(self, league: str) -> None:
        super().__init__(name="scout-backfill", daemon=True)
        self.league = league

    def run(self) -> None:
        try:
            result = backfill_from_scout(self.league)
        except Exception as exc:                                # noqa: BLE001
            log(f"  · scout 历史自补出错（不影响本机抓取）：{exc}")
            return
        if not result.get("ok"):
            reason = result.get("reason") or result.get("note") or ""
            if reason:
                log(f"  · scout 历史自补跳过：{reason}")


class CloudSyncWorker(threading.Thread):
    """定时拉云端数据补洞。拉不到就跳过，绝不影响本机抓取。"""

    def __init__(self, interval: int = CLOUD_SYNC_INTERVAL) -> None:
        super().__init__(name="cloud-sync", daemon=True)
        self.interval = interval

    def run(self) -> None:
        time.sleep(6)      # 等本机首轮抓取落地，别和启动流程抢资源
        while True:
            try:
                sync_from_cloud()
            except Exception as exc:                           # noqa: BLE001
                log(f"  · 云端同步异常：{exc}")
            time.sleep(self.interval)


# --------------------------------------------------------------------- 数据库

_local = threading.local()


def db() -> sqlite3.Connection:
    connection: sqlite3.Connection | None = getattr(_local, "connection", None)
    if connection is None:
        connection = sqlite3.connect(DB_PATH, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        _local.connection = connection
    return connection


def init_db() -> None:
    with db() as connection:
        connection.executescript(SCHEMA)
        # 旧版本数据库缺列时自动补上
        columns = {row["name"] for row in connection.execute("PRAGMA table_info(snapshot)")}
        if "spark" not in columns:
            connection.execute("ALTER TABLE snapshot ADD COLUMN spark TEXT")
        if "source" not in columns:
            connection.execute("ALTER TABLE snapshot ADD COLUMN source TEXT DEFAULT 'real'")
            connection.execute("UPDATE snapshot SET source = 'real' WHERE source IS NULL")
        if "stock" not in columns:
            connection.execute("ALTER TABLE snapshot ADD COLUMN stock REAL")
        if "orders" not in columns:
            connection.execute("ALTER TABLE snapshot ADD COLUMN orders INTEGER")


# ------------------------------------------------------------------ 后台调度

STATE: dict = {
    "league": LEAGUE,
    "running": False,
    "last_update": 0,
    "next_update": 0,
    "last_count": 0,
    "last_error": "",
    "leagues": [LEAGUE],
    "categories": [{"id": c, "label": l} for c, l in CATEGORIES],
    # 联网/离线状态：断网时界面要如实告诉用户数据有多旧，而不是假装一切正常
    # 初值乐观给 True——启动那一刻还没判定过，别让图标一律走占位图
    "online": True,         # 连续抓取失败后会被置为 False
    "last_success": 0,      # 最近一次成功抓到数据的时间戳
    "offline_since": 0,     # 从什么时候开始连续失败
    "fail_streak": 0,       # 连续失败次数（连续 2 次才判定离线，避免偶发抖动误报）
}
STATE_LOCK = threading.Lock()


def fmt_duration(seconds: int) -> str:
    """把秒数说成人话：90 -> "1 分 30 秒"，60 -> "1 分钟"。"""
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds} 秒"
    minutes, rest = divmod(seconds, 60)
    return f"{minutes} 分钟" if rest == 0 else f"{minutes} 分 {rest} 秒"


_LOG_LOCK = threading.Lock()
_LOGS_PRUNED = False


def _log_file() -> Path:
    return LOG_DIR / f"{datetime.now().strftime('%Y-%m-%d')}.log"


def _write_log(line: str) -> None:
    """把一行日志追加到当天日志文件。

    ⚠️ 控制台可以被关掉（也可以压根不分配），但排障不能没有线索，
    所以这里任何异常都吞掉——写日志失败绝不能反过来把主流程搞挂。
    """
    global _LOGS_PRUNED
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        with _LOG_LOCK:
            with open(_log_file(), "a", encoding="utf-8") as fp:
                fp.write(line + "\n")
        if not _LOGS_PRUNED:
            _LOGS_PRUNED = True
            _prune_logs()
    except Exception:  # noqa: BLE001
        pass


def _prune_logs() -> None:
    """只保留最近 LOG_KEEP_DAYS 天的日志，别让它跟着程序一起越攒越大。"""
    try:
        files = sorted(LOG_DIR.glob("*.log"))
        for old in files[:-LOG_KEEP_DAYS]:
            try:
                old.unlink()
            except Exception:  # noqa: BLE001
                pass
    except Exception:  # noqa: BLE001
        pass


_LOG_THROTTLE: dict[str, float] = {}
_LOG_THROTTLE_LOCK = threading.Lock()


def log_once(key: str, message: str, every: float = 1800.0) -> None:
    """同一件事按 key 节流后再写日志。

    `fetch_category` 是**按类别**跑的（一轮 14 次），而「源站停更」这类提示描述的是
    整轮共用的一个状态 —— 不打招呼就会重复 14 遍，把真正的关键行淹掉
    （2026-09-29 实测：一轮 90 行日志里 60 行都是同一句「poe2scout 已停更」，
    后面「644 个价格无变化」这种关键行得翻很久才找得到）。
    """
    now = time.time()
    with _LOG_THROTTLE_LOCK:
        last = _LOG_THROTTLE.get(key, 0.0)
        if now - last < every:
            return
        _LOG_THROTTLE[key] = now
    log(message)


def log(message: str) -> None:
    stamp = datetime.now().strftime("%H:%M:%S")
    line = f"[{stamp}] {message}"
    try:
        print(line, flush=True)
    except UnicodeEncodeError:
        # ⚠️ 只降级「打印」，绝不能顺手把要落盘的那一行也改成 ASCII 版——
        #    以前这里是 `line = ...ascii...` 然后照旧 _write_log(line)，
        #    结果日志里凡是带中文的行全变成 "? ??? 642 ????14 ????"，
        #    而没有控制台时日志文件是唯一的排障线索，等于自断一臂。
        #    （不带控制台打包后 print 更容易踩到编码问题，所以这条必须守住。）
        try:
            enc = getattr(sys.stdout, "encoding", None) or "utf-8"
            print(line.encode(enc, "replace").decode(enc, "replace"), flush=True)
        except Exception:  # noqa: BLE001
            pass
    _write_log(line)  # 永远写原始的、带中文的那一行


def planned_interval_seconds() -> int:
    """按 scout 实际的更新间隔推算本程序该多久抓一次。

    scout 的聚合价每 6 小时才变一次，照 config 里 30 分钟抓一轮，
    抓到的全是同一个数——既白打接口，又把重复值写进历史里，
    让倒货榜算出来的波动空间偏小。所以按「源间隔的三分之一」来抓，
    保证一个更新周期内至少采样 3 次，不会整段错过。
    还没探测到源间隔时先用 config 的兜底值。
    """
    fallback = INTERVAL_SECONDS
    if not AUTO_INTERVAL or PRICE_SOURCE != "scout":
        return fallback
    with STATE_LOCK:
        league = STATE["league"]
    source_interval = int(scout_source_info(league).get("interval_seconds") or 0)
    if source_interval < 3600:
        return fallback
    return max(AUTO_INTERVAL_MIN, min(AUTO_INTERVAL_MAX, source_interval // 3))


class FetchWorker(threading.Thread):
    """后台抓取线程：启动即抓一次，之后按 planned_interval_seconds() 的节奏抓。"""

    daemon = True

    def __init__(self, interval: int = INTERVAL_SECONDS) -> None:
        super().__init__(name="fetch-worker")
        self.interval = interval
        self.wake = threading.Event()  # 切换联盟时立即唤醒重抓

    def request_switch(self, league: str) -> None:
        """切换追踪的联盟，并让抓取线程立刻重抓一次。"""
        with STATE_LOCK:
            STATE["league"] = league
            STATE["last_error"] = ""
        self.wake.set()

    def _wait(self, seconds: float) -> None:
        """等待下次抓取；期间若被唤醒（联盟切换）则提前返回。"""
        deadline = time.time() + seconds
        while time.time() < deadline:
            if self.wake.wait(min(10, max(0.5, deadline - time.time()))):
                self.wake.clear()
                return

    def _after_fetch(self, ts: int, count: int) -> None:
        """抓取成功后的收尾：更新状态、预热图标。"""
        with STATE_LOCK:
            STATE["last_update"] = ts
            STATE["last_count"] = count
            STATE["running"] = False
            STATE["online"] = True
            STATE["last_success"] = ts
            STATE["offline_since"] = 0
            STATE["fail_streak"] = 0
        # 汇率已入库，接着顺手把图标往本地搬一批：攒够了断网也能看
        try:
            got = warm_icons(ICON_WARM_PER_ROUND)
            if got:
                left = len(icon_missing_ids())
                log(f"  · 图标缓存 +{got}（剩余 {left} 个未缓存）")
        except Exception as exc:  # noqa: BLE001 - 图标只是锦上添花
            log(f"  · 图标缓存未完成：{exc}")

    def _mark_failed(self, exc: Exception) -> int:
        """记录一次抓取失败，返回连续失败次数。"""
        with STATE_LOCK:
            STATE["last_error"] = str(exc)
            STATE["next_update"] = int(time.time()) + 300
            STATE["running"] = False
            streak = int(STATE.get("fail_streak", 0)) + 1
            STATE["fail_streak"] = streak
            if not STATE["offline_since"]:
                STATE["offline_since"] = int(time.time())
            if streak >= 2:
                STATE["online"] = False
        return streak

    def run(self) -> None:
        """主循环：先用低成本探测问「变了吗」，变了才拉全量。

        节奏不是写死的：摸清源站更新周期后，临近更新点就压到最短间隔紧盯，
        离得远就放宽间隔省请求；万一 ETag 探测失效，兜底时间到了也会强制抓。
        """
        first_round = True
        load_tracker_state(TRACKER)
        if TRACKER.typical_interval():
            log(f"  已载入上次的源站节奏：约 {round(TRACKER.typical_interval() / 60, 1)} 分钟更新一次")
        while True:
            self.wake.clear()
            # 每轮都重算一次：探测到 scout 的真实更新间隔后，节奏会随之变长
            self.interval = planned_interval_seconds()
            with STATE_LOCK:
                STATE["running"] = True
                STATE["last_error"] = ""
                league = STATE["league"]
            now = int(time.time())
            try:
                need_full = first_round  # 首次必须抓，既是建基线也是为了有数据可看
                if not first_round and ADAPTIVE_POLL:
                    changed, code = probe_updates(league)
                    if code == 429:
                        # 被限流：什么都不做，老老实实退避，别硬撞
                        backoff = min(POLL_MAX_SECONDS, max(600, TRACKER.wait * 2))
                        log(f"  · 探测被限流（429），退避 {backoff // 60} 分钟后再试")
                        with STATE_LOCK:
                            STATE["running"] = False
                            STATE["online"] = True  # 限流不等于断网
                            STATE["next_update"] = now + backoff
                        self._wait(backoff)
                        continue
                    if code == -1:
                        # 探测本身失败了（多半是断网），标记一次失败
                        streak = self._mark_failed(RuntimeError("探测请求失败"))
                        log(f"探测失败（连续 {streak} 次），5 分钟后重试")
                        self._wait(min(300, self.interval))
                        continue
                    stale = (now - int(STATE.get("last_success") or 0)) > POLL_FALLBACK_SECONDS
                    if changed:
                        log("  ✓ 探测到源站数据已更新，立即拉取")
                        need_full = True
                        TRACKER.note_change(now)
                    elif stale:
                        log(f"  · 距上次抓取已超过 {POLL_FALLBACK_SECONDS // 60} 分钟，兜底全量抓一次")
                        need_full = True
                    else:
                        TRACKER.note_no_change()

                if need_full:
                    log(f"开始抓取 {league} …")
                    ts, count = take_snapshot(league)
                    # 暗金是另一套接口，抓失败不能连累通货数据
                    try:
                        take_unique_snapshot(league, ts)
                    except Exception as exc:  # noqa: BLE001
                        log(f"  × 暗金快照失败：{exc}")
                    with STATE_LOCK:
                        STATE["next_update"] = ts + self.interval
                    log(f"抓取完成，写入 {count} 行")
                    self._after_fetch(ts, count)
                    first_round = False
                    wait = TRACKER.next_wait(int(time.time()))
                    with STATE_LOCK:
                        STATE["next_update"] = int(time.time()) + wait
                    tip = f"  下次探测 {fmt_duration(wait)}后"
                    typical = TRACKER.typical_interval()
                    if typical:
                        tip += f"（已摸清节奏：源站约 {round(typical / 60, 1)} 分钟更新一次）"
                    log(tip)
                else:
                    with STATE_LOCK:
                        STATE["running"] = False
                    wait = TRACKER.next_wait(int(time.time()))
                    with STATE_LOCK:
                        STATE["next_update"] = int(time.time()) + wait
                self._wait(wait)
            except Exception as exc:  # noqa: BLE001
                streak = self._mark_failed(exc)
                log(f"抓取失败：{exc}（5 分钟后重试，连续失败 {streak} 次）")
                self._wait(min(300, self.interval))


    def ensure_item_catalog(self) -> None:
        """条目为空或超过一天未更新时补齐名称/图标元数据。"""
        with db() as connection:
            row = connection.execute(
                "SELECT v FROM app_meta WHERE k = 'catalog_updated'"
            ).fetchone()
            count = connection.execute("SELECT COUNT(*) AS c FROM item_meta").fetchone()["c"]
        fresh = row and (int(time.time()) - int(row["v"])) < 86400
        if fresh and count > 0:
            return
        log("同步通货名称与图标元数据 …")
        try:
            total = load_item_catalog()
            if total:
                with db() as connection:
                    connection.execute(
                        "INSERT OR REPLACE INTO app_meta (k, v) VALUES ('catalog_updated', ?)",
                        (str(int(time.time())),),
                    )
                log(f"  ✓ 元数据 {total} 条")
                invalidate_exclusions()  # 物品库更新后重新解析排除名单
        except Exception as exc:  # noqa: BLE001
            log(f"  × 元数据同步失败：{exc}")

        # 每天同步时顺带探测一次官方新增类别，新版本加了新物品分类也能自动跟上
        if CONFIG.get("auto_discover_categories"):
            try:
                added = discover_categories()
                if added:
                    global CATEGORIES
                    CATEGORIES = enabled_categories()
                    log(f"  ✓ 新增 {added} 个类别")
            except Exception as exc:  # noqa: BLE001
                log(f"  · 类别探测跳过：{exc}")
            with STATE_LOCK:
                STATE["categories"] = [{"id": c, "label": l} for c, l in CATEGORIES]


class SpreadWorker(threading.Thread):
    """独立线程：小批量、慢节奏地积累真实买卖挂单。

    官方交易接口限流很严（触发后往往要罚等 2-3 分钟），所以不能一次扫很多，
    改成每轮只扫几个、间隔一段时间再来，一天下来就能铺满榜单。
    """

    def __init__(self, round_seconds: int, pairs: int) -> None:
        super().__init__(name="spread-worker", daemon=True)
        self.round_seconds = round_seconds
        self.pairs = pairs
        self.wake = threading.Event()

    def request_switch(self) -> None:
        self.wake.set()

    def run(self) -> None:
        # 启动时先让主抓取和物品库同步跑完，别一上来就去抢官方交易接口
        time.sleep(30)
        first_round = True
        while True:
            if self.wake.wait(self.round_seconds):
                self.wake.clear()
            try:
                # 还在官方罚等里就老实等到解禁，别让罚时越滚越长
                wait = TRADE_LIMITER.snapshot()["waiting"]
                if wait > 0:
                    time.sleep(min(wait, 900))
                # 刚启动时多扫一批，让榜单尽快有东西看，之后转入小批量慢速积累
                count = max(self.pairs, SPREAD_TOP_N) if first_round else self.pairs
                written = build_spread_snapshot(STATE["league"], count)
                first_round = False
                if written:
                    log(f"  ✓ 挂单扫描写入 {written} 组（每 {self.round_seconds} 秒一轮）")
            except Exception as exc:  # noqa: BLE001
                log(f"  · 挂单扫描未完成：{exc}")


# --------------------------------------------------------------------- 服务层

def rows_to_items(base: str, category: str, query: str, hours: int) -> dict:
    """组装列表接口数据：当前值 + 区间涨跌 + 迷你走势。"""
    column = BASE_COLUMNS[base]
    latest_ts_row = db().execute(
        "SELECT MAX(ts) AS ts FROM snapshot WHERE league = ?", (STATE["league"],)
    ).fetchone()
    if not latest_ts_row or latest_ts_row["ts"] is None:
        return {"meta": build_meta(base, 0, 0), "items": []}

    latest_ts = int(latest_ts_row["ts"])
    cutoff = latest_ts - hours * 3600

    # ★ 当前值按「每个通货各自最近一行」取，不能按 ts=最新一轮整轮取：
    #   开了 skip_unchanged 后最新一轮只含值变了的通货（v1.27.8 修复）。
    latest_rows = latest_snapshot_rows(
        STATE["league"],
        ["s.currency_id", "s.category", "s.value_divine", "s.value_chaos",
         "s.value_exalted", "s.volume", "s.trend", "s.spark"],
        category,
    )

    sql_series = (
        f"SELECT currency_id, ts, {column} AS value FROM snapshot"
        " WHERE league = ? AND ts >= ?"
        # 同 arbitrage_rows：日线 synthetic 点不能进区间涨跌的计算
        " AND (source IS NULL OR source != 'synthetic')"
        " ORDER BY ts ASC"
    )
    series: dict[str, list[tuple[int, float]]] = {}
    for row in db().execute(sql_series, [STATE["league"], cutoff]).fetchall():
        value = row["value"]
        if value is None or value != value:  # 过滤 NaN
            continue
        series.setdefault(row["currency_id"], []).append((row["ts"], value))

    meta_rows = {
        row["currency_id"]: row
        for row in db().execute("SELECT * FROM item_meta").fetchall()
    }

    labels = dict(CATEGORIES)
    items: list[dict] = []
    no_zh = 0
    for row in latest_rows:
        cid = row["currency_id"]
        # 三种基准同时给出，前端可以直接对比换算
        values = {
            "exalted": row["value_exalted"],
            "chaos": row["value_chaos"],
            "divine": row["value_divine"],
        }
        value = values.get(base)
        if value is None or value != value:
            continue
        meta = meta_rows.get(cid)
        name_en = meta["name_en"] if meta and meta["name_en"] else cid
        name_zh = zh_display_name(cid, meta)
        icon = meta["icon"] if meta and meta["icon"] else ""

        if not matches_query(query, cid, name_en, name_zh):
            continue
        if not name_zh:
            no_zh += 1
            continue

        points = series.get(cid) or []
        try:
            ninja_spark = json.loads(row["spark"] or "[]")
        except (TypeError, ValueError):
            ninja_spark = []
        ninja_trend = float(row["trend"]) if row["trend"] is not None else None

        change = None
        if len(points) >= 2 and points[0][1]:
            change = (points[-1][1] - points[0][1]) / points[0][1] * 100.0

        # 走势点统一为 [时间戳, 数值] 形式，前端悬停时才能显示对应时点的价格。
        # 只要本机存了 2 个以上采样点就一律用自己的记录：详情大图读的是同一张表、同一段区间，
        # 两边共用一份数据，卡片小图和大图的形状才不会对不上。
        step = max(1, len(points) // 40)
        if len(points) >= LOCAL_POINTS_MIN:
            spark = [list(p) for p in points[::step]]
            spark_src = "local"
        elif isinstance(ninja_spark, list) and len(ninja_spark) >= 2:
            # 部分序列里会夹带 null，需过滤后再转 float
            cleaned = []
            for sample in ninja_spark:
                try:
                    cleaned.append(float(sample))
                except (TypeError, ValueError):
                    continue
            if len(cleaned) >= 2:
                # ninja 的趋势没有时间戳，用序号占位
                spark = [[index, sample] for index, sample in enumerate(cleaned)]
                spark_src = "ninja"
                change = ninja_trend if ninja_trend is not None else change
            else:
                spark = [list(p) for p in points[::step]]
                spark_src = "local"
        else:
            spark = [list(p) for p in points[::step]]
            spark_src = "local"
        if change is None:
            change = ninja_trend

        items.append(
            {
                "id": cid,
                "category": row["category"],
                "category_label": labels.get(row["category"], row["category"]),
                "name": name_en,
                "name_zh": name_zh,
                "icon": f"/icon?id={urllib.parse.quote(cid)}" if icon else "",
                "value": value,
                "values": values,
                "volume": row["volume"] or 0.0,
                "change": change,
                "spark": spark,
                "spark_src": spark_src,
            }
        )
    meta = build_meta(base, latest_ts, len(items))
    meta["no_zh"] = no_zh
    return {"meta": meta, "items": items}


CALC_BASES = ("exalted", "chaos", "divine")


def calc_payload() -> dict:
    """换汇计算器所需的数据。

    前端拿它做两件事：
      1. 把用户手工录入的「目标通货 换 崇高石 / 混沌石 / 神圣石」报价折算到同一把尺子上；
      2. 提供通货清单与聚合参考价，方便填表时对照。

    为什么不做自动抓挂单：官方 trade2 的挂单和游戏内交易所不是同一批数据
    （详见 _废弃_悬浮窗方案/问题总结.md），自动抓出来的价和游戏里对不上，
    所以这里只提供聚合汇率，买卖报价由用户按游戏内实际看到的手工录入。
    """
    row = db().execute(
        "SELECT MAX(ts) AS ts FROM snapshot WHERE league = ?", (STATE["league"],)
    ).fetchone()
    if not row or row["ts"] is None:
        return {"meta": build_meta("exalted", 0, 0), "bases": {}, "rates": {}, "items": []}

    latest_ts = int(row["ts"])
    rows = latest_snapshot_rows(
        STATE["league"],
        ["s.currency_id", "s.category", "s.value_divine", "s.value_chaos",
         "s.value_exalted"],
    )
    meta_rows = {r["currency_id"]: r for r in db().execute("SELECT * FROM item_meta").fetchall()}
    labels = dict(CATEGORIES)

    # 三种基准货币的「身价」：以崇高石为 1，算出混沌石 / 神圣石各值多少崇高石。
    # 它们自己也在快照里，直接取最准；快照里没有时用其它通货的三种报价反推中位数。
    by_id = {r["currency_id"]: r for r in rows}
    bases: dict[str, float] = {"exalted": 1.0}
    for cid in ("chaos", "divine"):
        entry = by_id.get(cid)
        value = float(entry["value_exalted"] or 0) if entry else 0.0
        if value > 0:
            bases[cid] = value
    if len(bases) < len(CALC_BASES):
        samples: dict[str, list[float]] = {"chaos": [], "divine": []}
        for entry in rows:
            exalted_value = float(entry["value_exalted"] or 0)
            if exalted_value <= 0:
                continue
            for cid in samples:
                other = float(entry[f"value_{cid}"] or 0)
                if other > 0:
                    # 1 个 cid 值多少崇高石 = 崇高石报价 / cid 报价
                    samples[cid].append(exalted_value / other)
        for cid, values in samples.items():
            if cid not in bases and values:
                bases[cid] = sorted(values)[len(values) // 2]

    # rates[a][b] = 1 个 a 值多少个 b
    rates = {
        a: {b: (bases[a] / bases[b] if bases.get(b) else 0.0) for b in CALC_BASES}
        for a in CALC_BASES
    }

    def icon_url(cid: str) -> str:
        meta = meta_rows.get(cid)
        raw = meta["icon"] if meta and meta["icon"] else ""
        return f"/icon?id={urllib.parse.quote(cid)}" if raw else ""

    items: list[dict] = []
    for entry in rows:
        cid = entry["currency_id"]
        values = {
            "exalted": entry["value_exalted"],
            "chaos": entry["value_chaos"],
            "divine": entry["value_divine"],
        }
        if not values["exalted"] or values["exalted"] != values["exalted"]:
            continue
        meta = meta_rows.get(cid)
        name_zh = zh_display_name(cid, meta)
        if not name_zh:
            continue
        items.append(
            {
                "id": cid,
                "name": (meta["name_en"] if meta and meta["name_en"] else cid),
                "name_zh": name_zh,
                "category": entry["category"],
                "category_label": labels.get(entry["category"], entry["category"]),
                "icon": icon_url(cid),
                "values": values,
            }
        )
    items.sort(key=lambda x: (x["category"], -(x["values"]["exalted"] or 0)))

    # 三个基准货币自己的图标，前端三张卡片要用
    base_icons = {cid: icon_url(cid) for cid in CALC_BASES}

    return {
        "meta": build_meta("exalted", latest_ts, len(items)),
        "bases": bases,
        "rates": rates,
        "items": items,
        "base_icons": base_icons,
    }


def _meta_value(key: str) -> int:
    row = db().execute("SELECT v FROM app_meta WHERE k = ?", (key,)).fetchone()
    return int(row["v"]) if row and row["v"] else 0


def build_meta(base: str, latest_ts: int, count: int) -> dict:
    with STATE_LOCK:
        state = dict(STATE)
    snapshot_row = db().execute(
        "SELECT COUNT(DISTINCT ts) AS c FROM snapshot WHERE league = ?",
        (state["league"],),
    ).fetchone()
    now = int(time.time())
    last_ok = int(state.get("last_success") or state["last_update"] or 0)
    # 源数据的更新时间。主源现在是 dadsofexile（分钟级刷新），得报它的采集时间；
    # 拿不到再退回 scout，ninja 完全没有源时间戳。
    source_info: dict[str, int] = {}
    # 实际用的是哪个源就报哪个：doe 拿到了就是它，拿不到才退回 scout。
    # 之前这里写死了 poe2scout，结果价格已经换成 dadsofexile 了，界面还标着旧源。
    source_name = "poe.ninja"
    if PRICE_SOURCE == "scout":
        source_info = doe_source_info(state["league"])
        if source_info.get("updated_at"):
            source_name = "dadsofexile"
        else:
            scout_info = scout_source_info(state["league"])
            if scout_info.get("updated_at"):
                source_info, source_name = scout_info, "poe2scout"
            else:
                # 冷启动：两个源都还没取过。按配置的主源显示，别谎报成 poe2scout——
                # 那样用户会以为价格仍来自那个系统性偏高的源。
                source_name = "dadsofexile"
    source_updated = int(source_info.get("updated_at") or 0)
    # 断网时最有价值的信息：手里这批数据有多旧、还能看多久
    return {
        # 带上版本号：exe 里塞了整个依赖链，靠二进制字符串判断版本根本不可靠，
        # 只能跑起来读这里（踩过：1.17/1.18/1.19 三个版本号在同一个 exe 里全都命中）
        "version": VERSION,
        "league": state["league"],
        "base": base,
        "base_label": BASE_LABELS[base],
        "last_update": latest_ts or state["last_update"],
        "next_update": state["next_update"],
        # 源自身的更新时间：价格到底是哪一时刻的市场价。
        # 和 last_update（本机快照写入时间）不是一个东西，断网时前者才说明数据有多旧。
        "source_updated": source_updated,
        "source_interval": source_info.get("interval_seconds", 0),
        "source_lag_minutes": (
            max(0, (now - source_updated) // 60) if source_updated else None
        ),
        "source": source_name,
        "fetching": state["running"],
        "error": state["last_error"],
        # 展示的是实际生效的节奏（自动调整后可能比 config 里写的长）
        "interval": planned_interval_seconds(),
        "auto_interval": AUTO_INTERVAL,
        "item_count": count,
        "leagues": state["leagues"],
        "snapshot_count": int(snapshot_row["c"]) if snapshot_row else 0,
        "online": bool(state.get("online")),
        "last_success": last_ok,
        "offline_since": int(state.get("offline_since") or 0),
        "data_age_minutes": max(0, (now - last_ok) // 60) if last_ok else None,
        "retention_days": RETENTION_DAYS,
        "poll": TRACKER.snapshot(now),
        "categories": state["categories"] or [{"id": c, "label": l} for c, l in CATEGORIES],
        "config": {
            "auto_follow_latest_league": CONFIG.get("auto_follow_latest_league", True),
            "auto_discover_categories": CONFIG.get("auto_discover_categories", True),
            "interval_minutes": INTERVAL_SECONDS // 60,
            "config_path": str(config_path()),
        },
    }


def _history_from_scout(currency_id: str, hours: int, meta) -> dict:
    """直接向 poe2scout 要这个通货的历史（小时级，最近 24 小时）。

    批量端点一次就能拿到全部物品；它挂了才退回逐个问（6 小时粒度、36 小时）。
    价格是 exalted 计价，divine/chaos 用同一时间网格上基准货币的价换算。
    """
    league = str(STATE.get("league") or "")
    cutoff = time.time() - hours * 3600
    bulk = _scout_bulk_history(league)
    hist = (bulk.get(currency_id) if bulk else None) or _scout_price_logs(league, currency_id)
    div = (bulk.get("divine") if bulk else None) or _scout_price_logs(league, "divine")
    ch = (bulk.get("chaos") if bulk else None) or _scout_price_logs(league, "chaos")
    gran = "小时级" if bulk else "6 小时级（批量端点不可用）"

    ex_pts, ch_pts, dv_pts = [], [], []
    for ts in sorted(hist):
        if ts < cutoff:
            continue
        ex = hist[ts]
        ex_pts.append([ts, ex])
        if ch.get(ts):
            ch_pts.append([ts, ex / ch[ts]])
        if div.get(ts):
            dv_pts.append([ts, ex / div[ts]])
    return _history_shell(currency_id, meta, ex_pts, ch_pts, dv_pts, "scout", gran)


def _history_from_ninja(currency_id: str, hours: int, meta) -> dict:
    """从 poe.ninja 的 7 天 sparkline 反推绝对价（日线）。

    ⚠️ ninja 没有绝对值历史端点（exchange/item/currency/temp2 的 history 全是 404），
    只有 sparkline：data[i] 是「窗口起点到该点」的累计涨跌百分比，
    最后一个元素等于 totalChange（即整个窗口的总涨跌）。
    所以：起点价 = 当前价 / (1 + totalChange/100)，再按 data[i] 逐点还原。
    时间戳没有给，按「每天一点、最后一点是现在」推算——**只能当趋势看**。
    """
    league = str(STATE.get("league") or "")
    cutoff = time.time() - hours * 3600
    q = urllib.parse.urlencode({"league": league, "type": "Currency"})
    payload = http_json(f"{NINJA_API}/exchange/current/overview?{q}", retries=2)
    rates = (payload.get("core") or {}).get("rates") or {}
    n_ex = float(rates.get("exalted") or 0.0)     # 1 divine = ? exalted
    n_ch = float(rates.get("chaos") or 0.0)       # 1 divine = ? chaos
    line = next((l for l in (payload.get("lines") or [])
                 if l.get("id") == currency_id), None)
    if not line or not n_ex:
        return _history_shell(currency_id, meta, [], [], [], "ninja", "无数据")

    pv = float(line.get("primaryValue") or 0.0)   # 当前价，divine 计价
    spark = line.get("sparkline") or {}
    data = spark.get("data") or []
    total = float(spark.get("totalChange") or 0.0)
    n = len(data)
    start = pv / (1 + total / 100.0) if total > -100 else pv

    ex_pts, ch_pts, dv_pts = [], [], []
    now = time.time()
    for i, d in enumerate(data):
        ts = int(now - (n - 1 - i) * 86400)
        if ts < cutoff:
            continue
        try:
            v = start * (1 + float(d) / 100.0)
        except (TypeError, ValueError):
            continue
        dv_pts.append([ts, v])
        ex_pts.append([ts, v * n_ex])
        if n_ch:
            ch_pts.append([ts, v * n_ch])
    return _history_shell(currency_id, meta, ex_pts, ch_pts, dv_pts, "ninja", "日线（7 天）")


def _history_shell(currency_id, meta, ex_pts, ch_pts, dv_pts, src, gran) -> dict:
    return {
        "id": currency_id,
        "name": zh_display_name(currency_id, meta) or (
            meta["name_en"] if meta and meta["name_en"] else currency_id
        ),
        "name_zh": zh_display_name(currency_id, meta),
        "name_en": meta["name_en"] if meta and meta["name_en"] else currency_id,
        "points": {"exalted": ex_pts, "chaos": ch_pts, "divine": dv_pts},
        "src": src,
        "granularity": gran,
        "synthetic_points": 0,
    }


def query_history(currency_id: str, hours: int, src: str = "db") -> dict:
    """返回某个通货在三种基准下的历史序列，供详情弹窗切换查看。

    src:
      db    = 本机记录（默认；本机 5 分钟一采，覆盖 646 种通货）
      scout = 直接问 poe2scout 要（小时级，最近 24 小时）
      ninja = 从 poe.ninja 的 7 天 sparkline 反推（日线）
    """
    meta = db().execute(
        "SELECT * FROM item_meta WHERE currency_id = ?", (currency_id,)
    ).fetchone()
    src = str(src or "db").strip().lower()
    if src == "scout":
        try:
            return _history_from_scout(currency_id, hours, meta)
        except Exception as exc:                                # noqa: BLE001
            log(f"  · 取 poe2scout 历史失败（{exc}），退回本机记录")
    elif src == "ninja":
        try:
            return _history_from_ninja(currency_id, hours, meta)
        except Exception as exc:                                # noqa: BLE001
            log(f"  · 取 poe.ninja 历史失败（{exc}），退回本机记录")

    cutoff = int(time.time()) - hours * 3600
    rows = db().execute(
        "SELECT ts, value_exalted, value_chaos, value_divine, source FROM snapshot"
        " WHERE league = ? AND currency_id = ? AND ts >= ? ORDER BY ts ASC",
        (STATE["league"], currency_id, cutoff),
    ).fetchall()

    def series(column: str) -> list[list[float]]:
        out: list[list[float]] = []
        for row in rows:
            value = row[column]
            if value is None or value != value:  # 过滤 NaN
                continue
            out.append([row["ts"], value])
        return out

    result = _history_shell(
        currency_id, meta,
        series("value_exalted"), series("value_chaos"), series("value_divine"),
        "db", f"本机记录（{INTERVAL_SECONDS // 60} 分钟一采）",
    )
    result["synthetic_points"] = sum(1 for row in rows if row["source"] == "synthetic")
    return result


# ------------------------------------------------------------------- 暗金榜

UNIQUE_VALUE_COLUMNS = {
    "exalted": "value_exalted",
    "chaos": "value_chaos",
    "divine": "value_divine",
}


def unique_zh(name: str) -> str:
    """暗金中文名：优先查 poe2db 词典，再用 config.json 的 unique_name_zh 手工补。"""
    zh: str = ZH.name(name)
    if zh:
        return zh
    table = CONFIG.get("unique_name_zh") or {}
    if isinstance(table, dict):
        return str(table.get(name) or "").strip()
    return ""


def unique_base_zh(name: str, base_type: str) -> str:
    """底材中文名：先按底材英文名查，再退回该暗金的主底材译名。"""
    zh: str = ZH.base_type(base_type) if base_type else ""
    if zh:
        return zh
    return ZH.base(name)


# ------------------------------------------------------------- 暗金细分类
# poe.ninja 只给 8 个粗类，UniqueArmours 里头盔/护手/胸甲/盾牌全混在一起，
# 按装备部位挑东西很不方便。粗类已经足够准的（碑牌/珠宝/武器…）直接采用，
# 护甲与首饰再按底材英文名细分一层。
UNIQUE_SUBCAT_BY_CATEGORY = {
    "UniqueTablets": "碑牌",
    "UniqueJewels": "珠宝",
    "UniqueCharms": "护符",
    "UniqueSanctumRelics": "遗物",
    "UniqueFlasks": "药剂",
    "UniqueWeapons": "武器",
}
UNIQUE_SUBCAT_ARMOUR: list[tuple[str, str]] = [
    ("盾牌", r"\b(?:Shield|Buckler)\b"),
    ("头盔", r"\b(?:Helmet|Greathelm|Circlet|Crown|Hood|Burgonet|Mask|Sallet|Bascinet|Cap)\b"),
    ("护手", r"\b(?:Gloves|Gauntlets|Mitts|Wraps|Grips)\b"),
    ("靴", r"\b(?:Boots|Greaves|Sandals|Shoes|Slippers|Sabatons)\b"),
    ("护甲", r"\b(?:Vest|Coat|Cuirass|Plate|Mail|Robe|Raiment|Armour|Mantle|Shroud|Carapace|Garb|Tunic|Scale)\b"),
]
UNIQUE_SUBCAT_ACCESSORY: list[tuple[str, str]] = [
    ("戒指", r"\bRing\b"),
    ("项链", r"\bAmulet\b"),
    ("腰带", r"\bBelt\b"),
]


def unique_subcategory(category: str, base_type: str) -> str:
    """把 ninja 的粗分类细化成装备部位：头盔 / 护手 / 护甲 / 盾牌 / 武器 / 碑牌 / 珠宝…"""
    fixed: str = UNIQUE_SUBCAT_BY_CATEGORY.get(category or "")
    if fixed:
        return fixed
    rules = UNIQUE_SUBCAT_ACCESSORY if category == "UniqueAccessories" else UNIQUE_SUBCAT_ARMOUR
    for label, pattern in rules:
        if re.search(pattern, base_type or "", re.I):
            return label
    if category == "UniqueAccessories":
        return "首饰"
    if category == "UniqueArmours":
        return "护甲"
    return category or "其他"


def unique_mod_impact(tiers: list[dict]) -> list[dict]:
    """找出造成档位差价的词条。

    做法是先把每条词缀的数值抹掉得到模板，同一模板视为同一条词条，
    再看它在各个档位之间的数值跨度——跨度越大，越可能是差价的来源。
    """
    groups: dict[str, dict] = {}
    for index, tier in enumerate(tiers):
        mods: list[tuple[str, dict]] = [("p", m) for m in (tier.get("properties") or [])]
        mods += [("e", m) for m in (tier.get("explicit") or [])]
        for kind, mod in mods:
            text: str = mod.get("text") or ""
            tmpl, _ = zhdict.template_of(zhdict.clean_en(text))
            if not tmpl:
                continue
            group = groups.setdefault(tmpl, {"tmpl": tmpl, "kind": kind, "samples": []})
            group["samples"].append(
                {"index": index, "lo": mod.get("lo"), "hi": mod.get("hi")}
            )

    out: list[dict] = []
    for group in groups.values():
        # 每个档位取该词条的中值，比较的是「档位之间」的差别，
        # 不是词条自身的 roll 区间——后者在同一档位内也会有跨度。
        mids: list[tuple[int, float]] = []
        for s in group["samples"]:
            if s["lo"] is None or s["hi"] is None:
                continue
            mids.append((s["index"], (float(s["lo"]) + float(s["hi"])) / 2.0))
        if len(mids) < 2:
            continue
        lo = min(m for _, m in mids)
        hi = max(m for _, m in mids)
        span = abs(hi - lo)
        if span <= 0:
            continue
        scale = max(abs(hi), abs(lo), 1e-9)
        out.append(
            {
                "zh": mod_label(group["tmpl"], group["kind"]),
                "en": zhdict.clean_en(group["tmpl"]),
                "kind": group["kind"],
                "lo": lo,
                "hi": hi,
                "span": span,
                "ratio": span / scale,
                "tiers": len({i for i, _ in mids}),
                "samples": group["samples"],
            }
        )
    out.sort(key=lambda o: (-o["ratio"], -o["span"]))
    return out[:8]


def unique_latest_ts(league: str) -> int:
    row = db().execute(
        "SELECT MAX(ts) AS ts FROM unique_snapshot WHERE league = ?", (league,)
    ).fetchone()
    return int(row["ts"]) if row and row["ts"] else 0


def _unique_spark(raw: str | None) -> list[float]:
    try:
        data = json.loads(raw or "[]")
    except (TypeError, ValueError):
        return []
    out: list[float] = []
    for sample in data if isinstance(data, list) else []:
        try:
            out.append(float(sample))
        except (TypeError, ValueError):
            continue
    return out


def weighted_median(tiers: list[dict]) -> float | None:
    """按挂单数加权的市场中位数。

    每件暗金在 poe.ninja 里会被拆成若干 roll 档位，各档挂单量差得很远。
    直接把每档价格取平均会被极端档位带偏，而只挑「挂单最多的一档」又丢掉了
    其它档位的信息。把每档的挂单数当权重求中位数，得到的就是「随机抓一个在
    售挂单，价格最可能的落点」，最接近玩家实际能成交的价。
    """
    points = sorted(
        ((float(t["value"]), max(1, int(t.get("listing_count") or 0))) for t in tiers),
        key=lambda p: p[0],
    )
    if not points:
        return None
    total = sum(w for _, w in points)
    acc = 0
    for value, weight in points:
        acc += weight
        if acc * 2 >= total:
            return value
    return points[-1][0]


def unique_rows(base: str, category: str, query: str) -> dict:
    """暗金榜：同一件物品按 roll 档位拆成多行，这里按物品归组后返回。

    参考价取「按挂单数加权的市场中位数」——比挑单一档位更能代表真实成交价；
    污染与未污染分别给出各自的中位数，供对比参考。
    挂单数低于阈值的档位只参与区间统计，不参与中位数计算。
    """
    if base not in UNIQUE_VALUE_COLUMNS:
        base = "divine"
    league: str = STATE["league"]
    ts: int = unique_latest_ts(league)
    empty = {
        "league": league, "ts": ts, "base": base,
        "min_listing": UNIQUE_MIN_LISTING, "categories": [], "items": [], "pending": ts == 0,
    }
    if not ts:
        return empty
    column: str = UNIQUE_VALUE_COLUMNS[base]
    rows = db().execute(
        "SELECT item_key, name, base_type, category, icon, level_req, corrupted,"
        f" {column} AS value, listing_count, trend, spark FROM unique_snapshot"
        " WHERE league = ? AND ts = ?",
        (league, ts),
    ).fetchall()

    groups: dict[str, dict] = {}
    for index, row in enumerate(rows):
        value = row["value"]
        if value is None or value != value:  # NULL / NaN
            continue
        name: str = row["name"] or ""
        if not name:
            continue
        if not matches_query(query, name, row["base_type"] or "", unique_zh(name)):
            continue
        # 只按物品名归组：poe.ninja 会把同一件暗金拆成「底材变体」和「roll 档位」两类多行，
        # 玩家眼里它们都是同一件东西，拆开就看不出差价是谁造成的了。
        group = groups.setdefault(
            name,
            {
                "name": name,
                "zh": unique_zh(name),
                "base_types": {},
                "categories": set(),
                "icon": row["icon"] or "",
                "level_req": int(row["level_req"] or 0),
                "corrupted": 0,
                "tiers": [],
                "listings": 0,
            },
        )
        base_type = row["base_type"] or ""
        group["base_types"][base_type] = (
            group["base_types"].get(base_type, 0) + int(row["listing_count"] or 0)
        )
        group["categories"].add(row["category"] or "")
        group["corrupted"] = max(group["corrupted"], int(row["corrupted"] or 0))
        group["tiers"].append(
            {
                "key": row["item_key"],
                "value": float(value),
                "listing_count": int(row["listing_count"] or 0),
                "trend": float(row["trend"] or 0.0),
                "spark": _unique_spark(row["spark"]),
                "corrupted": int(row["corrupted"] or 0),
            }
        )
        group["listings"] += int(row["listing_count"] or 0)

    items: list[dict] = []
    for group in groups.values():
        cats = sorted(c for c in group["categories"] if c)
        # 底材可能有好几种变体，展示时挑挂单最多的那个，其余在档位表里能看到
        base_type = max(group["base_types"].items(), key=lambda kv: kv[1])[0] if group["base_types"] else ""
        sub_cat = unique_subcategory(cats[0] if cats else "", base_type)
        # 前端既能按粗类筛，也能按装备部位（头盔/护手/护甲/盾牌…）筛
        if category and category not in ("all", "") and category not in cats and category != sub_cat:
            continue
        tiers = sorted(group["tiers"], key=lambda t: (-t["listing_count"], t["value"]))
        valid = [t for t in tiers if t["listing_count"] >= UNIQUE_MIN_LISTING]
        main = (valid or tiers)[0]
        pool = valid or tiers
        values = [t["value"] for t in pool]
        # 参考价 = 按挂单数加权的市场中位数；污染 / 未污染各自再算一个便于对比
        median = weighted_median(pool)
        clean_pool = [t for t in pool if not t.get("corrupted")]
        dirty_pool = [t for t in pool if t.get("corrupted")]
        items.append(
            {
                "key": main["key"],
                "name": group["name"],
                "zh": group["zh"],
                "base_type": base_type,
                "base_zh": unique_base_zh(group["name"], base_type),
                "category": cats[0] if cats else "",
                "sub": sub_cat,
                "icon": group["icon"],
                "level_req": group["level_req"],
                "corrupted": group["corrupted"],
                "value": median if median is not None else main["value"],
                "value_min": min(values),
                "value_max": max(values),
                "median": median,
                "median_clean": weighted_median(clean_pool),
                "median_corrupt": weighted_median(dirty_pool),
                "listings_clean": sum(t["listing_count"] for t in clean_pool),
                "listings_corrupt": sum(t["listing_count"] for t in dirty_pool),
                "listing_count": group["listings"],
                "trend": main["trend"],
                "spark": main.get("spark") or [],
                "tier_count": len(tiers),
                "low_sample": not valid,
            }
        )

    # 下拉里给的是装备部位（头盔/护手/护甲/盾牌/武器/碑牌/珠宝…），不是 ninja 的粗类
    counts: dict[str, int] = {}
    for item in items:
        counts[item["sub"]] = counts.get(item["sub"], 0) + 1
    categories = [
        {"id": sid, "label": sid, "count": count}
        for sid, count in sorted(counts.items(), key=lambda kv: -kv[1])
        if sid
    ]
    items.sort(key=lambda item: -item["value"])
    return {
        "league": league,
        "ts": ts,
        "base": base,
        "min_listing": UNIQUE_MIN_LISTING,
        "categories": categories,
        "items": items,
        "pending": False,
    }


def unique_tiers(name: str, base: str = "divine") -> dict:
    """同一件暗金的全部 roll 档位，用于「是什么数值造成了差价」的档位对照。"""
    if base not in UNIQUE_VALUE_COLUMNS:
        base = "divine"
    league: str = STATE["league"]
    ts: int = unique_latest_ts(league)
    if not ts or not name:
        return {"name": name, "base": base, "tiers": []}
    column: str = UNIQUE_VALUE_COLUMNS[base]
    rows = db().execute(
        "SELECT item_key, base_type, category, corrupted,"
        f" {column} AS value, listing_count, trend, mods FROM unique_snapshot"
        " WHERE league = ? AND ts = ? AND name = ? ORDER BY listing_count DESC, value ASC",
        (league, ts, name),
    ).fetchall()

    tiers: list[dict] = []
    for row in rows:
        value = row["value"]
        if value is None or value != value:
            continue
        try:
            mods: dict = json.loads(row["mods"] or "{}")
        except (TypeError, ValueError):
            mods = {}
        def pack(texts: list[str], kind: str) -> list[dict]:
            packed: list[dict] = []
            for text in texts or []:
                lo, hi = parse_mod_range(text)
                zh = ZH.prop(text) if kind == "p" else ZH.mod(text)
                packed.append({"text": text, "zh": zh, "lo": lo, "hi": hi})
            return packed
        tiers.append(
            {
                "key": row["item_key"],
                "base_type": row["base_type"] or "",
                "base_zh": unique_base_zh(name, row["base_type"] or ""),
                "category": row["category"] or "",
                "corrupted": int(row["corrupted"] or 0),
                "value": float(value),
                "listing_count": int(row["listing_count"] or 0),
                "trend": float(row["trend"] or 0.0),
                "properties": pack(mods.get("p") or [], "p"),
                "explicit": pack(mods.get("e") or [], "e"),
            }
        )
    # 与榜单同口径：只统计挂单数达标的档位，再按挂单数加权取中位数
    pool = [t for t in tiers if t["listing_count"] >= UNIQUE_MIN_LISTING] or tiers
    clean_pool = [t for t in pool if not t["corrupted"]]
    dirty_pool = [t for t in pool if t["corrupted"]]
    return {
        "name": name,
        "zh": unique_zh(name),
        "base": base,
        "ts": ts,
        "tiers": tiers,
        "impact": unique_mod_impact(tiers),
        "median": weighted_median(pool),
        "median_clean": weighted_median(clean_pool),
        "median_corrupt": weighted_median(dirty_pool),
        "listings_clean": sum(t["listing_count"] for t in clean_pool),
        "listings_corrupt": sum(t["listing_count"] for t in dirty_pool),
        "value_min": min((t["value"] for t in tiers), default=None),
        "value_max": max((t["value"] for t in tiers), default=None),
        "min_listing": UNIQUE_MIN_LISTING,
    }


def mod_label(tmpl: str, kind: str) -> str:
    """词条名（不含具体数值）：把连续的数字占位折叠成一个 #。"""
    text = ZH.mod(tmpl) if kind == "e" else ZH.prop(tmpl)
    text = re.sub(r"#(?:-#)+", "#", text)
    return re.sub(r"#{2,}", "#", text).strip()


def trade_get(path: str, *, timeout: int = 25, limiter: "TradeLimiter | None" = None) -> dict:
    """官方交易接口 GET（拉挂单详情），与 POST 共用同一套限流。"""
    gate = limiter or TRADE_LIMITER
    url = f"{TRADE_BASE}{path}"
    last: Exception | None = None
    with _TRADE_GATE:
        for _attempt in range(SPREAD_RETRIES):
            gate.wait_turn()
            try:
                request = urllib.request.Request(
                    url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"}
                )
                with _HTTP_OPENER.open(request, timeout=timeout) as response:
                    data = json.loads(response.read().decode("utf-8", "ignore"))
                gate.note_ok()
                return data
            except urllib.error.HTTPError as exc:
                last = exc
                if exc.code == 429:
                    hold = _retry_after_seconds(exc)
                    gate.note_penalty(hold, f"官方限流 {hold:.0f}s")
                    raise TradeRateLimited(f"官方限流 {hold:.0f}s") from exc
                break
            except Exception as exc:  # noqa: BLE001
                last = exc
                gate.note_penalty(max(gate.gap, SPREAD_REQUEST_GAP) * 2, str(exc))
    raise RuntimeError(f"交易接口请求失败 {url} -> {last}")




def currency_value_map(league: str) -> dict[str, float]:
    """最新快照里每种通货值多少神圣石，用来把挂单报价统一折算。

    ⚠️ v1.27.8：必须按「每个通货各自最近一行」取，不能按 ts = MAX(ts) 整轮取——
    开了 skip_unchanged 之后整库最新时间戳只属于这一轮真的变了的那些通货，
    整轮取会漏掉其余 600 多种，挂单折算时它们就没有汇率可用。
    """
    rows = latest_snapshot_rows(league, ["s.currency_id", "s.value_divine"])
    out: dict[str, float] = {}
    for r in rows:
        v = r["value_divine"]
        if v is not None and v == v and float(v) > 0:
            out[r["currency_id"]] = float(v)
    return out




# ------------------------------------------------ 暗金集市价（污染 / 未鉴定）
# poe.ninja 的暗金快照几乎不标污染（实测最新快照只有个位数污染行，且没有对照），
# 想要「污染价」「未鉴定价」只能查官方集市。官方限流很凶（5 秒间隔照样会 429），所以：
#   · 走全局限流器（间隔 8 秒），与其它功能共用一个出口，绝不并发
#   · 结果入库缓存 6 小时，短期内不会重复查同一件
#   · 一次查询要发 3 次 search + 若干次 fetch，耗时几十秒，只能在后台跑
UNIQUE_MARKET_TTL = _clamp_int(CONFIG.get("unique_market_ttl_hours", 6), 6, 1, 72) * 3600
# 每种口径取回多少条挂单。样本太小，中位数只代表最便宜的那一批；
# 取 20 条（2 次 fetch）能把「半边样本」的问题压下去，代价是每件多一次请求。
UNIQUE_MARKET_SAMPLE = _clamp_int(CONFIG.get("unique_market_sample", 20), 20, 5, 100)
# 集市查询的专属节拍器：跟差值扫描的 8 秒节奏分开，互不拖累。
# 实测 2.5s 间隔连续打 9 次不会触发限流；真撞上 429 会按官方给的秒数罚等。
UNIQUE_MARKET_GAP = float(CONFIG.get("unique_market_gap", 2.5) or 2.5)
UNIQUE_MARKET_COOLDOWN = _clamp_int(CONFIG.get("unique_market_cooldown", 300), 300, 30, 3600)
MARKET_VARIANTS = ("clean", "corrupt", "unidentified")
MARKET_JOBS: dict[str, dict] = {}
MARKET_LOCK = threading.Lock()
MARKET_LIMITER = TradeLimiter(UNIQUE_MARKET_GAP)


def _market_payload(name: str, base_type: str, variant: str) -> dict | None:
    """构造官方集市的查询体。

    POE2 的坑（逐个实测过）：
    · name 必须写成 {"discriminator": "unique", "option": ...}，写字符串查不到
    · type 反过来只能写字符串，写对象会报 Unknown discriminator
    · 未鉴定的物品在索引里没有暗金名，只能按底材(type)找
    """
    query: dict = {"status": {"option": "securable"}}
    # ⚠️ 未鉴定也必须按暗金名查，不能按底材查（这是踩过的大坑）。
    # 未鉴定的货在 API 返回里 name 是空、typeLine 只有底材，
    # 看着像"索引里没有暗金名"，于是早期版本改成按底材 + rarity=unique 查——
    # 结果全错：Temporalis 的未鉴定价算出 0.016 神圣石（真值 475 量级），
    # 因为 Silk Robe 底材下未鉴定的 36 条其实全是另一件暗金 Cloak of Flame。
    # 实测官方索引内部是认得未鉴定件属于哪件暗金的：
    #   Mageblood  → 25 条（底材 Utility Belt）
    #   Headhunter → 56 条（底材 Heavy Belt）
    #   Cloak of Flame → 37 条（底材 Silk Robe）
    #   Temporalis → 0 条（确实没人挂未鉴定的，不是查不到）
    # 所以这里三种口径统一走 name(unique)，只切换 identified / corrupted。
    query["name"] = {"discriminator": "unique", "option": name}
    filters: dict = {
        "misc_filters": {
            "filters": {"identified": {"option": "false" if variant == "unidentified" else "true"}}
        },
        "type_filters": {"filters": {"rarity": {"option": "unique"}}},
    }
    if variant in ("clean", "corrupt"):
        # 污染 / 未污染走 misc_filters 的 corrupted 开关，不能只靠名称：
        # 同一个暗金名下面污染和没污染是两拨货，价差能到好几倍。
        # 实测 Temporalis 名称查询 64 条 = 未污染 33 + 已污染 31，二分完整闭合。
        filters["misc_filters"]["filters"]["corrupted"] = {
            "option": "true" if variant == "corrupt" else "false"
        }
    query["filters"] = filters
    return {"query": query, "sort": {"price": "asc"}}


def market_match_mode(variant: str) -> str:
    """三种口径现在统一按暗金名精确匹配（未鉴定也能按名查，见 _market_payload 注释）。"""
    return "by_name"


def market_caveat(variant: str) -> str:
    """这一口径的失真说明，为空表示没有已知偏差。

    未鉴定件在 API 返回里 name 是空的，所以界面上看不出它到底是哪件暗金——
    这点要如实说明，免得用户以为取回来的价跟展示的物件对不上。
    匹配本身是精确的（官方索引认得未鉴定件归属哪件暗金），不是估算。
    """
    if variant != "unidentified":
        return ""
    return "官方不返回未鉴定件的名称，这里按暗金名精确匹配；展示时只能看到底材"


def _market_query_once(
    league: str, name: str, base_type: str, variant: str, values: dict[str, float]
) -> dict:
    """查一次官方集市，取该口径下的价格中位数。"""
    payload = _market_payload(name, base_type, variant)
    if not payload:
        return {
            "ok": False, "reason": "缺少暗金名", "median": None, "count": 0, "total": 0,
            "match_mode": "by_name", "warning": "",
        }
    # 走集市专属节拍器，别被差值扫描那套 8 秒节奏拖着
    data = trade_post(
        f"/search/poe2/{urllib.parse.quote(league)}", payload, limiter=MARKET_LIMITER
    )
    total = int(data.get("total") or 0)
    hashes = (data.get("result") or [])[:UNIQUE_MARKET_SAMPLE]
    query_id = str(data.get("id") or "")
    prices: list[float] = []
    if hashes and query_id:
        for i in range(0, len(hashes), 10):
            chunk = hashes[i:i + 10]
            got = trade_get(
                f"/fetch/{','.join(chunk)}?query={urllib.parse.quote(query_id)}",
                limiter=MARKET_LIMITER,
            )
            for entry in got.get("result") or []:
                price = (entry.get("listing") or {}).get("price") or {}
                amount = float(price.get("amount") or 0)
                per = values.get(str(price.get("currency") or ""))
                if per and amount > 0:
                    prices.append(amount * per)  # 统一折算成当前计价货币
    prices.sort()
    return {
        "ok": bool(prices),
        "median": statistics.median(prices) if prices else None,
        "lo": prices[0] if prices else None,
        "hi": prices[-1] if prices else None,
        "count": len(prices),
        "total": total,
        # 挂单数比取回的多时，中位数只代表最便宜的那一批，前端要如实说明
        "partial": len(hashes) < total,
        "match_mode": market_match_mode(variant),
        "warning": market_caveat(variant),
    }


def _market_read(league: str, name: str, variant: str) -> dict | None:
    row = db().execute(
        "SELECT median, lo, hi, count, total, partial, ts FROM unique_market"
        " WHERE league = ? AND name = ? AND variant = ?",
        (league, name, variant),
    ).fetchone()
    return dict(row) if row else None


def _market_write(league: str, name: str, variant: str, result: dict) -> None:
    db().execute(
        "INSERT INTO unique_market (league, name, variant, median, lo, hi, count, total, partial, ts)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        " ON CONFLICT (league, name, variant) DO UPDATE SET"
        " median = excluded.median, lo = excluded.lo, hi = excluded.hi,"
        " count = excluded.count, total = excluded.total, partial = excluded.partial,"
        " ts = excluded.ts",
        (
            league, name, variant,
            result.get("median"), result.get("lo"), result.get("hi"),
            int(result.get("count") or 0), int(result.get("total") or 0),
            1 if result.get("partial") else 0, int(time.time()),
        ),
    )
    db().commit()


def _market_run(key: str, league: str, name: str, base_type: str) -> None:
    """后台任务：把三种口径都查一遍并入库。"""
    try:
        values = currency_value_map(league)
        for variant in MARKET_VARIANTS:
            result = _market_query_once(league, name, base_type, variant, values)
            _market_write(league, name, variant, result)
        with MARKET_LOCK:
            MARKET_JOBS[key] = {"running": False, "ts": time.time(), "ok": True, "error": ""}
    except TradeRateLimited as exc:
        # 限流后老实等，绝不立刻重启线程——否则会变成打不停的请求风暴
        with MARKET_LOCK:
            MARKET_JOBS[key] = {
                "running": False, "ts": time.time(), "ok": False,
                "error": str(exc), "cooldown": UNIQUE_MARKET_COOLDOWN,
            }
        log(f"  集市查询被限流 {name}: {exc}")
    except Exception as exc:  # noqa: BLE001
        with MARKET_LOCK:
            MARKET_JOBS[key] = {
                "running": False, "ts": time.time(), "ok": False,
                "error": str(exc), "cooldown": 300,
            }
        log(f"  集市查询失败 {name}: {exc}")


def market_factor(base: str) -> float:
    """把神圣石计价的价格换算成当前计价货币的倍数。

    库里统一存神圣石，切换计价货币时直接换算，不用重新去查集市。
    """
    if base not in UNIQUE_VALUE_COLUMNS:
        base = "divine"
    if base == "divine":
        return 1.0
    values = currency_value_map(STATE["league"])
    per = values.get(base)  # 1 个该货币值多少神圣石
    return (1.0 / per) if per and per > 0 else 1.0


def _apply_factor(variants: dict[str, dict], factor: float) -> dict[str, dict]:
    if factor == 1.0:
        return variants
    for row in variants.values():
        for key in ("median", "lo", "hi"):
            if row.get(key) is not None:
                row[key] = float(row[key]) * factor
    return variants


def unique_market(name: str, base_type: str, base: str = "divine", refresh: bool = False) -> dict:
    """暗金在官方集市上的价格中位数（未污染 / 污染 / 未鉴定）。

    三种口径各查一次太慢（几十秒），所以第一次点开会先返回「查询中」，
    结果入库后 6 小时内直接读缓存。
    """
    league: str = STATE["league"]
    now: int = int(time.time())
    out: dict = {
        "name": name,
        "zh": unique_zh(name),
        "base_type": base_type,
        "variants": {},
        "ts": 0,
        "pending": False,
        "error": "",
        "cooldown": 0,
        # 查询节奏的信息，前端拿来说明「为什么要点一下等一会儿」
        "sample": UNIQUE_MARKET_SAMPLE,
        "gap": round(UNIQUE_MARKET_GAP, 2),
        "ttl_hours": UNIQUE_MARKET_TTL // 3600,
        "limiter": MARKET_LIMITER.snapshot(),
    }
    if not name:
        return out

    cached: dict[str, dict] = {}
    missing: list[str] = []
    for variant in MARKET_VARIANTS:
        row = _market_read(league, name, variant)
        fresh = row and (now - int(row.get("ts") or 0) < UNIQUE_MARKET_TTL)
        if row and fresh and not refresh:
            cached[variant] = {
                **row,
                "match_mode": market_match_mode(variant),
                "warning": market_caveat(variant),
            }
        else:
            missing.append(variant)
    if cached:
        out["variants"] = _apply_factor(cached, market_factor(base))
        out["ts"] = max(int(r.get("ts") or 0) for r in cached.values())
    if not missing:
        return out

    key: str = f"{league}|{name}|{base_type}"
    with MARKET_LOCK:
        job = MARKET_JOBS.get(key)
        if job and job.get("running"):
            out["pending"] = True
            return out
        if job and not job.get("ok"):
            waited = time.time() - float(job.get("ts") or 0)
            cooldown = float(job.get("cooldown") or UNIQUE_MARKET_COOLDOWN)
            if waited < cooldown:
                out["error"] = job.get("error") or "官方接口暂时不可用"
                out["cooldown"] = int(cooldown - waited)
                return out
        MARKET_JOBS[key] = {"running": True, "ts": time.time(), "ok": False, "error": ""}
    threading.Thread(
        target=_market_run, args=(key, league, name, base_type), daemon=True
    ).start()
    out["pending"] = True
    return out


def unique_history(item_key: str, hours: int) -> dict:
    """某个档位的价格历史，供详情曲线使用。"""
    cutoff = int(time.time()) - hours * 3600
    rows = db().execute(
        "SELECT ts, name, value_exalted, value_chaos, value_divine, listing_count"
        " FROM unique_snapshot WHERE league = ? AND item_key = ? AND ts >= ?"
        " ORDER BY ts ASC",
        (STATE["league"], item_key, cutoff),
    ).fetchall()

    def series(column: str) -> list[list[float]]:
        out: list[list[float]] = []
        for row in rows:
            value = row[column]
            if value is None or value != value:
                continue
            out.append([row["ts"], float(value)])
        return out

    name = rows[0]["name"] if rows else item_key
    return {
        "key": item_key,
        "name": name,
        "zh": unique_zh(name),
        "points": {
            "exalted": series("value_exalted"),
            "chaos": series("value_chaos"),
            "divine": series("value_divine"),
        },
        "samples": len(rows),
    }


# ------------------------------------------------------------ 倒货榜推荐排序
# 旧版 recommend 是在「当前筛选出来的这批」里做百分位排名：换个筛选条件，同一件通货
# 分数就变，横向没法比，也根本没把没人交易的冷门货剔掉。现在改成绝对分：
#
#   1) 热度 heat ∈ [0,1] —— 能不能真买到、能不能真卖掉
#        depth    挂出量：市场上挂了多少货，决定能不能真买到
#        bulk     求购量：有多少通货挂着收它，决定能不能真卖掉
#        turnover 成交热度：窗口内的累计成交量
#      权重 0.45 / 0.30 / 0.25：挂出量最能反映「有没有人在交易」，排第一。
#      这三项都跨好几个数量级，先取对数再按封顶值归一到 [0,1]。
#      ⚠️ 挂出量/求购量为 0 是「数据源没给」，不是「真的没有」——用成交量兜底，别直接判 0 分。
#
#   2) 机会 chance ∈ [0,1] —— 现在进场能赚多少
#        upsideN  距窗口最高价还有多少上行空间（15% 封顶）
#        cheapN   现价低于区间均价的程度（低 10% 封顶）
#        entryN   （现价在窗口区间里的位置，越低越好）× swingN
#        swingN   区间振幅够不够做波段（25% 封顶）
#      权重 0.30 / 0.25 / 0.25 / 0.20。
#      为什么单加一个 entryN：cheapN 拿均价当锚，窗口里只要出现一根极端高价
#      就会把均价拉偏、把「其实很便宜」的货误判成不便宜；entryN 只认最低/最高价，
#      不受单根极值影响，两者互补比只看一个稳。
#      entryN 必须乘 swingN：一条几乎不动的曲线「贴着最低价」毫无意义。
#
#   3) 置信度 confidence ∈ [0,1] = samples / ARB_CONF_SAMPLES（封顶 1）
#      窗口里只有两三个采样点时算出来的振幅很可能是噪声，必须打折，
#      否则刚装好、历史还没攒够的那批货会靠假波动霸榜。
#
#   4) 推荐分 score = chance × (0.25 + 0.75 × heat) × confidence × 100
#      热度当权重而不是一票否决：热度满分权重 1.0，热度 0 也留 0.25 的底权，
#      于是「机会特别好但热度一般」的通货不会彻底消失，只是被热度高的挤到后面。
#
#   4) 冷漠判定 cold：没人交易的通货，报价再漂亮也成交不了
#        · 挂出量已知 且 挂出量 < ARB_MIN_ORDERS（买不到）
#        · 求购量已知 且 求购量 < ARB_MIN_STOCK（卖不掉）
#        · 流动性已知 且 热度 < ARB_HEAT_MIN
#      ⚠️ 三条都要求「已知」才判冷漠。全未知就放行——否则升级后、下一次抓取落地之前
#         整张榜单会空掉（踩过这个坑）。
ARB_HEAT_MIN = 0.20
ARB_UPSIDE_CAP = 15.0
ARB_CHEAP_CAP = 10.0
ARB_SWING_CAP = 25.0
ARB_DEPTH_CAP = 2000.0
ARB_BULK_CAP = 50000.0
ARB_TURNOVER_CAP = 2_000_000.0


def _arb_norm(value: float, cap: float) -> float:
    """把跨数量级的量对数压到 [0,1]；0 / 负数一律算 0。"""
    if not value or value <= 0 or cap <= 1:
        return 0.0
    return min(1.0, math.log10(1.0 + value) / math.log10(1.0 + cap))


def arb_liquidity(orders: float, stock: float, volume: float) -> dict:
    """流动性热度。返回 {heat, depth, bulk, turnover, known, cold, reason}。

    orders = 挂出量（该通货自己挂出去多少＝能买到多少），
    stock  = 求购量（对手挂了多少通货在收它＝能卖掉多少）。
    两个方向都要看：买得到但卖不掉、卖得掉但买不到，都做不成倒货。
    """
    turnover = _arb_norm(volume, ARB_TURNOVER_CAP)
    # 某一侧缺失时用成交量兜底，避免把「没数据」误判成「没人交易」
    depth = _arb_norm(orders, ARB_DEPTH_CAP) if orders > 0 else turnover
    bulk = _arb_norm(stock, ARB_BULK_CAP) if stock > 0 else turnover
    heat = 0.45 * depth + 0.30 * bulk + 0.25 * turnover

    known = bool(orders > 0 or stock > 0 or volume > 0)
    reason = ""
    if known:
        if orders > 0 and orders < ARB_MIN_ORDERS:
            # 整个交易所上就挂了这么点，买都买不到
            reason = f"挂出量仅 {orders:.0f}（< {ARB_MIN_ORDERS}）"
        elif stock > 0 and stock < ARB_MIN_STOCK and orders < ARB_ACTIVE_ORDERS:
            # 没人收（求购少）、市场上货也不多，才算真冷清。
            # 挂出量够大的（>= ARB_ACTIVE_ORDERS）说明交易频繁，不该判冷漠。
            reason = (f"求购量 {stock:.0f}（< {ARB_MIN_STOCK}）"
                      f"且挂出量 {orders:.0f}（< {ARB_ACTIVE_ORDERS}）")
        elif heat < ARB_HEAT_MIN:
            reason = f"流动性热度 {heat:.2f}（< {ARB_HEAT_MIN}）"
    return {
        "heat": heat, "depth": depth, "bulk": bulk, "turnover": turnover,
        "known": known, "cold": bool(reason), "reason": reason,
    }


def arb_score(
    upside: float | None,
    dev: float | None,
    swing: float | None,
    heat: float,
    position: float | None = None,
    samples: int = 0,
) -> float:
    """推荐分 0~100 = 机会分 × 热度权重 × 样本置信度。

    · upside   距窗口最高价还能涨多少（%）
    · dev      现价相对窗口均价的偏离（%），负值＝比均价便宜
    · swing    窗口振幅（%）
    · position 现价在 [最低价, 最高价] 区间里的位置，0=贴最低、1=贴最高
    · samples  窗口内的采样点数，太少时整分打折
    """
    upside_n = _arb_norm(upside, ARB_UPSIDE_CAP)
    cheap_n = _arb_norm(-dev, ARB_CHEAP_CAP) if dev is not None else 0.0
    swing_n = _arb_norm(swing, ARB_SWING_CAP)
    # 位置分 = 「离最低价多近」×「振幅够不够」。
    # 乘 swing_n 是必须的：一条几乎没波动的曲线，「贴着最低价」毫无意义
    # （涨不上去），不乘的话平价货会白拿 0.25 的位置分混进榜单中段。
    entry_n = (1.0 - min(max(position, 0.0), 1.0)) * swing_n if position is not None else 0.0
    chance = 0.30 * upside_n + 0.25 * cheap_n + 0.25 * entry_n + 0.20 * swing_n
    confidence = min(1.0, max(samples, 0) / float(ARB_CONF_SAMPLES))
    return chance * (0.25 + 0.75 * heat) * confidence * 100.0


def arbitrage_rows(base: str, category: str, query: str, hours: int) -> dict:
    """倒货盈利榜：基于本机每小时历史，算出每个通货的低买高卖空间。

    注意：汇率源（dadsofexile / poe2scout / poe.ninja）每种通货只有一个聚合价
    （无买/卖价差），所以「同一时刻三种货币转一圈」恒等于 0% 盈亏；
    真正能赚的是时间维度上的低买高卖，这里用窗口内 最低价→最高价 的空间
    与当前价相对均线的位置来量化。
    """
    column = BASE_COLUMNS[base]
    latest_ts_row = db().execute(
        "SELECT MAX(ts) AS ts FROM snapshot WHERE league = ?", (STATE["league"],)
    ).fetchone()
    if not latest_ts_row or latest_ts_row["ts"] is None:
        return {"meta": build_meta(base, 0, 0), "items": []}

    latest_ts = int(latest_ts_row["ts"])
    cutoff = latest_ts - hours * 3600

    sql = f"""
    WITH ranked AS (
        SELECT currency_id, ts, {column} AS v,
               ROW_NUMBER() OVER (PARTITION BY currency_id ORDER BY ts ASC)  AS rn_first,
               ROW_NUMBER() OVER (PARTITION BY currency_id ORDER BY ts DESC) AS rn_last
        FROM snapshot
        WHERE league = ? AND ts >= ? AND {column} IS NOT NULL
          -- ⚠️ 必须排除 synthetic：那是 poe.ninja 7 天 sparkline 反推出来的**日线**点
          -- （一天一个）。混进来会让「现在 vs 昨天」冒充日内振幅，离线后真实点一少，
          -- 它还会拿来凑满 n=2 硬上榜，算出的波动完全不是日内波动。
          -- cloud 是云端真实抓取的，正常计入。
          AND (source IS NULL OR source != 'synthetic')
    )
    SELECT currency_id,
           MIN(v) AS lo, MAX(v) AS hi, AVG(v) AS mean, COUNT(*) AS n,
           MAX(CASE WHEN rn_first = 1 THEN v END) AS first_v,
           MAX(CASE WHEN rn_last  = 1 THEN v END) AS last_v,
           MAX(CASE WHEN rn_first = 1 THEN ts END) AS first_ts
    FROM ranked GROUP BY currency_id
    """
    stats = {
        row["currency_id"]: row
        for row in db().execute(sql, [STATE["league"], cutoff]).fetchall()
    }

    latest_rows = latest_snapshot_rows(
        STATE["league"],
        ["s.currency_id", "s.category", "s.value_divine", "s.value_chaos",
         "s.value_exalted", "s.volume",
         "COALESCE(s.stock, 0) AS stock", "COALESCE(s.orders, 0) AS orders"],
    )
    meta_rows = {
        row["currency_id"]: row
        for row in db().execute("SELECT * FROM item_meta").fetchall()
    }

    labels = dict(CATEGORIES)
    items: list[dict] = []
    hidden_small = 0
    hidden_low_stock = 0
    no_zh = 0
    for row in latest_rows:
        cid = row["currency_id"]
        stat = stats.get(cid)
        if not stat:
            continue  # 窗口内一个采样点都没有，无从谈起
        # ★ n==1 不再跳过（v1.27.8）：开了 skip_unchanged 后，价格纹丝不动的
        #   通货在窗口里就只有一行——那恰恰说明它波动为 0，应该以 0% 空间上榜，
        #   而不是从倒货榜上消失。新入库、真的没历史的通货同样只有 1 点，
        #   但 confidence（samples/8）会把它们压到低分，语义一致。
        if category and category != "all" and row["category"] != category:
            continue

        meta = meta_rows.get(cid)
        name_en = meta["name_en"] if meta and meta["name_en"] else cid
        name_zh = zh_display_name(cid, meta)
        icon = meta["icon"] if meta and meta["icon"] else ""
        if not matches_query(query, cid, name_en, name_zh):
            continue
        if not name_zh:
            no_zh += 1
            continue

        values = {
            "exalted": row["value_exalted"],
            "chaos": row["value_chaos"],
            "divine": row["value_divine"],
        }
        current = values.get(base)
        if current is None or current != current:
            continue
        # 现价小于两位小数（0.01）的通货不显示：这种价位算出来的百分比没有参考意义
        if current < ARB_MIN_VALUE:
            hidden_small += 1
            continue

        lo, hi, mean = float(stat["lo"]), float(stat["hi"]), float(stat["mean"])
        first_v, last_v = float(stat["first_v"]), float(stat["last_v"])
        swing = ((hi - lo) / lo * 100.0) if lo else None
        trend = ((last_v - first_v) / first_v * 100.0) if first_v else None
        # 当前价相对窗口均价的位置：负值说明现在比均价便宜，适合买入
        dev = ((current - mean) / mean * 100.0) if mean else None
        # 距离窗口最低价还能涨多少：正值说明仍有上行空间
        upside = ((hi - current) / current * 100.0) if current else None
        downside = ((current - lo) / lo * 100.0) if lo else None
        # 现价在窗口区间里的位置：0 = 正贴着最低价，1 = 正贴着最高价。
        # 振幅为 0（整窗口一个价）时给 0.5，表示「无从判断」而不是「最便宜」。
        position = ((current - lo) / (hi - lo)) if hi > lo else 0.5
        samples_n = int(stat["n"])

        stock_v = float(row["stock"] or 0.0)
        orders_v = int(row["orders"] or 0)
        volume_v = float(row["volume"] or 0.0)
        liquidity = arb_liquidity(orders_v, stock_v, volume_v)
        score = arb_score(
            upside, dev, swing, liquidity["heat"],
            position=position, samples=samples_n,
        )

        items.append(
            {
                "id": cid,
                "category": row["category"],
                "category_label": labels.get(row["category"], row["category"]),
                "name": name_en,
                "name_zh": name_zh,
                "icon": f"/icon?id={urllib.parse.quote(cid)}" if icon else "",
                "value": current,
                "values": values,
                "volume": volume_v,
                # 挂出量 / 求购量只有 dadsofexile 给；老快照没有这两列，COALESCE 兜成 0。
                # 0 一律表示「数据源没给」，不能读成「真的没有」。
                "stock": stock_v,
                "orders": orders_v,
                # 流动性热度与其三个分项（0~1），前端拿来做推荐条与 tooltip
                "heat": liquidity["heat"],
                "depth": liquidity["depth"],
                "bulk": liquidity["bulk"],
                "turnover": liquidity["turnover"],
                "liquidity_known": liquidity["known"],
                # 冷漠通货：没人交易，报价再漂亮也成交不了
                "cold": liquidity["cold"],
                "cold_reason": liquidity["reason"],
                "score": score,
                # 兼容旧前端：eligible = 不冷漠
                "eligible": not liquidity["cold"],
                "lo": lo,
                "hi": hi,
                "mean": mean,
                "swing": swing,
                "trend": trend,
                "dev": dev,
                "upside": upside,
                "downside": downside,
                # 现价在窗口区间里的位置（0=贴最低、1=贴最高），前端画区间条用
                "position": position,
                # 采样点不足时推荐分被打折，记下来好让前端如实说明
                "confidence": min(1.0, samples_n / float(ARB_CONF_SAMPLES)),
                "samples": samples_n,
                "window_hours": hours,
            }
        )
    # 排序：先按推荐分降序，冷漠的一律沉底（沉底而非删掉，见 ARB_HIDE_COLD）。
    # 后端排好序，前端换排序字段时也有一致的兜底顺序，不会每次刷新乱跳。
    items.sort(key=lambda item: (item["cold"], -item["score"], item["id"]))
    for index, item in enumerate(items, 1):
        item["rank"] = index

    # 「剔出冷漠通货」的配额：最多只能剔掉榜单总数的 ARB_MAX_COLD_RATIO。
    # 前端照这个数从榜单末尾（最差的那些）开始剔，保证任何时候都还剩 60%。
    total_items = len(items)
    max_hidden_cold = int(total_items * ARB_MAX_COLD_RATIO)
    # 只统计「确实知道流动性、且判定为冷漠」的，全未知的不算被隐藏
    hidden_cold = sum(1 for item in items if item["cold"])
    hidden_low_stock = sum(
        1
        for item in items
        if item["cold"] and float(item.get("stock") or 0.0) > 0.0
        and float(item.get("stock") or 0.0) < ARB_MIN_STOCK
    )
    hidden_low_orders = sum(
        1
        for item in items
        if item["cold"] and int(item.get("orders") or 0) > 0
        and int(item.get("orders") or 0) < ARB_MIN_ORDERS
    )
    meta = build_meta(base, latest_ts, sum(1 for i in items if not i["cold"]))
    meta["no_zh"] = no_zh
    meta["arb"] = {
        "min_value": ARB_MIN_VALUE,
        "hidden_small": hidden_small,
        "min_stock": ARB_MIN_STOCK,
        "min_orders": ARB_MIN_ORDERS,
        "active_orders": ARB_ACTIVE_ORDERS,
        "hide_cold": ARB_HIDE_COLD,
        "heat_min": ARB_HEAT_MIN,
        "hidden_cold": hidden_cold,
        "hidden_low_stock": hidden_low_stock,
        "hidden_low_orders": hidden_low_orders,
        # 冷漠剔除的上限：勾了「剔出冷漠通货」也最多剔这么多
        "max_hidden_cold": max_hidden_cold,
        "max_cold_ratio": ARB_MAX_COLD_RATIO,
        "conf_samples": ARB_CONF_SAMPLES,
    }
    return {"meta": meta, "items": items}


PLACEHOLDER_PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d494844520000002000000020080600000073"
    "7a7af40000001a49444154789cedc1010d000000c2a0f74f6d0e37a00000"
    "0000000000be0d2e0000019ea8080000000049454e44ae426082"
)


def icon_missing_ids(limit: int = 0) -> list[str]:
    """还没缓存到本地的图标 id（联网时用来预热，让断网也能看到图标）。"""
    rows = db().execute(
        "SELECT currency_id, icon FROM item_meta WHERE icon IS NOT NULL AND icon != ''"
    ).fetchall()
    missing = [r["currency_id"] for r in rows if not (ICON_DIR / f"{r['currency_id']}.png").exists()]
    return missing[:limit] if limit else missing


def download_icon(currency_id: str) -> bool:
    """把单个图标拉到本地缓存；失败（离线/被限流）返回 False。"""
    row = db().execute(
        "SELECT icon FROM item_meta WHERE currency_id = ?", (currency_id,)
    ).fetchone()
    if not row or not row["icon"]:
        return False
    for host in IMAGE_HOSTS:
        try:
            # 超时压到 8 秒：真断网时这只是个图标，不该让人干等
            payload = http_get(host + row["icon"], timeout=8, retries=1)
            if payload.startswith(b"\x89PNG"):
                (ICON_DIR / f"{currency_id}.png").write_bytes(payload)
                return True
        except Exception:  # noqa: BLE001 - 换下一个镜像
            continue
    return False


def warm_icons(limit: int = 40) -> int:
    """每轮补几个图标，慢慢把图标库搬到本地。

    只在确认联网时调用：断网时下载会逐个卡满超时，反而拖慢界面。
    """
    done = 0
    for cid in icon_missing_ids(limit):
        if not download_icon(cid):
            break  # 拉不动就停，多半是断网或被限流，下一轮再试
        done += 1
        time.sleep(0.2)
    return done


def serve_icon(currency_id: str) -> tuple[int, bytes]:
    cached = ICON_DIR / f"{currency_id}.png"
    if cached.exists():
        return 200, cached.read_bytes()
    row = db().execute(
        "SELECT icon FROM item_meta WHERE currency_id = ?", (currency_id,)
    ).fetchone()
    if not row or not row["icon"]:
        return 200, PLACEHOLDER_PNG
    with STATE_LOCK:
        online = bool(STATE.get("online"))
    if not online:
        # 离线就别去碰网络了：每个图标都要等满超时，会把页面拖垮
        return 200, PLACEHOLDER_PNG
    try:
        payload = None
        for host in IMAGE_HOSTS:
            try:
                payload = http_get(host + row["icon"], timeout=8, retries=1)
                if payload.startswith(b"\x89PNG"):
                    break
                payload = None
            except Exception:  # noqa: BLE001 - 换下一个镜像
                continue
        if payload is None:
            return 200, PLACEHOLDER_PNG
    except Exception:  # noqa: BLE001
        return 200, PLACEHOLDER_PNG
    cached.write_bytes(payload)
    return 200, payload


# ----------------------------------------------------------------- HTTP 服务

class Handler(BaseHTTPRequestHandler):
    server_version = APP_NAME
    protocol_version = "HTTP/1.1"

    # 界面上只有「切换取价源」这一个写操作，所以 POST 就只认 /api/source。
    # 其余路径一律 405，别让 do_POST 变成第二个入口——
    # 两套路由迟早漂成两份不一致的行为。
    def do_POST(self) -> None:  # noqa: N802 - HTTP 约定
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != "/api/source":
            self._json({"error": "该接口不支持 POST"}, status=405)
            return
        try:
            body = self._read_json_body()
            value = str((body or {}).get("source") or "").strip().lower()
            self._json({"ok": True, "source": set_primary_source(value)})
        except Exception as exc:                                # noqa: BLE001
            log(f"接口异常 {parsed.path}: {exc}")
            self._json({"error": str(exc)}, status=500)

    def do_GET(self) -> None:  # noqa: N802 - HTTP 约定
        parsed = urllib.parse.urlparse(self.path)
        path, args = parsed.path, urllib.parse.parse_qs(parsed.query)

        try:
            if path == "/api/meta":
                payload: dict = {
                    "meta": build_meta("exalted", 0, tracked_item_count(STATE["league"])),
                    "items": [],
                    # 挂出量/求购量是 poe2scout 独占的源，它停更时这两列会整片变「—」。
                    # 把停更状态带出去，前端才知道该提示「上游停更」而不是「程序坏了」。
                    "qty": scout_qty_status(STATE["league"]),
                    # 当前取价源是谁、它的价多久没动过（供顶部横幅提示）
                    "price": price_status(STATE["league"]),
                    # 三个源各自的请求间隔（秒）——它们是分开的，前端别当成一个值展示
                    "sources": source_intervals(),
                }
                self._json(payload)
            elif path == "/api/current":
                base = args.get("base", ["exalted"])[0]
                if base not in BASE_COLUMNS:
                    base = "exalted"
                # 上限必须跟着 retention_days 走：超出保留期的区间根本没有数据，
                # 放它过去只会让人以为能查 30 天。原来写死 24*30，和 3 天保留期对不上。
                hours = _safe_int(
                    args.get("hours", ["24"])[0], 24, 1, 24 * RETENTION_DAYS
                )
                self._json(
                    rows_to_items(
                        base, args.get("category", ["all"])[0], args.get("q", [""])[0], hours
                    )
                )
            elif path.startswith("/api/spread"):
                # 差价榜板块已移除，接口保留但默认停用：
                # 开启方式见 config.json 的 spread_enabled。
                if not SPREAD_ENABLED:
                    self._json(
                        {
                            "disabled": True,
                            "reason": "买卖差价榜已停用（官方交易接口限流过严）。"
                            "如需恢复，把 config.json 的 spread_enabled 改成 true 并重启。",
                            "items": [],
                        }
                    )
                elif path == "/api/spread/history":
                    ref = args.get("ref", ["chaos"])[0]
                    hours = _safe_int(args.get("hours", ["72"])[0], 72, 1, 24 * RETENTION_DAYS)
                    self._json(spread_history(args.get("id", [""])[0], ref, hours))
                elif path == "/api/spread/rescan":
                    self._json(SPREAD_JOB.start_scan(args.get("limit", ["30"])[0]))
                elif path == "/api/spread/status":
                    self._json(SPREAD_JOB.status())
                else:
                    ref = args.get("ref", ["chaos"])[0]
                    if ref not in ("chaos", "divine", "exalted"):
                        ref = "chaos"
                    hours = _safe_int(args.get("hours", ["24"])[0], 24, 1, 24 * RETENTION_DAYS)
                    self._json(spread_rows(ref, hours, args.get("q", [""])[0]))
            elif path == "/api/calc":
                # 换汇计算器：只给聚合汇率与通货清单，买卖报价由用户在页面上手工录入
                self._json(calc_payload())
            elif path == "/api/history":
                hours = _safe_int(args.get("hours", ["72"])[0], 72, 1, 24 * RETENTION_DAYS)
                self._json(query_history(
                    args.get("id", [""])[0], hours,
                    args.get("src", ["db"])[0],
                ))
            elif path.startswith("/api/uniques"):
                if path == "/api/uniques/history":
                    hours = _safe_int(
                        args.get("hours", ["72"])[0], 72, 1, 24 * UNIQUE_RETENTION_DAYS
                    )
                    self._json(unique_history(args.get("key", [""])[0], hours))
                elif path == "/api/uniques/tiers":
                    self._json(
                        unique_tiers(
                            args.get("name", [""])[0], args.get("base", ["divine"])[0]
                        )
                    )
                elif path == "/api/uniques/market":
                    # 官方集市的污染 / 未污染 / 未鉴定 价格中位数，点开某一件才查
                    refresh = str(args.get("refresh", [""])[0]).lower() in ("1", "true", "yes")
                    self._json(
                        unique_market(
                            args.get("name", [""])[0],
                            args.get("base_type", [""])[0],
                            args.get("base", ["divine"])[0],
                            refresh=refresh,
                        )
                    )
                else:
                    self._json(
                        unique_rows(
                            args.get("base", ["divine"])[0],
                            args.get("category", ["all"])[0],
                            args.get("q", [""])[0],
                        )
                    )
            elif path == "/api/arbitrage":
                base = args.get("base", ["exalted"])[0]
                if base not in BASE_COLUMNS:
                    base = "exalted"
                hours = _safe_int(args.get("hours", ["24"])[0], 24, 1, 24 * RETENTION_DAYS)
                self._json(
                    arbitrage_rows(
                        base, args.get("category", ["all"])[0], args.get("q", [""])[0], hours
                    )
                )
            elif path.startswith("/api/switch"):
                worker = getattr(self.server, "worker", None)
                league = args.get("league", [""])[0]
                allowed = STATE["leagues"] or [STATE["league"]]
                if worker and league in allowed:
                    worker.request_switch(league)
                    spread_worker = getattr(self.server, "spread_worker", None)
                    if spread_worker:
                        spread_worker.request_switch()
                    log(f"收到联盟切换请求：{league}")
                    self._json({"ok": True, "league": league})
                else:
                    self._json({"ok": False, "league": STATE["league"]}, status=400)
            elif path == "/api/source":
                # 取价主源。切换走 POST（见 do_POST），这里只负责告诉前端有哪些可选。
                self._json({
                    "source": primary_source(),
                    "default": PRIMARY_SOURCE_DEFAULT,
                    "options": [
                        {"value": "auto", "label": "自动（doe 优先，覆盖最全）"},
                        {"value": "doe", "label": "dadsofexile"},
                        {"value": "scout", "label": "poe2scout"},
                        {"value": "ninja", "label": "poe.ninja（最准，约 52 种）"},
                    ],
                })
            elif path == "/api/leagues":
                self._json({"leagues": STATE["leagues"], "current": STATE["league"]})
            elif path == "/api/config":
                self._json(
                    {
                        "config": CONFIG,
                        "path": str(config_path()),
                        "categories": STATE["categories"],
                        "catalog_updated": _meta_value("catalog_updated"),
                    }
                )
            elif path == "/api/refresh":
                # 手动触发「赛季 + 类别 + 物品」同步，应对游戏版本更新
                self._json(refresh_metadata())
            elif path.startswith("/icon"):
                status, blob = serve_icon(args.get("id", [""])[0])
                self._binary(status, blob)
            else:
                self._static(path)
        except Exception as exc:  # noqa: BLE001
            log(f"接口异常 {path}: {exc}")
            self._json({"error": str(exc)}, status=500)

    # -- helpers -------------------------------------------------------
    def _json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self._respond(status, "application/json; charset=utf-8", body)

    def _read_json_body(self) -> dict:
        """读 POST 的 JSON 体。读不出来就返回空 dict，调用方按缺省处理。"""
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except (TypeError, ValueError):
            return {}
        if length <= 0:
            return {}
        try:
            raw = self.rfile.read(length).decode("utf-8", "replace")
            data = json.loads(raw)
        except Exception:                                       # noqa: BLE001
            return {}
        return data if isinstance(data, dict) else {}

    def _binary(self, status: int, body: bytes) -> None:
        # 图标一旦缓存到本地就不会再变，直接给 7 天长缓存
        self._respond(status, "image/png", body, cache_age=604800)

    def _static(self, path: str) -> None:
        if path in ("/", ""):
            path = "/index.html"
        target = (web_dir() / path.lstrip("/")).resolve()
        root = web_dir().resolve()
        if not str(target).startswith(str(root)) or not target.exists():
            self.send_error(404)
            return
        content_type = "text/html; charset=utf-8"
        if target.suffix == ".js":
            content_type = "application/javascript; charset=utf-8"
        elif target.suffix == ".css":
            content_type = "text/css; charset=utf-8"
        # 引用里带 ?v= 的脚本和样式是定版的，内容不会再变，给长缓存；
        # index.html 不带版本号，必须每次校验，否则改了版用户拿不到新页面。
        cache_age = 604800 if "v=" in self.path else 0
        self._respond(200, content_type, target.read_bytes(), cache_age=cache_age)

    def _respond(
        self, status: int, content_type: str, body: bytes, cache_age: int = 0
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        # cache_age > 0 的内容（图标这类不会再变的）给长缓存。
        # 否则每次切回页面，几百张图标都要发条件请求重新验证，
        # 把浏览器的连接池占满，连访问别的网站都被拖慢。
        self.send_header(
            "Cache-Control", f"public, max-age={cache_age}" if cache_age else "no-cache"
        )
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:  # noqa: A002 - 兼容基类
        pass


def _safe_int(raw: str, fallback: int, low: int, high: int) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return fallback
    return max(low, min(high, value))


def load_leagues() -> None:
    """拉取可选联盟列表（第一项是官方推荐的当前挑战联盟）。

    开启 auto_follow_latest_league 后，新赛季上线时会自动切到最新联盟，
    不需要改代码或改配置。
    """
    try:
        leagues = [entry["id"] for entry in http_json(NINJA_LEAGUES, retries=2)]
        if not leagues:
            raise RuntimeError("联盟列表为空")
    except Exception as exc:  # noqa: BLE001
        log(f"联盟列表拉取失败：{exc}（沿用 {LEAGUE}）")
        with STATE_LOCK:
            STATE["leagues"] = [LEAGUE]
        return

    with STATE_LOCK:
        STATE["leagues"] = leagues

    if CONFIG.get("auto_follow_latest_league") and leagues and leagues[0] != STATE["league"]:
        log(f"检测到新赛季：{STATE['league']} → {leagues[0]}")
        with STATE_LOCK:
            STATE["league"] = leagues[0]
        CONFIG["league"] = leagues[0]
        save_config(CONFIG)


def discover_categories() -> int:
    """探测官方新增的通货类别。

    官方静态数据里会列出所有物品分组，逐个去 poe.ninja 试一次，
    能正常返回且有数据的就纳入追踪列表——这样版本更新新增类别时无需改代码。
    """
    try:
        sections = [
            entry["id"] for entry in http_json(EN_STATIC, retries=2)["result"]
        ]
    except Exception as exc:  # noqa: BLE001
        log(f"类别探测失败（静态数据不可用）：{exc}")
        return 0

    known = {entry["id"]: entry for entry in CONFIG.get("categories", [])}
    added = 0
    for section in sections:
        if section in known:
            continue
        try:
            data = http_json(
                f"{NINJA_API}/exchange/current/overview"
                f"?league={urllib.parse.quote(STATE['league'])}&type={urllib.parse.quote(section)}",
                retries=1,
            )
        except Exception:  # noqa: BLE001 - 不支持的类别会返回 4xx，直接跳过
            continue
        if not (data.get("lines") or []):
            continue
        known[section] = {"id": section, "label": section, "enabled": True}
        added += 1
        log(f"发现新类别：{section}（{len(data['lines'])} 项）")
        time.sleep(0.3)

    if added:
        CONFIG["categories"] = list(known.values())
        save_config(CONFIG)
    return added


def refresh_metadata() -> dict:
    """一次性同步「赛季 + 类别 + 物品名称图标」，供界面上的「检查更新」按钮调用。"""
    result = {"leagues": False, "categories": 0, "items": 0, "error": ""}
    try:
        load_leagues()
        result["leagues"] = True
        if CONFIG.get("auto_discover_categories"):
            result["categories"] = discover_categories()
        result["items"] = load_item_catalog()
        if result["items"]:
            with db() as connection:
                connection.execute(
                    "INSERT OR REPLACE INTO app_meta (k, v) VALUES ('catalog_updated', ?)",
                    (str(int(time.time())),),
                )
        global CATEGORIES
        CATEGORIES = enabled_categories()
        with STATE_LOCK:
            STATE["categories"] = [{"id": c, "label": l} for c, l in CATEGORIES]
    except Exception as exc:  # noqa: BLE001
        result["error"] = str(exc)
        log(f"元数据同步失败：{exc}")
    return result


# ------------------------------------------------------------------ 界面载体
# 早期只有「系统浏览器」一种载体（webbrowser.open）。v1.26 起默认改成原生窗口：
# 用 pywebview 起一个真正的桌面窗口，里面嵌系统自带的 WebView2 渲染 web/ 那一套前端。
# 后端（抓取线程 + :8712 HTTP 服务）和前端（app.js 等）一行都不用改，只是换了个壳。
#
# ⚠️ 为什么必须把 server 挪到后台线程：webview.start() 必须在主线程跑（WinForms 要求），
# 而它和 serve_forever() 一样是阻塞的。所以窗口模式 = 主线程跑窗口、serve_forever 退到子线程。
# 关掉窗口 → start() 返回 → 主线程接着停服务、进程退出（关窗即退出，不再留后台进程）。

WINDOW_WIDTH = 1440
WINDOW_HEIGHT = 920
WINDOW_MIN_WIDTH = 980
WINDOW_MIN_HEIGHT = 640


def window_size() -> tuple[int, int, int, int]:
    """算窗口尺寸，顺手收一收，别超出屏幕。

    pywebview 的宽高按**逻辑像素**算（缩放由系统处理，125% 缩放下 1440 会渲染成
    1800 物理像素），所以这里也拿逻辑尺寸来夹。不夹的话，1366x768 的老笔记本上
    窗口会伸到屏幕外面，右下角连关闭按钮都点不到。
    """
    w, h = WINDOW_WIDTH, WINDOW_HEIGHT
    try:
        import ctypes

        sw = ctypes.windll.user32.GetSystemMetrics(0)
        sh = ctypes.windll.user32.GetSystemMetrics(1)
        if sw > 200 and sh > 200:
            w = min(w, int(sw * 0.94))
            h = min(h, int(sh * 0.92))
    except Exception:                                      # noqa: BLE001
        pass
    # 屏幕极小时最小尺寸也得跟着缩，否则 min_size > 实际尺寸会被系统拒绝
    return w, h, min(WINDOW_MIN_WIDTH, w), min(WINDOW_MIN_HEIGHT, h)


def hide_console() -> None:
    """隐藏本进程的控制台黑框（--hide-console）。

    ⚠️ v1.27.4 起 exe 是「不带控制台」打包的，平时压根没有黑框，
    这个函数只在「已经开了一个（--console / config console: true / 从 cmd 启动）」
    又想临时收起来的时候才有用，保留它是为了不破坏老用法。
    """
    try:
        import ctypes

        hwnd = ctypes.windll.kernel32.GetConsoleWindow()
        if hwnd:
            ctypes.windll.user32.ShowWindow(hwnd, 0)      # 0 = SW_HIDE
    except Exception:                                      # noqa: BLE001
        pass


def has_console() -> bool:
    """当前进程是否已经挂着一个控制台窗口。"""
    try:
        import ctypes

        return bool(ctypes.windll.kernel32.GetConsoleWindow())
    except Exception:                                      # noqa: BLE001
        return False


def ensure_console() -> bool:
    """需要看实时输出时，现场给自己开一个控制台。

    exe 是用 console=False 打包的（双击只有程序窗口，不闪黑框），
    代价是进程根本没有 stdout，日志一股脑全丢。两条路都想要，就只能在
    真正需要的时候把控制台「补」回来：

      · 纯服务模式（--no-window，局域网给别的设备访问）——没黑框就没法 Ctrl+C；
      · config.json 里 console 改成 true —— 排障时看实时日志；
      · 命令行加 --console —— 同上，临时看一眼。

    AllocConsole 之后标准句柄（0/1/2）指向的还是「没有」，必须自己
    把 CONIN$/CONOUT$ dup2 过去，再重建 sys.stdin/stdout/stderr，
    否则 print 依旧什么都不输出。
    """
    if has_console():
        return False                       # 已经有了（从 cmd 启动 / 控制台版打包）
    try:
        import ctypes

        if not ctypes.windll.kernel32.AllocConsole():
            return False
    except Exception:                      # noqa: BLE001
        return False
    try:
        # CONOUT$/CONIN$ 是 Windows 控制台的设备名，os.open 拿到 fd 再 dup2 到标准句柄
        out_fd = os.open("CONOUT$", os.O_RDWR | os.O_BINARY)
        os.dup2(out_fd, 1)
        os.dup2(out_fd, 2)
        in_fd = os.open("CONIN$", os.O_RDONLY | os.O_BINARY)
        os.dup2(in_fd, 0)
        sys.stdout = io.TextIOWrapper(os.fdopen(1, "wb", closefd=False),
                                      encoding="utf-8", errors="replace",
                                      line_buffering=True)
        sys.stderr = io.TextIOWrapper(os.fdopen(2, "wb", closefd=False),
                                      encoding="utf-8", errors="replace",
                                      line_buffering=True)
        sys.stdin = io.TextIOWrapper(os.fdopen(0, "rb", closefd=False),
                                     encoding="utf-8", errors="replace")
        return True
    except Exception:                      # noqa: BLE001
        return False


def alert(message: str) -> None:
    """弹一个系统消息框。控制台可能被隐藏，光 print 用户看不见。"""
    try:
        import ctypes

        ctypes.windll.user32.MessageBoxW(
            None, message, f"倒狗工具 v{VERSION}", 0x40)   # 0x40 = MB_ICONINFORMATION
    except Exception:                                      # noqa: BLE001
        print(message)


def run_window(url: str, server) -> bool:
    """起原生窗口并阻塞到窗口关闭。返回 False 表示窗口不可用，调用方应回退到浏览器。

    pywebview 走系统 WebView2（Win10/11 自带），所以不往包里塞浏览器内核。
    任何一步失败都不让程序挂掉——退回浏览器模式，功能一点不少。
    """
    try:
        import webview
    except Exception as exc:                               # noqa: BLE001
        log(f"原生窗口不可用（{exc}），改用浏览器打开")
        return False

    w, h, mw, mh = window_size()
    log(f"窗口尺寸 {w}x{h}（最小 {mw}x{mh}）")
    try:
        window = webview.create_window(
            f"PoE2 通货追踪 / 倒货工具 v{VERSION}",
            url,
            width=w,
            height=h,
            min_size=(mw, mh),
        )

        def on_closed() -> None:
            # 关窗那一刻就把服务停掉，别留下占着端口的孤儿进程。
            # ⚠️ 必须丢到别的线程做：server.shutdown() 会一直阻塞到 serve_forever 退出，
            #    而 closed 回调跑在 pywebview 的 GUI 线程里 —— 在那儿阻塞会把窗口销毁
            #    流程一起卡住，结果是 start() 永远不返回、进程退不掉
            #    （实测踩过：端口都释放了，任务管理器里那个 exe 还挂着）。
            threading.Thread(target=server.shutdown, daemon=True).start()

        window.events.closed += on_closed
    except Exception as exc:                               # noqa: BLE001
        log(f"窗口创建失败（{exc}），改用浏览器打开")
        return False

    try:
        webview.start()                                    # 阻塞，直到窗口关闭
    except Exception as exc:                               # noqa: BLE001
        log(f"窗口运行异常（{exc}），改用浏览器打开")
        return False

    try:
        server.shutdown()
    except Exception:                                      # noqa: BLE001
        pass
    return True


def port_in_use(port: int) -> bool:
    """端口上是不是已经有一个本程序在跑。

    Windows 上 SO_REUSEADDR 允许两个进程同时绑定同一个端口，请求会被随机分给其中一个，
    结果就是「明明换了新版却还是旧行为」。宁可启动时直接拒绝，也不要这种玄学问题。
    """
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/meta", timeout=2) as resp:
            return resp.status == 200
    except Exception:  # noqa: BLE001 - 连不上就说明没人占着
        return False


def main() -> None:
    # ⚠️ 决定「要不要黑框」必须排在任何 print 之前：
    #    exe 是不带控制台打包的，晚一步开就丢掉了这之前的全部输出
    #    （端口占用那段提示最要命——用户会看到一个弹框，却不知道前因后果）。
    #    需要黑框的三种情况：配置里打开、命令行加 --console、
    #    纯服务模式（--no-window，没有窗口可关，得留个能 Ctrl+C 的地方）。
    wants_console = (
        bool(CONFIG.get("console"))
        or "--console" in sys.argv
        or "--no-window" in sys.argv
        or "--no-browser" in sys.argv
        or bool(os.environ.get("POE2_NO_BROWSER"))
    )
    if wants_console:
        ensure_console()
    # 记一笔控制台状态。这是「到底有没有黑框」唯一可靠的判据——
    # 外部没法判断：Win10 之后控制台窗口归 conhost.exe 所有，不属于本进程，
    # 按 PID 枚举顶层窗口永远找不到它（踩过：据此写出的用例 A 假通过、B 假失败）。
    log(f"控制台：{'已显示' if has_console() else '未显示（正常，日志见 ' + str(LOG_DIR) + '）'}")
    setup_dirs()
    init_db()

    if port_in_use(PORT):
        # 用 log 而不是 print：不只写屏幕，也要落进当天日志文件
        log("=" * 58)
        log(f" 端口 {PORT} 上已经有一个本程序在运行了。")
        log(" 请先关闭原来的那个窗口，再重新启动——")
        log(" 否则新旧两份会抢同一个端口，看到的可能还是旧版本。")
        log("=" * 58)
        # 控制台可能被 --hide-console 隐藏了，光靠 print 用户什么也看不到
        alert(f"端口 {PORT} 上已经有一个本程序在运行了。\n\n"
              f"请先关闭原来那个窗口，再重新启动——\n"
              f"否则新旧两份会抢同一个端口，看到的可能还是旧版本。")
        return

    # 先补齐物品库，排除名单里的中文名才有东西可对照
    boot_worker = FetchWorker()
    boot_worker.ensure_item_catalog()

    # 一律走 log()：屏幕上看不看得见无所谓，但必须落进当天日志文件
    log("=" * 58)
    log(f" PoE2 通货汇率追踪器 v{VERSION}")
    if ADAPTIVE_POLL:
        log(
            f" 联盟：{LEAGUE}    更新跟踪：自适应探测 "
            f"{POLL_MIN_SECONDS}~{POLL_MAX_SECONDS} 秒（源站一更新就抓，兜底 {POLL_FALLBACK_SECONDS // 60} 分钟）"
        )
    else:
        log(f" 联盟：{LEAGUE}    抓取间隔：固定 {INTERVAL_SECONDS // 60} 分钟")
    log(f" 数据：{DB_PATH}")
    log(f" 配置：{config_path()}（可直接编辑，改完重启生效）")
    log(f" 类别：{len(CATEGORIES)} 个")
    log(f" 日志：{LOG_DIR}")
    if SPREAD_ENABLED:
        log(
            f" 买卖挂单：每 {SPREAD_ROUND_SECONDS // 60} 分钟扫 {SPREAD_PAIRS_PER_ROUND} 个通货"
            f"（基准 {'/'.join(SPREAD_REFS)}，间隔 {SPREAD_REQUEST_GAP:.0f}s，"
            f"榜单保留 {SPREAD_WINDOW_HOURS} 小时）"
        )
        log(
            f" 差价榜：排除 {len(excluded_currency_ids())} 种基础通货，"
            f"只显示点差前 {SPREAD_DISPLAY_LIMIT} 名"
        )
    else:
        log(" 买卖差价榜：已停用（不再请求官方交易接口，避免官方限流）")
    log("=" * 58)

    load_leagues()

    worker = boot_worker
    worker.start()

    # 买卖挂单走独立线程：少量多次，避免撞上官方接口的长惩罚限流
    # 差价榜板块已移除，默认不启动——官方交易接口限流太狠，没必要为一个不显示的板块冒险
    spread_worker = None
    if SPREAD_ENABLED:
        spread_worker = SpreadWorker(SPREAD_ROUND_SECONDS, SPREAD_PAIRS_PER_ROUND)
        spread_worker.start()

    # 云端补数据：本机没抓到的时段（关机、没开程序）由云端那份补上。
    # 拉不到就静默跳过，不影响本机抓取与界面。
    if CLOUD_SYNC_URL:
        CloudSyncWorker().start()
        # ⚠️ 这句说的是「本机多久去云端取一次」，不是「云端多久抓一轮」——
        #    后者由 GitHub Actions 的 cron 决定。两者以前都写成「每 N 分钟一次」，
        #    容易让人以为云端抓取频率被改了，所以这里必须点明是本机拉取间隔。
        log(f"云端补数据已启用（本机每 {CLOUD_SYNC_INTERVAL // 60} 分钟拉一次云端数据；"
            f"云端抓取由 GitHub Actions 定时执行）")

    # scout 历史自补：不依赖 GitHub Actions，直接向 scout 要 6 小时粒度的历史。
    # 云端那头实测只有 ~23% 的跑成率，这个兜底保证「离线回来一定补得到东西」。
    if SCOUT_BACKFILL:
        ScoutBackfillWorker(str(STATE.get("league") or CONFIG.get("league") or "")).start()

    server, port = bind_server()
    server.worker = worker  # type: ignore[attr-defined] - 供 HTTP handler 调用切换联盟
    server.spread_worker = spread_worker  # type: ignore[attr-defined]
    url = f"http://127.0.0.1:{port}/"
    # 界面载体：window（默认，原生窗口）/ browser（系统浏览器）/ none（只起服务）
    # --no-browser 与 POE2_NO_BROWSER 沿用旧语义 = 纯服务模式（给手机 / 局域网访问用）
    if "--browser" in sys.argv:
        mode = "browser"
    elif ("--no-window" in sys.argv or "--no-browser" in sys.argv
          or os.environ.get("POE2_NO_BROWSER")):
        mode = "none"
    else:
        mode = "window"
    log(f"本地服务已启动：{url}  （{'关闭窗口即可停止' if mode == 'window' else '关闭此窗口即可停止'}）")
    if HOST in ("0.0.0.0", "", "::"):
        others = lan_ips()
        if others:
            log("局域网访问地址（同一 WiFi/网线下可直接打开）：")
            for ip in others:
                log(f"    http://{ip}:{port}/")
            log("    若其它设备打不开，通常是 Windows 防火墙拦截，允许一次即可；")
            log("    只想本机使用的话，把 config.json 里的 host 改成 127.0.0.1。")
        else:
            log("未检测到局域网 IP，其它设备暂时无法访问（当前可能没有联网）")
    if "--hide-console" in sys.argv:
        hide_console()

    if mode == "window":
        # webview.start() 必须在主线程，所以 serve_forever 退到后台线程
        srv_thread = threading.Thread(target=server.serve_forever,
                                      name="http-server", daemon=True)
        srv_thread.start()
        log("正在打开程序窗口 …")
        if run_window(url, server):
            log("窗口已关闭，正在退出 …")
            return
        # 窗口起不来 → 回退浏览器。serve_forever 已在子线程跑，主线程不能再跑一遍
        log("已回退到浏览器模式")
        threading.Timer(1.2, lambda: webbrowser.open(url)).start()
        try:
            while srv_thread.is_alive():
                srv_thread.join(1.0)
        except KeyboardInterrupt:
            log("正在退出 …")
            server.shutdown()
        return

    if mode == "browser":
        threading.Timer(1.2, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log("正在退出 …")
        server.shutdown()


def lan_ips() -> list[str]:
    """列出本机所有局域网 IPv4——有线网卡和无线网卡都会列出来。

    服务绑在 0.0.0.0 上，所以这些地址（分属不同网段时也一样）都能访问。
    """
    found: list[str] = []
    try:
        # 主机名解析会带出本机每张网卡的地址（有线 / 无线 / 虚拟网卡都在里面）
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ip = info[4][0]
            if ip.startswith("127.") or ip.startswith("169.254."):
                continue  # 回环与自动配置地址不可能是别人要访问的地址
            if ip not in found:
                found.append(ip)
    except OSError:
        pass
    try:
        # 路由表认定的默认出口才是真正在用的那张网卡，把它排到最前面
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        probe.connect(("8.8.8.8", 80))  # UDP connect 不发包，只为问路由表要出口 IP
        ip = probe.getsockname()[0]
        probe.close()
        if ip and not ip.startswith("127.") and not ip.startswith("169.254."):
            if ip in found:
                found.remove(ip)
            found.insert(0, ip)
    except OSError:
        pass
    return found


def bind_server() -> tuple[ThreadingHTTPServer, int]:
    """绑定端口；被占用时顺延尝试后续 5 个端口。"""
    last: OSError | None = None
    for candidate in range(PORT, PORT + 5):
        try:
            return ThreadingHTTPServer((HOST, candidate), Handler), candidate
        except OSError as exc:
            last = exc
    raise RuntimeError(f"无法绑定 {HOST}:{PORT}-{PORT + 4}：{last}")


if __name__ == "__main__":
    # ⚠️ 没有控制台之后，未捕获异常的表现是「双击了一下，什么都没发生」——
    #    用户既看不到窗口也看不到报错，只会以为程序坏了。所以这里必须兜住：
    #    写日志 + 弹系统消息框，两样都不依赖控制台。
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception as exc:                                # noqa: BLE001
        import traceback

        try:
            log(f"程序异常退出：{exc}")
            log(traceback.format_exc())
        except Exception:                                   # noqa: BLE001
            pass
        alert(f"程序出错了，已停止运行：\n\n{exc}\n\n"
              f"详细信息已写入 data\\logs 目录下当天的日志文件。")
