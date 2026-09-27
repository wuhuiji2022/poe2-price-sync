"""暗金中文化词典（运行时只读）。

词典由 build_zh.py 从 poe2db.tw/tw（繁体，对标国际服用语）抓取，
并与同一站点的英文页（/us/）逐条对齐后生成，存成 data/unique_zh.json。
本模块只负责加载和查表，不发任何网络请求。

译名来自第三方中文数据库，仅用于显示，不影响价格计算。
"""
from __future__ import annotations

import json
import pathlib
import re

# ---------- 文本工具（build_zh.py 与 app.py 共用） ----------
_MOD_LINK = re.compile(r"\[([^\]\|]*)\|([^\]]*)\]")
_SINGLE_LINK = re.compile(r"\[([^\]\|]*)\]")
_NUM = re.compile(r"-?\d+(?:\.\d+)?")
_DASH = re.compile(r"[—–−]")  # poe2db 用 em dash，ninja 用减号
_RANGE_SEP = re.compile(r"(\d)\s*-\s*(\d)")


def clean_en(text: str) -> str:
    """ninja 词缀文本 → 可读英文：解开 [Key|Display] 与 [Key] 标记。"""
    if not text:
        return ""
    text = _MOD_LINK.sub(lambda m: (m.group(2) or m.group(1) or "").strip(), text)
    text = _SINGLE_LINK.sub(lambda m: m.group(1).strip(), text)
    return re.sub(r"\s+", " ", text).strip()


def template_of(text: str) -> tuple[str, list[str]]:
    """抽出文本里的数字，返回 (数字占位模板, 数值列表)。"""
    if not text:
        return "", []
    norm = _DASH.sub("-", text)
    # "-(2-1.9)" 整体取负，把负号挪进括号，否则会拆成 2 和 -1.9。
    # 但 "(29-34)-(54-65)" 里的 "-" 只是区间连字符（前面是右括号），不能动。
    norm = re.sub(r"(?<![\d)\]])-\s*\(", "(-", norm)
    # 区间连字符先保护起来：否则 "45-60" 会被拆成 45 和 -60
    norm = _RANGE_SEP.sub(r"\1~\2", norm)
    values = [m.group(0) for m in _NUM.finditer(norm)]
    tmpl = _NUM.sub("#", norm).replace("~", "-")
    return tmpl, values


def collapse_tmpl(tmpl: str) -> str:
    """把区间占位折叠成单个 #，让「区间写法」和「单值写法」能互相匹配。

    poe.ninja 给的是区间 "+(100-137)"，官方挂单给的是单值 "+91"，
    两者必须是同一个键才能查到同一条翻译。
    """
    t = tmpl
    for _ in range(3):
        t = re.sub(r"#\s*-\s*#", "#", t)
    # 区间写法常带括号 "+(45-60)"，单值写法没有 "+91"，连括号一起剥掉才能对上
    t = re.sub(r"\(\s*#\s*\)", "#", t)
    return re.sub(r"#{2,}", "#", t).strip()


def fill_template(tmpl: str, values: list[str]) -> str:
    """把模板里的 # 按顺序换成实际数值。

    挂单给的是单值（+91），而词典里的中文模板是区间写法（+(#-#)），
    这时把同一个值填进整段区间，别留下一个填不上的 #。
    """
    if len(values) == 1 and tmpl.count("#") > 1:
        value = values[0]
        merged = re.sub(r"#\s*-\s*#", value, tmpl)
        # 单值不需要括号：+(91) -> +91
        return re.sub(r"\(\s*-?\d+(?:\.\d+)?\s*\)", value, merged.replace("#", value))
    out: list[str] = []
    idx = 0
    for ch in tmpl:
        if ch == "#" and idx < len(values):
            out.append(values[idx])
            idx += 1
        else:
            out.append(ch)
    return "".join(out)


def slugify(name: str) -> str:
    """物品名 → poe2db 的 URL slug：去撇号、非字母数字转下划线、小写。"""
    s = (name or "").replace("'", "").replace("’", "")
    s = re.sub(r"[^A-Za-z0-9]+", "_", s)
    return s.strip("_").lower()


# 属性名种类很少，固定译名；药水类整句替换。
# 与词典一致，全部用 poe2db.tw/tw 的繁体用语（对标国际服）。
PROP_ZH = {
    "armour": "護甲",
    "runic ward": "符文結界",
    "energy shield": "能量護盾",
    "evasion rating": "閃避值",
    "critical hit chance": "暴擊率",
    "attacks per second": "每秒攻擊次數",
    "physical damage": "物理傷害",
    "lightning damage": "閃電傷害",
    "cold damage": "冰冷傷害",
    "fire damage": "火焰傷害",
    "chaos damage": "混沌傷害",
    "block chance": "格擋率",
    "spirit": "精魂",
    "reload time": "裝填時間",
    "charm slots": "咒符欄",
}
PROP_TPL = {
    "Lasts # Seconds": "持續 # 秒",
    "Consumes # of # Charges on use": "使用時消耗 # / # 充能",
    "Recovers # Life over # Seconds": "在 # 秒內回復 # 點生命",
    "Recovers # Mana over # Seconds": "在 # 秒內回復 # 點魔力",
    "Recovers # Life every # Seconds": "每 # 秒回復 # 點生命",
    "Recovers # Mana every # Seconds": "每 # 秒回復 # 點魔力",
}


class ZhDict:
    """暗金中文名 + 词缀翻译。词典缺失时退化成清洗过的英文。"""

    def __init__(self) -> None:
        self.names: dict[str, dict] = {}
        self.mods: dict[str, str] = {}
        self.bases: dict[str, str] = {}  # 底材英文名 slug -> 中文（可为空）
        self.loaded: bool = False
        self.path: str = ""

    def load(self, path: pathlib.Path | str) -> bool:
        try:
            data = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return False
        self.names = data.get("names") or {}
        self.mods = data.get("mods") or {}
        self.bases = data.get("bases") or {}
        self.path = str(path)
        self.loaded = bool(self.names or self.mods)
        return self.loaded

    def name(self, en_name: str) -> str:
        """暗金中文名，找不到返回空串（前端自行回落英文名）。"""
        if not self.names:
            return ""
        entry = self.names.get(slugify(en_name))
        return str(entry.get("zh") or "") if isinstance(entry, dict) else ""

    def base(self, en_name: str) -> str:
        """暗金对应的底材中文名（poe2db 给的是该暗金的主底材）。"""
        if not self.names:
            return ""
        entry = self.names.get(slugify(en_name))
        return str(entry.get("base") or "") if isinstance(entry, dict) else ""

    def base_type(self, en_base: str) -> str:
        """按底材英文名查中文；没有底材词典时返回空串，由调用方回落。"""
        if not self.bases or not en_base:
            return ""
        return str(self.bases.get(slugify(en_base)) or "")

    def mod(self, text: str) -> str:
        """词缀翻译：命中词典就回填真实数值，否则退回英文。"""
        en = clean_en(text)
        if not en:
            return ""
        tmpl, values = template_of(en)
        zh_tmpl = self.mods.get(collapse_tmpl(tmpl))
        if not zh_tmpl:
            return en
        return fill_template(zh_tmpl, values)

    def prop(self, text: str) -> str:
        """属性翻译（护甲/能量护盾/伤害等）。"""
        en = clean_en(text)
        if not en:
            return ""
        tmpl, values = template_of(en)
        zh_tmpl = PROP_TPL.get(tmpl)
        if zh_tmpl:
            return fill_template(zh_tmpl, values)
        if ":" in en:
            head, _, rest = en.partition(":")
            zh_name = PROP_ZH.get(head.strip().lower())
            if zh_name:
                rest_tmpl, rest_vals = template_of(rest)
                rest_text = fill_template(rest_tmpl, rest_vals) if rest_vals else rest.strip()
                return f"{zh_name}: {rest_text}"
        return en


ZH = ZhDict()
