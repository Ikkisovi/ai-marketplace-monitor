import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple

from .listing import Listing
from .utils import amm_home


_MODEL_PATTERN_RAW: dict[str, tuple[str, ...]] = {
    "ricoh_gr3xhdf": (
        r"\bgr3x\s*hdf\b",
        r"\bgr3xhdf\b",
        r"\bgriiix\s*hdf\b",
        r"\bgriiixhdf\b",
        r"\bgr\s*iii\s*x\s*hdf\b",
    ),
    "ricoh_gr3hdf": (
        r"\bgr3\s*hdf\b",
        r"\bgr3hdf\b",
        r"\bgriii\s*hdf\b",
        r"\bgriiihdf\b",
        r"\bgr\s*iii\s*hdf\b",
    ),
    "ricoh_gr3x": (
        r"\bgr3x(?!\s*hdf|hdf)\b",
        r"\bgriiix(?!\s*hdf|hdf)\b",
        r"\bgr\s*iii\s*x(?!\s*hdf)\b",
    ),
    "ricoh_gr4": (r"\bgr4\b", r"\bgriv\b", r"\bgr\s*iv\b"),
    "ricoh_gr3": (
        r"\bgr3(?!x|\s*hdf|hdf)\b",
        r"\bgriii(?!x|\s*hdf|hdf)\b",
        r"\bgr\s*iii(?!\s*x|\s*hdf)\b",
    ),
    "ricoh_gr2": (r"\bgr2\b", r"\bgrii\b", r"\bgr\s*ii\b"),
    "sony_a7c2": (
        r"\ba7c\s*ii\b",
        r"\ba7cii\b",
        r"\ba7c2\b",
        r"\balpha\s*7c\s*ii\b",
        r"\bsony\s*a7c\s*ii\b",
        r"\bsony\s*a7cii\b",
        r"\bsony\s*a7c2\b",
        r"\bilce[-\s]?7cm2\b",
    ),
    "sony_a7cr": (r"\ba7cr\b", r"\balpha\s*7cr\b", r"\bilce[-\s]?7cr\b"),
    "sony_a1ii": (
        r"\ba1\s*ii\b",
        r"\ba1ii\b",
        r"\ba1m2\b",
        r"\balpha\s*1\s*ii\b",
        r"\bilce[-\s]?1m2\b",
    ),
    "sony_a1": (
        r"\bsony\s*a1(?!\s*(ii|2|m2)|ii|2|m2)\b",
        r"\balpha\s*1(?!\s*(ii|2)|ii|2)\b",
        r"\bilce[-\s]?1\b",
    ),
    "sony_a9iii": (
        r"\ba9\s*iii\b",
        r"\ba9iii\b",
        r"\ba93\b",
        r"\balpha\s*9\s*iii\b",
        r"\bilce[-\s]?9m3\b",
    ),
    "sony_a9ii": (
        r"\ba9\s*ii\b",
        r"\ba9ii\b",
        r"\ba92\b",
        r"\balpha\s*9\s*ii\b",
        r"\bilce[-\s]?9m2\b",
    ),
    "sony_a9": (
        r"\ba9(?!\s*(ii|iii|2|3)|ii|iii|2|3)\b",
        r"\balpha\s*9(?!\s*(ii|iii|2|3)|ii|iii|2|3)\b",
        r"\bsony\s*a9(?!\s*(ii|iii|2|3)|ii|iii|2|3)\b",
        r"\bilce[-\s]?9\b",
    ),
    "sony_a7c": (
        r"\ba7c(?!\s*(ii|2|r)|ii|2|r)\b",
        r"\balpha\s*7c(?!\s*(ii|2|r))\b",
        r"\bsony\s*a7c(?!\s*(ii|2|r))\b",
        r"\bilce[-\s]?7c\b",
    ),
    "sony_a7ii": (r"\ba7\s*ii\b", r"\ba7ii\b"),
    "sony_a7iii": (r"\ba7\s*iii\b", r"\ba7iii\b"),
    "sony_a7iv": (r"\ba7\s*iv\b", r"\ba7iv\b"),
    "sony_a7r": (
        r"\ba7r(?!\s*(ii|iii|iv|v|2|3|4)|ii|iii|iv|v|2|3|4)\b",
        r"\balpha\s*7r(?!\s*(ii|iii|iv|v|2|3|4)|ii|iii|iv|v|2|3|4)\b",
        r"\bsony\s*a7r(?!\s*(ii|iii|iv|v|2|3|4)|ii|iii|iv|v|2|3|4)\b",
        r"\ba7r\s*[56]\b",
        r"\ba7r[56]\b",
    ),
    "sony_a7rii": (r"\ba7r\s*ii\b", r"\ba7rii\b", r"\ba7r2\b"),
    "sony_a7riii": (r"\ba7r\s*iii\b", r"\ba7riii\b", r"\ba7r3\b"),
    "sony_a7riv": (r"\ba7r\s*iv\b", r"\ba7riv\b", r"\ba7r4\b"),
    "sony_a7rv": (r"\ba7r\s*v\b", r"\ba7rv\b"),
    "sony_a7sii": (r"\ba7s\s*ii\b", r"\ba7sii\b", r"\ba7s2\b"),
    "sony_a7siii": (r"\ba7s\s*iii\b", r"\ba7siii\b", r"\ba7s3\b"),
    "sony_a7s": (r"\ba7s\b",),
    "sony_a7": (
        r"\ba7(?!\s*(ii|iii|iv|v|2|3|4|5|c|cr|r|s)|ii|iii|iv|v|2|3|4|5|c|cr|r|s)\b",
        r"\balpha\s*7(?!\s*(ii|iii|iv|v|2|3|4|5|c|cr|r|s)|ii|iii|iv|v|2|3|4|5|c|cr|r|s)\b",
        r"\bsony\s*a7(?!\s*(ii|iii|iv|v|2|3|4|5|c|cr|r|s)|ii|iii|iv|v|2|3|4|5|c|cr|r|s)\b",
    ),
    "sony_rx100vii": (
        r"\brx100\s*vii\b",
        r"\brx100vii\b",
        r"\brx100m7\b",
        r"\brx100\s*mark\s*vii\b",
        r"\brx1007\b",
    ),
    "sony_rx100vi": (
        r"\brx100\s*vi\b",
        r"\brx100vi\b",
        r"\brx100m6\b",
        r"\brx100\s*mark\s*vi\b",
        r"\brx1006\b",
    ),
    "sony_rx100va": (
        r"\brx100\s*va\b",
        r"\brx100va\b",
        r"\brx100m5a\b",
    ),
    "sony_rx100v": (
        r"\brx100\s*v(?!a)\b",
        r"\brx100v(?!a)\b",
        r"\brx100m5(?!a)\b",
        r"\brx1005\b",
    ),
    "sony_rx100iv": (
        r"\brx100\s*iv\b",
        r"\brx100iv\b",
        r"\brx100m4\b",
        r"\brx100\s*mark\s*iv\b",
        r"\brx1004\b",
    ),
    "sony_rx100iii": (
        r"\brx100\s*iii\b",
        r"\brx100iii\b",
        r"\brx100m3\b",
        r"\brx100\s*mark\s*iii\b",
        r"\brx1003\b",
    ),
    "sony_rx100ii": (
        r"\brx100\s*ii\b",
        r"\brx100ii\b",
        r"\brx100m2\b",
        r"\brx100\s*mark\s*ii\b",
        r"\brx1002\b",
    ),
    "sony_rx100": (
        r"\brx100(?!\s*(ii|iii|iv|v|vi|vii|2|3|4|5|6|7|m2|m3|m4|m5|m5a|m6|m7)|ii|iii|iv|v|vi|vii|2|3|4|5|6|7|m2|m3|m4|m5|m5a|m6|m7)\b",
    ),
    "sony_rx10iv": (
        r"\brx10\s*iv\b",
        r"\brx10iv\b",
        r"\brx10m4\b",
        r"\brx104\b",
    ),
    "sony_rx10iii": (
        r"\brx10\s*iii\b",
        r"\brx10iii\b",
        r"\brx10m3\b",
        r"\brx103\b",
    ),
    "sony_rx10ii": (
        r"\brx10\s*ii\b",
        r"\brx10ii\b",
        r"\brx10m2\b",
        r"\brx102\b",
    ),
    "sony_rx10": (
        r"\brx10(?!\s*(ii|iii|iv|2|3|4|m2|m3|m4)|ii|iii|iv|2|3|4|m2|m3|m4)\b",
    ),
    "sony_rx1r2": (
        r"\brx1r\s*ii\b",
        r"\brx1rii\b",
        r"\brx1r2\b",
        r"\brx1r\s*mark\s*ii\b",
    ),
    "sony_rx1r": (r"\brx1r(?!\s*(ii|2|mark\s*ii)|ii|2)\b",),
    "sony_rx1": (r"\brx1(?!r)\b",),
    "sony_fx30": (r"\bfx30\b",),
    "sony_fx3": (r"\bfx3\b",),
    "sony_fx6": (r"\bfx6\b",),
    "sony_fx9": (r"\bfx9\b",),
    "sony_zve1": (r"\bzv[-\s]?e1\b", r"\bzve1\b"),
    "sony_zve10ii": (
        r"\bzv[-\s]?e10\s*ii\b",
        r"\bzve10ii\b",
        r"\bzv[-\s]?e10m2\b",
    ),
    "sony_zv1": (r"\bzv[-\s]?1\b", r"\bzv1\b"),
    "sony_zv1ii": (r"\bzv[-\s]?1\s*ii\b", r"\bzv1ii\b", r"\bzv[-\s]?1m2\b"),
    "sony_zv1f": (r"\bzv[-\s]?1f\b", r"\bzv1f\b"),
    "sony_zve10": (r"\bzv[-\s]?e10\b", r"\bzve10\b"),
    "sony_a5000": (r"\ba5000\b", r"\ba\s*5000\b", r"\balpha\s*5000\b"),
    "sony_a5100": (r"\ba5100\b", r"\ba\s*5100\b", r"\balpha\s*5100\b"),
    "sony_a6000": (r"\ba6000\b", r"\ba\s*6000\b", r"\balpha\s*6000\b"),
    "sony_a6100": (r"\ba6100\b", r"\ba\s*6100\b", r"\balpha\s*6100\b"),
    "sony_a6300": (r"\ba6300\b", r"\ba\s*6300\b", r"\balpha\s*6300\b"),
    "sony_a6700": (r"\ba6700\b", r"\ba\s*6700\b"),
    "sony_a6400": (r"\ba6400\b", r"\ba\s*6400\b"),
    "sony_a6500": (r"\ba6500\b", r"\ba\s*6500\b", r"\balpha\s*6500\b"),
    "sony_a6600": (r"\ba6600\b", r"\ba\s*6600\b", r"\balpha\s*6600\b"),
    "olympus_om1": (r"\bom[-\s]?1\b", r"\bolympus\s*om[-\s]?1\b"),
    "olympus_om2": (r"\bom[-\s]?2\b", r"\bolympus\s*om[-\s]?2\b"),
}

_MODEL_PATTERNS = {
    model: tuple(re.compile(pattern, re.IGNORECASE) for pattern in patterns)
    for model, patterns in _MODEL_PATTERN_RAW.items()
}

_TARGET_MODEL_ALIASES = {
    "gr2": "ricoh_gr2",
    "grii": "ricoh_gr2",
    "ricoh_gr2": "ricoh_gr2",
    "gr3": "ricoh_gr3",
    "griii": "ricoh_gr3",
    "ricoh_gr3": "ricoh_gr3",
    "gr3x": "ricoh_gr3x",
    "griiix": "ricoh_gr3x",
    "ricoh_gr3x": "ricoh_gr3x",
    "gr3_hdf": "ricoh_gr3hdf",
    "gr3hdf": "ricoh_gr3hdf",
    "griii_hdf": "ricoh_gr3hdf",
    "griiihdf": "ricoh_gr3hdf",
    "ricoh_gr3_hdf": "ricoh_gr3hdf",
    "ricoh_gr3hdf": "ricoh_gr3hdf",
    "gr3x_hdf": "ricoh_gr3xhdf",
    "gr3xhdf": "ricoh_gr3xhdf",
    "griiix_hdf": "ricoh_gr3xhdf",
    "griiixhdf": "ricoh_gr3xhdf",
    "ricoh_gr3x_hdf": "ricoh_gr3xhdf",
    "ricoh_gr3xhdf": "ricoh_gr3xhdf",
    "gr4": "ricoh_gr4",
    "griv": "ricoh_gr4",
    "ricoh_gr4": "ricoh_gr4",
    "a1": "sony_a1",
    "sony_a1": "sony_a1",
    "a1ii": "sony_a1ii",
    "sony_a1ii": "sony_a1ii",
    "a9": "sony_a9",
    "sony_a9": "sony_a9",
    "a9ii": "sony_a9ii",
    "sony_a9ii": "sony_a9ii",
    "a9iii": "sony_a9iii",
    "sony_a9iii": "sony_a9iii",
    "a7c2": "sony_a7c2",
    "a7cii": "sony_a7c2",
    "sony_a7c2": "sony_a7c2",
    "sony_a7cii": "sony_a7c2",
    "sony_alpha_7c_ii": "sony_a7c2",
    "sony_alpha_7c2": "sony_a7c2",
    "a7c": "sony_a7c",
    "sony_a7c": "sony_a7c",
    "a7": "sony_a7",
    "sony_a7": "sony_a7",
    "a7r": "sony_a7r",
    "sony_a7r": "sony_a7r",
    "a5000": "sony_a5000",
    "sony_a5000": "sony_a5000",
    "a5100": "sony_a5100",
    "sony_a5100": "sony_a5100",
    "a6000": "sony_a6000",
    "sony_a6000": "sony_a6000",
    "a6100": "sony_a6100",
    "sony_a6100": "sony_a6100",
    "a6300": "sony_a6300",
    "sony_a6300": "sony_a6300",
    "a6400": "sony_a6400",
    "sony_a6400": "sony_a6400",
    "a6500": "sony_a6500",
    "sony_a6500": "sony_a6500",
    "a6600": "sony_a6600",
    "sony_a6600": "sony_a6600",
    "a6700": "sony_a6700",
    "sony_a6700": "sony_a6700",
    "fx3": "sony_fx3",
    "sony_fx3": "sony_fx3",
    "fx30": "sony_fx30",
    "sony_fx30": "sony_fx30",
    "fx6": "sony_fx6",
    "sony_fx6": "sony_fx6",
    "fx9": "sony_fx9",
    "sony_fx9": "sony_fx9",
    "rx100": "sony_rx100",
    "sony_rx100": "sony_rx100",
    "rx100ii": "sony_rx100ii",
    "sony_rx100ii": "sony_rx100ii",
    "rx100iii": "sony_rx100iii",
    "sony_rx100iii": "sony_rx100iii",
    "rx100iv": "sony_rx100iv",
    "sony_rx100iv": "sony_rx100iv",
    "rx100v": "sony_rx100v",
    "sony_rx100v": "sony_rx100v",
    "rx100va": "sony_rx100va",
    "sony_rx100va": "sony_rx100va",
    "rx100vi": "sony_rx100vi",
    "sony_rx100vi": "sony_rx100vi",
    "rx100vii": "sony_rx100vii",
    "sony_rx100vii": "sony_rx100vii",
    "rx10": "sony_rx10",
    "sony_rx10": "sony_rx10",
    "rx10ii": "sony_rx10ii",
    "sony_rx10ii": "sony_rx10ii",
    "rx10iii": "sony_rx10iii",
    "sony_rx10iii": "sony_rx10iii",
    "rx10iv": "sony_rx10iv",
    "sony_rx10iv": "sony_rx10iv",
    "rx1": "sony_rx1",
    "sony_rx1": "sony_rx1",
    "sony_a7sii": "sony_a7sii",
    "sony_a7riii": "sony_a7riii",
    "sony_a7rii": "sony_a7rii",
    "sony_a7r2": "sony_a7rii",
    "sony_a7r3": "sony_a7riii",
    "sony_a7r5": "sony_a7r",
    "sony_a7r6": "sony_a7r",
    "sony_rx1r": "sony_rx1r",
    "sony_rx1r2": "sony_rx1r2",
    "zve1": "sony_zve1",
    "sony_zve1": "sony_zve1",
    "zve10ii": "sony_zve10ii",
    "sony_zve10ii": "sony_zve10ii",
    "zv1ii": "sony_zv1ii",
    "sony_zv1ii": "sony_zv1ii",
    "zv1f": "sony_zv1f",
    "sony_zv1f": "sony_zv1f",
    "sony_zv1": "sony_zv1",
    "sony_zve10": "sony_zve10",
    "olympus_film": "olympus_film",
    "olympus": "olympus_unknown",
}

_ACCESSORY_PATTERNS = (
    re.compile(r"\bquick[\s-]?release\b", re.IGNORECASE),
    re.compile(r"\bplate\b", re.IGNORECASE),
    re.compile(r"\bgrip\b", re.IGNORECASE),
    re.compile(r"\bhandgrip\b", re.IGNORECASE),
    re.compile(r"\bl[-\s]?shape\b", re.IGNORECASE),
    re.compile(r"\bl[-\s]?bracket\b", re.IGNORECASE),
    re.compile(r"\bcage\b", re.IGNORECASE),
    re.compile(r"\brig\b", re.IGNORECASE),
    re.compile(r"\bmount\b", re.IGNORECASE),
    re.compile(r"\badapter\b", re.IGNORECASE),
    re.compile(r"\bbattery\b", re.IGNORECASE),
    re.compile(r"\bcharger\b", re.IGNORECASE),
    re.compile(r"\bcase\b", re.IGNORECASE),
    re.compile(r"\bstrap\b", re.IGNORECASE),
    re.compile(r"\btripod\b", re.IGNORECASE),
    re.compile(r"\bgimbal\b", re.IGNORECASE),
    re.compile(r"\bhot[\s-]?shoe\b", re.IGNORECASE),
)

_LENS_PATTERNS = (
    re.compile(r"\blens\b", re.IGNORECASE),
    re.compile(r"\b\d{2,3}\s?mm\b", re.IGNORECASE),
    re.compile(r"\bf\/\d", re.IGNORECASE),
)

_CAMERA_PATTERNS = (
    re.compile(r"\bcamera\b", re.IGNORECASE),
    re.compile(r"\bmirrorless\b", re.IGNORECASE),
    re.compile(r"\bcamera body\b", re.IGNORECASE),
    re.compile(r"\bbody only\b", re.IGNORECASE),
)

_COMPATIBILITY_PATTERNS = (
    re.compile(r"\bfor\s+(sony|alpha|a7|zv|rx|ilce)", re.IGNORECASE),
    re.compile(r"\bcompatible with\b", re.IGNORECASE),
    re.compile(r"\bworks with\b", re.IGNORECASE),
    re.compile(r"\bfits?\b", re.IGNORECASE),
)

_SONY_MODEL_TOKEN = re.compile(
    r"\b(?:sony\s*)?(a[0-9][a-z0-9]{0,7})\b",
    re.IGNORECASE,
)

_SONY_RX_MODEL_TOKEN = re.compile(
    r"\b(?:sony\s*)?(rx(?:1r?|10|100)[a-z0-9]{0,8})\b",
    re.IGNORECASE,
)

_GENERIC_SONY_ALIAS = {
    "a1": "sony_a1",
    "a1ii": "sony_a1ii",
    "a12": "sony_a1ii",
    "a1m2": "sony_a1ii",
    "a9": "sony_a9",
    "a9ii": "sony_a9ii",
    "a92": "sony_a9ii",
    "a9iii": "sony_a9iii",
    "a93": "sony_a9iii",
    "a7sii": "sony_a7sii",
    "a7s2": "sony_a7sii",
    "a7siii": "sony_a7siii",
    "a7s3": "sony_a7siii",
    "a7": "sony_a7",
    "a7r": "sony_a7r",
    "a7riii": "sony_a7riii",
    "a7r3": "sony_a7riii",
    "a7rii": "sony_a7rii",
    "a7r2": "sony_a7rii",
    "a7riv": "sony_a7riv",
    "a7r4": "sony_a7riv",
    "a7rv": "sony_a7rv",
    # tolerate numeric suffix typos like a7r5/a7r6 as a7r family instead of unknown.
    "a7r5": "sony_a7r",
    "a7r6": "sony_a7r",
    "a7iv": "sony_a7iv",
    "a74": "sony_a7iv",
    "a7iii": "sony_a7iii",
    "a73": "sony_a7iii",
    "a7ii": "sony_a7ii",
    "a72": "sony_a7ii",
    "a7c": "sony_a7c",
    "a7c2": "sony_a7c2",
    "a7cii": "sony_a7c2",
    "a7cr": "sony_a7cr",
    "a5000": "sony_a5000",
    "a5100": "sony_a5100",
    "a6000": "sony_a6000",
    "a6100": "sony_a6100",
    "a6300": "sony_a6300",
    "a6400": "sony_a6400",
    "a6500": "sony_a6500",
    "a6600": "sony_a6600",
    "a6700": "sony_a6700",
    "zve1": "sony_zve1",
    "zve10": "sony_zve10",
    "zve10ii": "sony_zve10ii",
    "zv1": "sony_zv1",
    "zv1ii": "sony_zv1ii",
    "zv1f": "sony_zv1f",
    "fx3": "sony_fx3",
    "fx30": "sony_fx30",
    "fx6": "sony_fx6",
    "fx9": "sony_fx9",
}

_GENERIC_RX_ALIAS = {
    "rx1": "sony_rx1",
    "rx1r": "sony_rx1r",
    "rx1rii": "sony_rx1r2",
    "rx1r2": "sony_rx1r2",
    "rx1rm2": "sony_rx1r2",
    "rx100": "sony_rx100",
    "rx100ii": "sony_rx100ii",
    "rx1002": "sony_rx100ii",
    "rx100m2": "sony_rx100ii",
    "rx100iii": "sony_rx100iii",
    "rx1003": "sony_rx100iii",
    "rx100m3": "sony_rx100iii",
    "rx100iv": "sony_rx100iv",
    "rx1004": "sony_rx100iv",
    "rx100m4": "sony_rx100iv",
    "rx100v": "sony_rx100v",
    "rx1005": "sony_rx100v",
    "rx100m5": "sony_rx100v",
    "rx100va": "sony_rx100va",
    "rx100m5a": "sony_rx100va",
    "rx100vi": "sony_rx100vi",
    "rx1006": "sony_rx100vi",
    "rx100m6": "sony_rx100vi",
    "rx100vii": "sony_rx100vii",
    "rx1007": "sony_rx100vii",
    "rx100m7": "sony_rx100vii",
    "rx10": "sony_rx10",
    "rx10ii": "sony_rx10ii",
    "rx102": "sony_rx10ii",
    "rx10m2": "sony_rx10ii",
    "rx10iii": "sony_rx10iii",
    "rx103": "sony_rx10iii",
    "rx10m3": "sony_rx10iii",
    "rx10iv": "sony_rx10iv",
    "rx104": "sony_rx10iv",
    "rx10m4": "sony_rx10iv",
}

_SONY_FOUR_DIGIT_ALPHA = re.compile(r"a(\d{4})", re.IGNORECASE)

_GENERIC_MODEL_OVERRIDES: dict[str, set[str]] = {
    "sony_a1": {"sony_a1ii"},
    "sony_a9": {"sony_a9ii", "sony_a9iii"},
    "sony_a7": {
        "sony_a7ii",
        "sony_a7iii",
        "sony_a7iv",
        "sony_a7c",
        "sony_a7c2",
        "sony_a7cr",
        "sony_a7r",
        "sony_a7rii",
        "sony_a7riii",
        "sony_a7riv",
        "sony_a7rv",
        "sony_a7s",
        "sony_a7sii",
        "sony_a7siii",
    },
    "sony_a7c": {"sony_a7c2", "sony_a7cr"},
    "sony_a7r": {"sony_a7rii", "sony_a7riii", "sony_a7riv", "sony_a7rv"},
    "sony_a7s": {"sony_a7sii", "sony_a7siii"},
    "sony_rx1": {"sony_rx1r", "sony_rx1r2"},
    "sony_rx100": {
        "sony_rx100ii",
        "sony_rx100iii",
        "sony_rx100iv",
        "sony_rx100v",
        "sony_rx100va",
        "sony_rx100vi",
        "sony_rx100vii",
    },
    "sony_rx10": {"sony_rx10ii", "sony_rx10iii", "sony_rx10iv"},
    "sony_zv1": {"sony_zv1ii", "sony_zv1f"},
    "sony_zve10": {"sony_zve10ii"},
}


@dataclass(frozen=True)
class ListingClassification:
    detected_model: str
    listing_type: str
    is_target_exact: int
    confidence: float
    reason: str


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parse_datetime(ts: str) -> datetime:
    parsed = datetime.fromisoformat(ts)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _parse_price(price_text: str) -> Tuple[float | None, str | None]:
    if not price_text or price_text == "**unspecified**":
        return None, None

    primary = price_text.split("|")[0].strip()
    matched = re.search(
        r"(?P<currency>[^\d\s]{1,3}|[A-Z]{3})?\s*(?P<value>\d[\d,]*(?:\.\d+)?)",
        primary,
    )
    if matched is None:
        return None, None

    value = float(matched.group("value").replace(",", ""))
    currency = matched.group("currency")
    return value, currency.strip() if currency else None


def _first_sentence(text: str) -> str:
    for sentence in re.split(r"[.!?\n]", text or ""):
        normalized = " ".join(sentence.split()).strip()
        if normalized:
            return normalized
    return ""


def _normalize_item_name(item_name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", item_name.lower()).strip("_")


def _contains_any(text: str, patterns: tuple[re.Pattern[str], ...]) -> bool:
    return any(pattern.search(text) is not None for pattern in patterns)


def _normalize_sony_token(token: str) -> str:
    normalized = re.sub(r"[\s\-]+", "", token.lower())
    # normalize mark-style shorthand: a7m3/a7rm4/a7sm3/a7cm2 -> a73/a7r4/a7s3/a7c2
    normalized = re.sub(r"^a7m([2-6])$", r"a7\1", normalized)
    normalized = re.sub(r"^a7([crs])m([2-6])$", r"a7\1\2", normalized)
    normalized = re.sub(r"^a9m([23])$", r"a9\1", normalized)
    if normalized == "a1m2":
        return "a1ii"
    return normalized


def _normalize_rx_token(token: str) -> str:
    normalized = re.sub(r"[\s\-]+", "", token.lower())
    normalized = normalized.replace("mark", "")
    return normalized


def _extract_models(text: str) -> list[str]:
    matched: list[str] = []
    for model, patterns in _MODEL_PATTERNS.items():
        if _contains_any(text, patterns):
            matched.append(model)

    if "sony" in text:
        for token in _SONY_MODEL_TOKEN.findall(text):
            normalized_token = _normalize_sony_token(token)
            fallback_model = _GENERIC_SONY_ALIAS.get(normalized_token)
            # Broad alpha coverage: a5000/a5100/a6000/a6100/... should not end up unknown.
            if fallback_model is None:
                m = _SONY_FOUR_DIGIT_ALPHA.fullmatch(normalized_token)
                if m is not None:
                    fallback_model = f"sony_a{m.group(1)}"
            if fallback_model and fallback_model not in matched:
                matched.append(fallback_model)

    for token in _SONY_RX_MODEL_TOKEN.findall(text):
        normalized_token = _normalize_rx_token(token)
        fallback_model = _GENERIC_RX_ALIAS.get(normalized_token)
        if fallback_model and fallback_model not in matched:
            matched.append(fallback_model)

    matched_set = set(matched)
    for generic_model, specific_models in _GENERIC_MODEL_OVERRIDES.items():
        if generic_model in matched_set and matched_set.intersection(specific_models):
            matched_set.remove(generic_model)

    return [model for model in matched if model in matched_set]


def _target_model_from_item_name(item_name: str) -> str | None:
    normalized = _normalize_item_name(item_name)
    if normalized in _TARGET_MODEL_ALIASES:
        return _TARGET_MODEL_ALIASES[normalized]

    for model in _MODEL_PATTERNS:
        if model in normalized:
            return model
    return None


def _canonical_item_name(item_name: str, detected_model: str) -> str:
    if detected_model and detected_model != "unknown":
        return detected_model
    return _normalize_item_name(item_name)


def _detect_listing_type(text: str, matched_models: list[str]) -> str:
    if _contains_any(text, _ACCESSORY_PATTERNS):
        return "accessory"
    if matched_models and _contains_any(text, _COMPATIBILITY_PATTERNS) and not _contains_any(
        text, _CAMERA_PATTERNS
    ):
        return "accessory"
    if _contains_any(text, _LENS_PATTERNS) and not _contains_any(text, _CAMERA_PATTERNS):
        return "lens"
    if matched_models or _contains_any(text, _CAMERA_PATTERNS):
        return "camera_body"
    return "other"


def _classify_listing(listing: Listing, item_name: str) -> ListingClassification:
    combined_text = " ".join([listing.title or "", listing.description or ""]).strip().lower()
    target_model = _target_model_from_item_name(item_name)
    matched_models = _extract_models(combined_text)
    listing_type = _detect_listing_type(combined_text, matched_models)

    detected_model = "unknown"
    if target_model and target_model in matched_models:
        detected_model = target_model
    elif matched_models:
        detected_model = matched_models[0]

    non_target_models = [
        model for model in matched_models if target_model is not None and model != target_model
    ]
    is_target_exact = int(
        target_model is not None
        and target_model in matched_models
        and len(non_target_models) == 0
        and listing_type == "camera_body"
    )

    if is_target_exact:
        return ListingClassification(
            detected_model=detected_model,
            listing_type=listing_type,
            is_target_exact=1,
            confidence=0.98,
            reason=(
                f"Detected exact target camera model {target_model} in title/description "
                "without conflicting model signals."
            ),
        )

    if target_model and target_model in matched_models and listing_type != "camera_body":
        return ListingClassification(
            detected_model=detected_model,
            listing_type=listing_type,
            is_target_exact=0,
            confidence=0.92,
            reason=(
                f"Detected {listing_type} that references target model {target_model}, "
                "not a target camera body."
            ),
        )

    if matched_models:
        detected = ", ".join(matched_models)
        return ListingClassification(
            detected_model=detected_model,
            listing_type=listing_type,
            is_target_exact=0,
            confidence=0.95,
            reason=(
                f"Detected model signal(s): {detected}. "
                f"Target model is {target_model or 'unknown'}, so this listing is not an exact match."
            ),
        )

    return ListingClassification(
        detected_model=detected_model,
        listing_type=listing_type,
        is_target_exact=0,
        confidence=0.35,
        reason=(
            "Could not detect a reliable camera model in title/description; "
            "marked as non-exact by default."
        ),
    )


class MarketDataStore:
    def __init__(self: "MarketDataStore", db_path: Path) -> None:
        self.db_path = db_path
        self._schema_ready = False

    def _connect(self: "MarketDataStore") -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _ensure_listing_observation_columns(self: "MarketDataStore", conn: sqlite3.Connection) -> None:
        existing_columns = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(listing_observations)")
        }
        required_columns = {
            "detected_model": "TEXT NOT NULL DEFAULT 'unknown'",
            "listing_type": "TEXT NOT NULL DEFAULT 'other'",
            "is_target_exact": "INTEGER NOT NULL DEFAULT 0",
            "classification_confidence": "REAL",
            "classification_reason": "TEXT",
        }
        for column_name, column_sql in required_columns.items():
            if column_name in existing_columns:
                continue
            conn.execute(
                f"ALTER TABLE listing_observations ADD COLUMN {column_name} {column_sql}"
            )

    def _backfill_classification(self: "MarketDataStore", conn: sqlite3.Connection) -> None:
        rows = conn.execute(
            """
            SELECT
                id,
                marketplace,
                item_name,
                listing_id,
                title,
                post_url,
                price_text,
                location,
                seller,
                item_condition,
                description
            FROM listing_observations
            WHERE classification_reason IS NULL
            """
        ).fetchall()
        for row in rows:
            listing = Listing(
                marketplace=str(row[1] or ""),
                name=str(row[2] or ""),
                id=str(row[3] or ""),
                title=str(row[4] or ""),
                image="",
                price=str(row[6] or ""),
                post_url=str(row[5] or ""),
                location=str(row[7] or ""),
                seller=str(row[8] or ""),
                condition=str(row[9] or ""),
                description=str(row[10] or ""),
            )
            classification = _classify_listing(listing, str(row[2] or ""))
            canonical_item_name = _canonical_item_name(
                item_name=str(row[2] or ""),
                detected_model=classification.detected_model,
            )
            conn.execute(
                """
                UPDATE listing_observations
                SET
                    item_name = ?,
                    detected_model = ?,
                    listing_type = ?,
                    is_target_exact = ?,
                    classification_confidence = ?,
                    classification_reason = ?
                WHERE id = ?
                """,
                (
                    canonical_item_name,
                    classification.detected_model,
                    classification.listing_type,
                    classification.is_target_exact,
                    classification.confidence,
                    classification.reason,
                    row[0],
                ),
            )

    def reclassify_unknown_rows(self: "MarketDataStore") -> int:
        with self._connect() as conn:
            self._ensure_schema(conn)
            rows = conn.execute(
                """
                SELECT
                    id,
                    marketplace,
                    item_name,
                    listing_id,
                    title,
                    post_url,
                    price_text,
                    location,
                    seller,
                    item_condition,
                    description
                FROM listing_observations
                WHERE COALESCE(detected_model, 'unknown') = 'unknown'
                   OR classification_reason IS NULL
                """
            ).fetchall()

            updated = 0
            for row in rows:
                listing = Listing(
                    marketplace=str(row[1] or ""),
                    name=str(row[2] or ""),
                    id=str(row[3] or ""),
                    title=str(row[4] or ""),
                    image="",
                    price=str(row[6] or ""),
                    post_url=str(row[5] or ""),
                    location=str(row[7] or ""),
                    seller=str(row[8] or ""),
                    condition=str(row[9] or ""),
                    description=str(row[10] or ""),
                )
                classification = _classify_listing(listing, str(row[2] or ""))
                canonical_item_name = _canonical_item_name(
                    item_name=str(row[2] or ""),
                    detected_model=classification.detected_model,
                )
                conn.execute(
                    """
                    UPDATE listing_observations
                    SET
                        item_name = ?,
                        detected_model = ?,
                        listing_type = ?,
                        is_target_exact = ?,
                        classification_confidence = ?,
                        classification_reason = ?
                    WHERE id = ?
                    """,
                    (
                        canonical_item_name,
                        classification.detected_model,
                        classification.listing_type,
                        classification.is_target_exact,
                        classification.confidence,
                        classification.reason,
                        row[0],
                    ),
                )
                updated += 1
            conn.commit()
            return updated

    def has_observation(
        self: "MarketDataStore",
        marketplace: str,
        listing_id: str,
        availability: str | None = None,
    ) -> bool:
        with self._connect() as conn:
            self._ensure_schema(conn)
            if availability is None:
                row = conn.execute(
                    """
                    SELECT 1
                    FROM listing_observations
                    WHERE marketplace = ? AND listing_id = ?
                    LIMIT 1
                    """,
                    (marketplace, listing_id),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT 1
                    FROM listing_observations
                    WHERE marketplace = ? AND listing_id = ? AND availability = ?
                    LIMIT 1
                    """,
                    (marketplace, listing_id, availability),
                ).fetchone()
            return row is not None

    def has_non_out_observation(self: "MarketDataStore", marketplace: str, listing_id: str) -> bool:
        with self._connect() as conn:
            self._ensure_schema(conn)
            row = conn.execute(
                """
                SELECT 1
                FROM listing_observations
                WHERE marketplace = ? AND listing_id = ? AND availability != 'out'
                LIMIT 1
                """,
                (marketplace, listing_id),
            ).fetchone()
            return row is not None

    def get_latest_listing_snapshot(
        self: "MarketDataStore", marketplace: str, listing_id: str
    ) -> Listing | None:
        with self._connect() as conn:
            self._ensure_schema(conn)
            row = conn.execute(
                """
                SELECT
                    marketplace,
                    item_name,
                    listing_id,
                    title,
                    post_url,
                    price_text,
                    location,
                    seller,
                    item_condition,
                    description
                FROM listing_observations
                WHERE marketplace = ? AND listing_id = ?
                ORDER BY observed_at DESC
                LIMIT 1
                """,
                (marketplace, listing_id),
            ).fetchone()
            if row is None:
                return None

            return Listing(
                marketplace=str(row[0] or ""),
                name=str(row[1] or ""),
                id=str(row[2] or ""),
                title=str(row[3] or ""),
                image="",
                post_url=str(row[4] or ""),
                price=str(row[5] or ""),
                location=str(row[6] or ""),
                seller=str(row[7] or ""),
                condition=str(row[8] or ""),
                description=str(row[9] or ""),
            )

    def _ensure_schema(self: "MarketDataStore", conn: sqlite3.Connection) -> None:
        if self._schema_ready:
            return

        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS listing_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at TEXT NOT NULL,
                marketplace TEXT NOT NULL,
                item_name TEXT NOT NULL,
                search_city TEXT NOT NULL,
                search_phrase TEXT NOT NULL,
                listing_id TEXT NOT NULL,
                post_url TEXT NOT NULL,
                title TEXT NOT NULL,
                price_text TEXT NOT NULL,
                price_value REAL,
                currency TEXT,
                location TEXT,
                seller TEXT,
                item_condition TEXT,
                description TEXT,
                detected_model TEXT NOT NULL DEFAULT 'unknown',
                listing_type TEXT NOT NULL DEFAULT 'other',
                is_target_exact INTEGER NOT NULL DEFAULT 0,
                classification_confidence REAL,
                classification_reason TEXT,
                availability TEXT NOT NULL,
                sold_estimated_at TEXT,
                sold_time_method TEXT,
                UNIQUE(observed_at, marketplace, listing_id, availability)
            );

            CREATE INDEX IF NOT EXISTS idx_listing_obs_lookup
                ON listing_observations(item_name, marketplace, search_city, availability);

            CREATE INDEX IF NOT EXISTS idx_listing_obs_sold_time
                ON listing_observations(sold_estimated_at, observed_at);

            CREATE TABLE IF NOT EXISTS market_price (
                item_name TEXT NOT NULL,
                marketplace TEXT NOT NULL,
                search_city TEXT NOT NULL,
                window_days INTEGER NOT NULL,
                sample_size INTEGER NOT NULL,
                msrp_estimate REAL,
                currency TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (item_name, marketplace, search_city, window_days)
            );
            """
        )
        self._ensure_listing_observation_columns(conn)
        self._backfill_classification(conn)
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_listing_obs_target_exact
            ON listing_observations(item_name, marketplace, search_city, availability, is_target_exact);
            """
        )
        conn.commit()
        self._schema_ready = True

    def _estimate_sold_time(
        self: "MarketDataStore",
        conn: sqlite3.Connection,
        marketplace: str,
        listing_id: str,
        observed_at: str,
    ) -> Tuple[str, str]:
        existing = conn.execute(
            """
            SELECT sold_estimated_at, sold_time_method
            FROM listing_observations
            WHERE marketplace = ? AND listing_id = ? AND availability = 'out'
              AND sold_estimated_at IS NOT NULL
            ORDER BY observed_at ASC
            LIMIT 1
            """,
            (marketplace, listing_id),
        ).fetchone()
        if existing is not None:
            return str(existing[0]), str(existing[1] or "first_seen_out_of_stock")

        last_active = conn.execute(
            """
            SELECT observed_at
            FROM listing_observations
            WHERE marketplace = ? AND listing_id = ? AND availability != 'out'
            ORDER BY observed_at DESC
            LIMIT 1
            """,
            (marketplace, listing_id),
        ).fetchone()
        if last_active is None:
            return observed_at, "first_seen_out_of_stock"

        last_active_ts = _parse_datetime(str(last_active[0]))
        first_sold_ts = _parse_datetime(observed_at)
        midpoint = last_active_ts + (first_sold_ts - last_active_ts) / 2
        return midpoint.isoformat(), "midpoint_last_active_and_first_sold"

    def record_observation(
        self: "MarketDataStore",
        listing: Listing,
        item_name: str,
        search_city: str,
        search_phrase: str,
        availability: str,
    ) -> None:
        observed_at = _utc_now_iso()
        price_value, currency = _parse_price(listing.price)
        classification = _classify_listing(listing, item_name)
        canonical_item_name = _canonical_item_name(
            item_name=item_name,
            detected_model=classification.detected_model,
        )

        with self._connect() as conn:
            self._ensure_schema(conn)

            sold_estimated_at = None
            sold_time_method = None
            if availability == "out":
                sold_estimated_at, sold_time_method = self._estimate_sold_time(
                    conn=conn,
                    marketplace=listing.marketplace,
                    listing_id=listing.id,
                    observed_at=observed_at,
                )

            conn.execute(
                """
                INSERT OR IGNORE INTO listing_observations (
                    observed_at,
                    marketplace,
                    item_name,
                    search_city,
                    search_phrase,
                    listing_id,
                    post_url,
                    title,
                    price_text,
                    price_value,
                    currency,
                    location,
                    seller,
                    item_condition,
                    description,
                    detected_model,
                    listing_type,
                    is_target_exact,
                    classification_confidence,
                    classification_reason,
                    availability,
                    sold_estimated_at,
                    sold_time_method
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    observed_at,
                    listing.marketplace,
                    canonical_item_name,
                    search_city,
                    search_phrase,
                    listing.id,
                    listing.post_url.split("?")[0],
                    listing.title,
                    listing.price,
                    price_value,
                    currency,
                    listing.location,
                    listing.seller,
                    listing.condition,
                    listing.description,
                    classification.detected_model,
                    classification.listing_type,
                    classification.is_target_exact,
                    classification.confidence,
                    classification.reason,
                    availability,
                    sold_estimated_at,
                    sold_time_method,
                ),
            )
            conn.commit()

    def refresh_market_price(
        self: "MarketDataStore",
        item_name: str,
        marketplace: str,
        search_city: str,
        window_days: int = 30,
    ) -> Tuple[int, Optional[float], Optional[str]]:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        cutoff = (now - timedelta(days=window_days)).isoformat()

        with self._connect() as conn:
            self._ensure_schema(conn)

            row = conn.execute(
                """
                WITH latest_listing_state AS (
                    SELECT
                        listing_id,
                        availability,
                        observed_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY listing_id
                            ORDER BY observed_at DESC
                        ) AS rn
                    FROM listing_observations
                    WHERE marketplace = ?
                      AND search_city = ?
                ),
                latest_sold_row_per_listing AS (
                    SELECT
                        o.listing_id,
                        o.price_value,
                        o.currency,
                        o.observed_at,
                        o.sold_estimated_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY o.listing_id
                            ORDER BY o.observed_at DESC
                        ) AS rn
                    FROM listing_observations o
                    JOIN latest_listing_state s
                      ON s.listing_id = o.listing_id
                     AND s.rn = 1
                     AND s.availability = 'out'
                    WHERE o.marketplace = ?
                      AND o.search_city = ?
                      AND o.availability = 'out'
                      AND COALESCE(o.sold_estimated_at, o.observed_at) >= ?
                      AND o.price_value IS NOT NULL
                      AND COALESCE(o.listing_type, 'other') = 'camera_body'
                      AND o.detected_model = ?
                )
                SELECT COUNT(price_value), AVG(price_value)
                FROM latest_sold_row_per_listing
                WHERE rn = 1
                """,
                (
                    marketplace,
                    search_city,
                    marketplace,
                    search_city,
                    cutoff,
                    item_name,
                ),
            ).fetchone()

            sample_size = int((row[0] if row is not None else 0) or 0)
            msrp_estimate = (
                float(row[1]) if row is not None and row[1] is not None else None
            )

            currency_row = conn.execute(
                """
                WITH latest_listing_state AS (
                    SELECT
                        listing_id,
                        availability,
                        observed_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY listing_id
                            ORDER BY observed_at DESC
                        ) AS rn
                    FROM listing_observations
                    WHERE marketplace = ?
                      AND search_city = ?
                ),
                latest_sold_row_per_listing AS (
                    SELECT
                        o.listing_id,
                        o.currency,
                        o.observed_at,
                        o.sold_estimated_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY o.listing_id
                            ORDER BY o.observed_at DESC
                        ) AS rn
                    FROM listing_observations o
                    JOIN latest_listing_state s
                      ON s.listing_id = o.listing_id
                     AND s.rn = 1
                     AND s.availability = 'out'
                    WHERE o.marketplace = ?
                      AND o.search_city = ?
                      AND o.availability = 'out'
                      AND COALESCE(o.sold_estimated_at, o.observed_at) >= ?
                      AND COALESCE(o.listing_type, 'other') = 'camera_body'
                      AND o.detected_model = ?
                )
                SELECT currency, COUNT(*) AS cnt
                FROM latest_sold_row_per_listing
                WHERE rn = 1
                  AND currency IS NOT NULL
                GROUP BY currency
                ORDER BY cnt DESC, currency ASC
                LIMIT 1
                """,
                (
                    marketplace,
                    search_city,
                    marketplace,
                    search_city,
                    cutoff,
                    item_name,
                ),
            ).fetchone()
            currency = str(currency_row[0]) if currency_row is not None else None

            conn.execute(
                """
                INSERT INTO market_price (
                    item_name,
                    marketplace,
                    search_city,
                    window_days,
                    sample_size,
                    msrp_estimate,
                    currency,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(item_name, marketplace, search_city, window_days)
                DO UPDATE SET
                    sample_size = excluded.sample_size,
                    msrp_estimate = excluded.msrp_estimate,
                    currency = excluded.currency,
                    updated_at = excluded.updated_at
                """,
                (
                    item_name,
                    marketplace,
                    search_city,
                    window_days,
                    sample_size,
                    msrp_estimate,
                    currency,
                    now.isoformat(),
                ),
            )
            conn.commit()

        return sample_size, msrp_estimate, currency


_market_data_store: MarketDataStore | None = None


def get_market_data_store() -> MarketDataStore:
    global _market_data_store
    if _market_data_store is None:
        _market_data_store = MarketDataStore(amm_home / "market_data.db")
    return _market_data_store
