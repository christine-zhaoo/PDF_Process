#!/usr/bin/env python3
"""
step3_pdf_quality_check.py — SELF-CONTAINED. No other .py file needed
(other than pipeline_config.py for shared settings).

Reads every PDF under a folder/GCS prefix and writes one QC row per file to a
partitioned BigQuery table (see pipeline_config.py's BQ_TABLE_QUALITY):

    <GCP_PROJECT_ID>.<BQ_DATASET>.pdf_quality

overall_quality (clear/unclear/totally_unreadable) and recommended_route
(pixel/vision/fallback) are DIFFERENT questions - see the ROUTING section
below.

DEPENDS ON step2's BIGQUERY TABLE, NOT ITS .py FILE
---------------------------------------------------------
This script is self-contained code-wise (no import of step2_pdf_
calibration.py or any other local file - all geometry/ink logic needed for
the SELF-MEASURED fallback is duplicated inline below). But its PRIMARY
input for each file's measurements is step2's BigQuery output table,
pdf_calibration_profile: for every file, this script first queries that
table by file_name for the already-measured box rects, ink threshold and
registration-mark geometry, and uses those directly instead of re-deriving
them.

If a file has no row there (step2 hasn't been run on it yet, or its run
failed), this script prints an explicit [ALERT] and falls back to
measuring that one file itself from scratch, using its own inlined copy of
the same measurement code - so a pdf_quality row always gets written, never
silently skipped, and never guessed. Which path was taken is recorded per
row in calibration_source ('pdf_calibration_profile' or 'self-measured') and
noted in error_reasons when the fallback was used.

Because of this, run step2_pdf_calibration.py FIRST for best results - not
because this script cannot run without it (it can), but because every file
it falls back on re-measures work step2 already did.

PARTITIONING
------------
Each row's `partition_date` is the DATE parsed from the PDF's parent folder
name (e.g. "Nov 10 2025", "Nov10_2025", "2025-11-10"). A file with no parent
folder gets today's date (this run's date) as its partition date. The table
is DATE-partitioned on this column.

USAGE
-----
    import step3_pdf_quality_check as step3
    df = step3.run("gs://tps_survey/TPS_Scanned_2025_Reorgnized/",
                    vision=True)

    # local folder, print only, don't touch BigQuery
    df = step3.run("./pdfs", dry_run=True)
"""
import datetime as _dt
import json
import os
import re
from pathlib import Path

import pipeline_config

RENDER_DPI = 300
BQ_TABLE = f"{pipeline_config.GCP_PROJECT_ID}.{pipeline_config.BQ_DATASET}.{pipeline_config.BQ_TABLE_QUALITY}"
CLASSIFIER_VERSION = "notebook_2-1.0"

# --- measured thresholds; see revision docs for each one's derivation ---
TILT_DEG = 0.5
FAINT_MARK_FLOOR = 0.30
INK_MARK_STYLE_BOUNDARY = FAINT_MARK_FLOOR
INK_GAP_CLEAR = 0.08
BORDERS_READABLE_RATE = 0.95
FAINT_BORDER_COV = 0.70
SHADOW_COVERAGE = 0.01
SHADOW_MIN_BANDS = 6
MAX_TRUSTED_RESIDUAL = 8.0
VISION_ROUTE_MIN_FLAGS = 5

VISION_MODEL = pipeline_config.STEP3_VISION_MODEL
# PLACEHOLDER - set this before relying on the fallback route.
FALLBACK_VISION_MODEL = None

# Q27's first layout is absent from every reference file, so it is excluded
# from the borders-readable denominator.
_ABSENT_LAYOUTS = ("27#0",) if not pipeline_config.FORM_CONTROL_CALIBRATION else ()

# ==========================================================================
# calibration baseline — measured box geometry, identical to step2_pdf_
# calibration.py's own copy. Deliberately duplicated (not imported from
# step2) rather than shared: this describes the form's PHYSICAL print
# layout, which pipeline_config.py's own docstring explains is intentionally
# kept out of the shared config module (see its "WHAT'S DELIBERATELY *NOT*
# HERE" section) - step2/step3 each measure/consume this geometry
# independently rather than one depending on the other's internals.
# ==========================================================================
BASELINE_MARKS = {
    0: ((184.0, 223.0), (2384.0, 224.0), (176.0, 3120.0), (2374.0, 3119.0)),
    1: ((193.0, 211.0), (2380.0, 209.0), (200.0, 3107.0), (2377.0, 3106.0)),
}
BASELINE_MARKS = (
    pipeline_config.FORM_BASELINE_MARKS
    if pipeline_config.FORM_PROFILE_CONFIGURED else BASELINE_MARKS
)
INK_THRESHOLD = pipeline_config.FORM_INK_THRESHOLD
GRID_COLUMN_CENTERS = (
    pipeline_config.FORM_GRID_COLUMN_CENTERS
    if pipeline_config.FORM_PROFILE_CONFIGURED
    else (1661.75, 1790.4, 1919.6, 2049.9, 2179.6, 2309.1)
)

YESNO_BOX_CALIBRATION = {
    "19": (0, {
        "None": (658, 704, 2446, 2490), "Very little": (882, 928, 2446, 2490),
        "About half": (1168, 1214, 2446, 2490), "Almost all": (1471, 1516, 2446, 2490),
        "All": (1765, 1811, 2446, 2490),
    }),
    "20": (0, {
        "Much better": (659, 703, 2554, 2598), "Somewhat better": (987, 1031, 2554, 2598),
        "About the same": (1400, 1444, 2554, 2598), "Somewhat worse": (1792, 1837, 2554, 2598),
        "N/A": (2206, 2251, 2554, 2598),
    }),
    "21": (0, {"Yes": (1421, 1465, 2657, 2703), "No": (1701, 1747, 2657, 2702)}),
    "22": (0, {"Yes": (1421, 1465, 2728, 2772), "No": (1701, 1747, 2728, 2774)}),
    "23": (0, {
        "Strongly Agree": (584, 631, 2840, 2886), "Agree": (962, 1008, 2840, 2886),
        "I am Neutral": (1196, 1243, 2840, 2886), "Disagree": (1532, 1578, 2840, 2886),
        "Strongly Disagree": (1815, 1862, 2840, 2886), "N/A": (2240, 2287, 2840, 2886),
    }),
    "25": (1, {
        "First visit/day": (272, 305, 1101, 1133), "2 weeks or less": (272, 305, 1147, 1179),
        "More than 2 weeks but less than 4 weeks": (272, 305, 1195, 1225),
        "4 weeks or more": (272, 305, 1240, 1271),
    }),
    "27": [
        (1, {"Yes": (1355, 1390, 1595, 1630), "No": (1708, 1743, 1595, 1630)}),
        (1, {"Yes": (274, 306, 1538, 1571), "No": (560, 595, 1538, 1572)}),
    ],
    "28": (1, {
        "Yes, I am currently receiving Contingency Management services": (273, 306, 1732, 1764),
        "Yes, I received Contingency Management services in the past": (273, 306, 1851, 1884),
        "No, I have never received Contingency Management services": (273, 306, 1962, 1995),
    }),
    "29": (1, {
        "Male": (274, 307, 2295, 2327), "Female": (274, 307, 2343, 2376),
        "Female-to-Male (FTM)/Transgender Male/Trans Man": (274, 307, 2388, 2420),
        "Male-to-Female (MTF)/Transgender Female/Trans Woman": (274, 307, 2434, 2464),
        "Gender Queer/Gender Non-Conforming": (274, 307, 2483, 2512),
        "Other (specify)": (274, 307, 2543, 2571), "Prefer not to state": (275, 308, 2618, 2649),
    }),
    "30": (1, {
        "Female": (284, 312, 2776, 2804), "Male": (785, 818, 2773, 2806),
        "Other (specify)": (284, 312, 2825, 2853), "Prefer not to state": (785, 818, 2822, 2855),
    }),
    "31": (1, {
        "Heterosexual/Straight": (1352, 1386, 1028, 1062), "Lesbian (Female)": (1352, 1386, 1076, 1110),
        "Gay (Male)": (1352, 1387, 1122, 1156), "Bisexual": (1352, 1386, 1172, 1206),
        "Unsure/Questioning/Don't know": (1352, 1386, 1219, 1253), "Pansexual": (1858, 1893, 1028, 1061),
        "Asexual": (1858, 1893, 1074, 1108), "Other (specify)": (1858, 1893, 1121, 1155),
        "Queer": (1858, 1893, 1171, 1205), "Prefer not to state": (1858, 1893, 1219, 1253),
    }),
    "32": (1, {
        "Yes": (1354, 1389, 1453, 1486), "No": (1647, 1680, 1453, 1486), "Unknown": (1900, 1931, 1453, 1486),
    }),
    "35": (1, {
        "Post-release Community Supervision (AB109) or on Probation from any federal, state, or local jurisdiction": (1354, 1388, 2642, 2675),
        "Awaiting trial, charges or sentencing": (1354, 1388, 2750, 2779),
        "On parole from any other jurisdiction": (1354, 1388, 2805, 2838),
        "Any other criminal justice involvement": (1354, 1388, 2858, 2891),
        "No criminal justice involvement": (1354, 1388, 2912, 2945),
    }),
}

MULTISELECT_BOX_CALIBRATION = {
    "33": (1, {
        "American Indian/Alaskan Native": (1357, 1392, 1618, 1652), "Asian": (1357, 1392, 1671, 1705),
        "Black/African American": (1357, 1392, 1720, 1755),
        "Native Hawaiian/Pacific Islander": (1357, 1392, 1773, 1807),
        "White/Caucasian": (1357, 1392, 1825, 1859), "Other (specify)": (1357, 1392, 1876, 1910),
        "Prefer not to state": (1357, 1392, 1958, 1992),
    }),
    "34": (1, {
        "Physically Disabled": (1360, 1388, 2121, 2149), "Visually Impaired/Blind": (1360, 1388, 2169, 2196),
        "Hearing Impaired/Deaf": (1360, 1389, 2217, 2245),
        "Co-occurring Mental Health Condition": (1360, 1389, 2270, 2299),
        "Developmentally or Intellectually Disabled": (1360, 1389, 2321, 2349),
        "Other (specify)": (1360, 1389, 2389, 2418), "None": (1360, 1389, 2444, 2473),
    }),
}
if pipeline_config.FORM_PROFILE_CONFIGURED:
    configured_yesno = {}
    configured_multiselect = {}
    for item in pipeline_config.FORM_CONTROL_CALIBRATION:
        if item["control_type"] != "checkbox":
            continue
        question = item["question"]
        page = item["page"]
        label = item["label"]
        rect = item["rect"]
        if question in pipeline_config.MULTI_SELECT_QUESTION_NUMBERS:
            entry = configured_multiselect.setdefault(question, (page, {}))
            if entry[0] != page:
                raise ValueError(f"Multi-select question {question!r} spans multiple pages")
            entry[1][label] = rect
        else:
            layout = str(item["layout"])
            configured_yesno.setdefault(question, {}).setdefault(
                layout, (page, {})
            )[1][label] = rect

    YESNO_BOX_CALIBRATION = {}
    for question, layouts in configured_yesno.items():
        candidates = [
            calibration
            for _layout, calibration in sorted(layouts.items(), key=lambda entry: entry[0])
        ]
        YESNO_BOX_CALIBRATION[question] = (
            candidates if len(candidates) > 1 else candidates[0]
        )
    MULTISELECT_BOX_CALIBRATION = configured_multiselect
QUESTION_BASELINE_CORRECTION = (
    {}
    if pipeline_config.FORM_PROFILE_CONFIGURED
    else {"31": (0.0, 23.0), "33": (0.0, 17.0), "34": (0.0, 16.0)}
)
SEARCH_BOUND = 22
REG_MARK_SIZE, REG_MARK_SIZE_TOL, REG_MARK_FILL = 74, 28, 0.75
CONFIRM_PAD = 8
MIN_PLAUSIBLE_MARKED, MAX_PLAUSIBLE_MARKED, CLEAR_GAP = 8, 25, 0.08


def all_box_questions():
    out = []
    for q, raw in YESNO_BOX_CALIBRATION.items():
        cands = raw if isinstance(raw, list) else [raw]
        for ci, (pg, boxes) in enumerate(cands):
            out.append((q, pg, ci, boxes, "yesno_box"))
    for q, (pg, boxes) in MULTISELECT_BOX_CALIBRATION.items():
        out.append((q, pg, 0, boxes, "multiselect"))
    return out


# ==========================================================================
# folder-name -> partition date (identical logic to notebook_1)
# ==========================================================================
_MONTHS = {m.lower(): i for i, m in enumerate(
    ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]) if m}
_MONTHS.update({
    "january": 1, "february": 2, "march": 3, "april": 4, "june": 6, "july": 7,
    "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
})


def parse_folder_date(folder_name, default=None):
    """Best-effort date parse from a folder name; `default` (typically
    today's date) when nothing recognisable is found - see notebook_1's
    identical function for the accepted formats."""
    if not folder_name:
        return default
    s = folder_name.strip()
    m = re.search(r"(\d{4})[-_/]?(\d{1,2})[-_/](\d{1,2})\b", s)
    if m:
        try:
            return _dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass
    m = re.search(
        r"([A-Za-z]{3,9})\.?\s*[_ ]?(\d{1,2})(?:(?:st|nd|rd|th)?)[,_ ]*\s*(\d{4})?", s)
    if m:
        mon = _MONTHS.get(m.group(1).lower())
        if mon:
            day = int(m.group(2))
            year = int(m.group(3)) if m.group(3) else (default or _dt.date.today()).year
            try:
                return _dt.date(year, mon, day)
            except ValueError:
                pass
    return default


# ==========================================================================
# geometry (identical to notebook_1 - duplicated on purpose, no cross-import)
# ==========================================================================
def _binarize(png_bytes):
    import cv2
    import numpy as np
    arr = np.frombuffer(png_bytes, dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_GRAYSCALE)
    if img is None:
        return None, None
    _, binary = cv2.threshold(img, INK_THRESHOLD, 255, cv2.THRESH_BINARY_INV)
    return img, binary


def _group(positions, max_gap=3):
    if not positions:
        return []
    groups, cur = [], [positions[0]]
    for v in positions[1:]:
        if v - cur[-1] <= max_gap:
            cur.append(v)
        else:
            groups.append(cur)
            cur = [v]
    groups.append(cur)
    return [int(sum(g) / len(g)) for g in groups]


def detect_registration_marks(binary):
    import cv2
    n, _lab, stats, _c = cv2.connectedComponentsWithStats(binary, 8)
    H, W = binary.shape
    lo, hi = REG_MARK_SIZE - REG_MARK_SIZE_TOL, REG_MARK_SIZE + REG_MARK_SIZE_TOL
    found = []
    for i in range(1, n):
        x, y, w, h, area = stats[i]
        if not (lo <= w <= hi and lo <= h <= hi):
            continue
        if abs(w - h) > REG_MARK_SIZE_TOL or area < REG_MARK_FILL * w * h:
            continue
        found.append((x + w / 2.0, y + h / 2.0))
    quads = {}
    for cx, cy in found:
        key = (0 if cy < H / 2 else 1, 0 if cx < W / 2 else 1)
        quads.setdefault(key, []).append((cx, cy))
    if any(len(quads.get(k, [])) != 1 for k in ((0, 0), (0, 1), (1, 0), (1, 1))):
        return None
    return (quads[(0, 0)][0], quads[(0, 1)][0], quads[(1, 0)][0], quads[(1, 1)][0])


def similarity_transform(src_marks, dst_marks):
    import numpy as np
    if not src_marks or not dst_marks:
        return None
    s = np.array(src_marks, float)
    d = np.array(dst_marks, float)
    cs, cd = s.mean(0), d.mean(0)
    a = (s - cs)[:, 0] + 1j * (s - cs)[:, 1]
    b = (d - cd)[:, 0] + 1j * (d - cd)[:, 1]
    denom = (np.abs(a) ** 2).sum()
    if denom == 0:
        return None
    z = (np.conj(a) * b).sum() / denom
    return z, cs, cd


def _apply(xf, x, y):
    z, cs, cd = xf
    w = z * complex(x - cs[0], y - cs[1])
    return w.real + cd[0], w.imag + cd[1]


def transform_residual(xf, src_marks, dst_marks):
    import numpy as np
    pred = [_apply(xf, p[0], p[1]) for p in src_marks]
    return float(np.abs(np.array(pred) - np.array(dst_marks, float)).max())


def locate_box(binary, y0, y1, x0, x1, expected_w, expected_h):
    import cv2
    H, W = binary.shape
    cy0, cy1 = max(0, int(y0)), min(H, int(y1))
    cx0, cx1 = max(0, int(x0)), min(W, int(x1))
    win = binary[cy0:cy1, cx0:cx1]
    if win.size == 0:
        return None
    vk = cv2.getStructuringElement(cv2.MORPH_RECT, (1, 20))
    hk = cv2.getStructuringElement(cv2.MORPH_RECT, (20, 1))
    vert = cv2.dilate(cv2.erode(win, vk), vk)
    horiz = cv2.dilate(cv2.erode(win, hk), hk)
    cols = _group([x for x in range(win.shape[1]) if vert[:, x].sum() / 255 > 16])
    rows = _group([y for y in range(win.shape[0]) if horiz[y, :].sum() / 255 > 16])
    if len(cols) < 2 or len(rows) < 2:
        return None

    def pairs(g):
        return [(g[i], g[j]) for i in range(len(g)) for j in range(i + 1, len(g))
                if 18 <= g[j] - g[i] <= 55]

    wh, ww = win.shape
    cp = [(a, b) for a, b in pairs(cols) if a != 0 and b != ww - 1]
    rp = [(a, b) for a, b in pairs(rows) if a != 0 and b != wh - 1]
    if not cp or not rp:
        return None
    best, best_score = None, None
    for lx, rx in cp:
        for ty, by in rp:
            bx0, bx1, by0, by1 = lx + cx0, rx + cx0, ty + cy0, by + cy0
            if not (0 <= by0 < by1 <= H and 0 <= bx0 < bx1 <= W):
                continue
            sides = [binary[by0, bx0:bx1], binary[by1 - 1, bx0:bx1],
                     binary[by0:by1, bx0], binary[by0:by1, bx1 - 1]]
            if any(float((s > 0).sum()) / max(len(s), 1) < 0.6 for s in sides):
                continue
            score = abs((rx - lx) - expected_w) + abs((by - ty) - expected_h)
            if best_score is None or score < best_score:
                best_score, best = score, (bx0, bx1, by0, by1)
    return best


def ink_ratio(binary, box, border=2):
    x0, x1, y0, y1 = box
    crop = binary[y0:y1, x0:x1]
    h, w = crop.shape
    if h <= 2 * border or w <= 2 * border:
        return 0.0
    return float(crop[border:h - border, border:w - border].mean()) / 255.0


def render_pages(pdf_bytes, dpi=RENDER_DPI):
    import pymupdf
    doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
    try:
        out = []
        for page in doc:
            pm = page.get_pixmap(dpi=dpi)
            img, binary = _binarize(pm.tobytes("png"))
            out.append({"gray": img, "binary": binary, "w": pm.width, "h": pm.height,
                        "rotation": page.rotation,
                        "w_mm": round(page.rect.width * 25.4 / 72.0, 1),
                        "h_mm": round(page.rect.height * 25.4 / 72.0, 1)})
        return out
    finally:
        doc.close()


def _split_clusters(ratios):
    import numpy as np
    r = np.array(sorted(ratios), float)
    n = len(r)
    if n < 12:
        return None
    lo_k = max(1, n - MAX_PLAUSIBLE_MARKED)
    hi_k = max(lo_k + 1, n - MIN_PLAUSIBLE_MARKED)
    hi_k = min(hi_k, n - 1)
    if hi_k < lo_k:
        return None
    best_gap, best_k = -1.0, lo_k
    for k in range(lo_k, hi_k + 1):
        gap = float(r[k] - r[k - 1])
        if gap > best_gap:
            best_gap, best_k = gap, k
    blank_hi, mark_lo = float(r[best_k - 1]), float(r[best_k])
    n_marked = n - best_k
    quality = ("clear" if best_gap >= CLEAR_GAP
               and MIN_PLAUSIBLE_MARKED <= n_marked <= MAX_PLAUSIBLE_MARKED else "tight")
    return {"threshold": round((blank_hi + mark_lo) / 2.0, 4),
            "blank_hi": round(blank_hi, 4), "mark_lo": round(mark_lo, 4),
            "gap": round(best_gap, 4), "n_marked": n_marked, "quality": quality}


def profile_pdf_bytes(pdf_bytes, name="(bytes)", source=None, dpi=RENDER_DPI):
    """Same measurement as notebook_1.profile_pdf_bytes() (duplicated on
    purpose - see module docstring). Used here only as the geometry/ink input
    to the QC measurements below; not itself written to BigQuery."""
    import numpy as np
    pages = render_pages(pdf_bytes, dpi=dpi)
    result = {"file": name, "source": source or name, "page_count": len(pages),
              "pages": [], "questions": {}, "warnings": []}
    transforms = {}
    for idx, pg in enumerate(pages):
        marks = detect_registration_marks(pg["binary"]) if pg["binary"] is not None else None
        entry = {"page": idx + 1, "paper": f'{pg["w_mm"]}x{pg["h_mm"]}mm',
                 "rotation": pg["rotation"], "px": f'{pg["w"]}x{pg["h"]}',
                 "marks_found": marks is not None,
                 "paper_level": round(float(np.median(pg["gray"])), 1) if pg["gray"] is not None else None}
        base = BASELINE_MARKS.get(idx)
        if marks and base:
            xf = similarity_transform(base, marks)
            if xf:
                transforms[idx] = xf
                z, _cs, _cd = xf
                entry["scale"] = round(float(abs(z)), 5)
                entry["rotation_deg"] = round(float(np.degrees(np.angle(z))), 3)
                entry["fit_residual_px"] = round(transform_residual(xf, base, marks), 2)
        else:
            result["warnings"].append(f"p{idx+1}: registration marks not found")
        result["pages"].append(entry)

    ratios = []
    for q, pg_idx, cand_idx, boxes, kind in all_box_questions():
        if pg_idx not in transforms or pg_idx >= len(pages):
            continue
        xf = transforms[pg_idx]
        binary = pages[pg_idx]["binary"]
        cdx, cdy = QUESTION_BASELINE_CORRECTION.get(q, (0.0, 0.0))
        confirmed, rects, q_ratios = 0, {}, []
        for label, (x0, x1, y0, y1) in boxes.items():
            ax0, ay0 = _apply(xf, x0 + cdx, y0 + cdy)
            ax1, ay1 = _apply(xf, x1 + cdx, y1 + cdy)
            got = locate_box(binary, ay0 - CONFIRM_PAD, ay1 + CONFIRM_PAD,
                             ax0 - CONFIRM_PAD, ax1 + CONFIRM_PAD, x1 - x0, y1 - y0)
            if got:
                confirmed += 1
                rects[label] = [int(v) for v in got]
                r = ink_ratio(binary, got)
                q_ratios.append(r)
                ratios.append(r)
            else:
                rects[label] = [int(ax0), int(ax1), int(ay0), int(ay1)]
        key = f"{q}#{cand_idx}" if isinstance(YESNO_BOX_CALIBRATION.get(q), list) else q
        result["questions"][key] = {
            "question": q, "page_idx": pg_idx, "boxes_total": len(boxes),
            "boxes_confirmed": confirmed, "rects": rects,
        }
    result["ink"] = _split_clusters(ratios) if ratios else None
    result["boxes_confirmed"] = sum(v["boxes_confirmed"] for v in result["questions"].values())
    result["boxes_total"] = sum(v["boxes_total"] for v in result["questions"].values())
    return result


# ==========================================================================
# pdf_calibration_profile (notebook 1's BQ table) — PRIMARY input for measure()
# ==========================================================================
CALIBRATION_TABLE = f"{pipeline_config.GCP_PROJECT_ID}.{pipeline_config.BQ_DATASET}.{pipeline_config.BQ_TABLE_CALIBRATION}"


def fetch_calibration_row(file_name, table=CALIBRATION_TABLE, project=None, client=None):
    """The newest pdf_calibration_profile row for this file_name, or None if
    no row exists. A read failure (missing table, no BQ access, etc.) is
    also treated as "no row" - the caller alerts and falls back either way;
    it must never be mistaken for a clean, empty result."""
    from google.cloud import bigquery
    project = project or table.split(".")[0]
    if client is False:
        # A run()-level client build already failed for this whole sweep -
        # don't retry construction per file, just report unavailable.
        print(f"[ALERT] no BigQuery client available - cannot look up {file_name!r} "
             f"in {table} - falling back to self-sufficient measurement")
        return None
    try:
        client = client or bigquery.Client(project=project)
        query = (
            f"SELECT * FROM `{table}` WHERE file_name = @file_name "
            f"ORDER BY profiled_at DESC LIMIT 1"
        )
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("file_name", "STRING", file_name)])
        rows = list(client.query(query, job_config=job_config).result())
    except Exception as e:                                      # noqa: BLE001
        # Covers client construction failing (no ADC/credentials) as well as
        # the query itself (missing table, no access, network) - either way
        # this is "no row available", never an uncaught crash.
        print(f"[ALERT] could not query {table} for {file_name!r} ({e}) "
              "- falling back to self-sufficient measurement")
        return None
    if not rows:
        return None
    return dict(rows[0].items())


def profile_from_bq_row(bq_row, pages, name):
    """Reconstructs the same shape profile_pdf_bytes() returns, from a
    pdf_calibration_profile row plus this file's OWN freshly rendered pages
    (pixel data is not stored in BigQuery - only the rects/thresholds are, so
    rendering the PDF is still required to read border coverage, shadow ink
    and grid overflow against those rects)."""
    import json as _json
    result = {"file": name, "source": bq_row.get("gcs_uri") or name,
              "page_count": bq_row.get("page_count") or len(pages),
              "pages": [], "questions": {}, "warnings": []}

    marks_found_pages = (bq_row.get("marks_found_pages") or "").split("|")
    rot = {0: bq_row.get("rotation_deg_p1"), 1: bq_row.get("rotation_deg_p2")}
    resid = {0: bq_row.get("fit_residual_p1"), 1: bq_row.get("fit_residual_p2")}
    for idx, pg in enumerate(pages):
        found = (marks_found_pages[idx] == "yes") if idx < len(marks_found_pages) else None
        result["pages"].append({
            "page": idx + 1, "paper": f'{pg["w_mm"]}x{pg["h_mm"]}mm',
            "marks_found": found if found is not None else False,
            "rotation_deg": rot.get(idx), "fit_residual_px": resid.get(idx),
        })

    try:
        qj = _json.loads(bq_row.get("question_rects_json") or "{}")
    except Exception:                                           # noqa: BLE001
        qj = {}
    for key, e in qj.items():
        result["questions"][key] = {
            "question": e.get("question", key.split("#")[0]), "page_idx": e.get("page_idx", 0),
            "boxes_total": e.get("boxes_total", 0), "boxes_confirmed": e.get("boxes_confirmed", 0),
            "rects": e.get("rects", {}),
        }

    result["ink"] = ({"threshold": bq_row.get("ink_threshold"), "blank_hi": bq_row.get("ink_blank_hi"),
                      "mark_lo": bq_row.get("ink_mark_lo"), "gap": bq_row.get("ink_gap"),
                      "n_marked": bq_row.get("ink_n_marked"), "quality": bq_row.get("ink_quality")}
                     if bq_row.get("ink_quality") is not None else None)
    result["boxes_confirmed"] = bq_row.get("boxes_confirmed") or 0
    result["boxes_total"] = bq_row.get("boxes_total") or 0
    if bq_row.get("warnings"):
        result["warnings"] = str(bq_row["warnings"]).split(" | ")
    return result


# ==========================================================================
# QC measured fields
# ==========================================================================
def _border_coverage(binary, box):
    import numpy as np
    x0, x1, y0, y1 = (int(v) for v in box)
    H, W = binary.shape
    if not (0 <= y0 < y1 <= H and 0 <= x0 < x1 <= W):
        return 0.0
    sides = [binary[y0, x0:x1], binary[y1 - 1, x0:x1],
             binary[y0:y1, x0], binary[y0:y1, x1 - 1]]
    return float(np.mean([float((s > 0).sum()) / max(len(s), 1) for s in sides]))


def _margin_shadow(binary, marks):
    import numpy as np
    H, W = binary.shape
    (tlx, tly), (trx, tryy), (blx, bly), (brx, bry) = marks
    left_x, right_x = int(min(tlx, blx)) - 40, int(max(trx, brx)) + 40
    y0, y1 = int(min(tly, tryy)), int(max(bly, bry))
    worst = (0.0, 0)
    for a, b in ((max(0, left_x - 60), max(1, left_x)),
                 (min(W - 1, right_x), min(W, right_x + 60))):
        strip = binary[y0:y1, a:b]
        if strip.size == 0:
            continue
        cov = float(strip.mean()) / 255.0
        bands = np.array_split(strip, 20, axis=0)
        n = sum(1 for bd in bands if bd.size and float(bd.mean()) / 255.0 > 0.01)
        if cov > worst[0]:
            worst = (cov, n)
    return worst


def _grid_overflow_rows(binary):
    import cv2
    import numpy as np
    if len(GRID_COLUMN_CENTERS) != 6:
        return None
    H, W = binary.shape
    hk = cv2.getStructuringElement(cv2.MORPH_RECT, (60, 1))
    hl = cv2.dilate(cv2.erode(binary, hk), hk)
    sums = hl.sum(axis=1) / 255
    peaks = [y for y in range(H) if sums[y] > W * 0.5]
    if not peaks:
        return None
    lines = _group(peaks)
    best, cur = [], [lines[0]]
    for y in lines[1:]:
        if 45 <= (y - cur[-1]) <= 145:
            cur.append(y)
        else:
            if len(cur) > len(best):
                best = cur
            cur = [y]
    if len(cur) > len(best):
        best = cur
    if len(best) < 19:
        return None
    rows = best[:19]
    C, HALF = GRID_COLUMN_CENTERS, 16
    out = []
    for i in range(18):
        cy = (rows[i] + rows[i + 1]) // 2
        vals = []
        for cx in C:
            crop = binary[cy - HALF:cy + HALF, int(cx) - HALF:int(cx) + HALF]
            vals.append(float(crop.mean()) / 255.0 if crop.size else 0.0)
        floor = float(np.min(vals))
        worst = 0.0
        for j in range(5):
            gx = int((C[j] + C[j + 1]) / 2)
            crop = binary[cy - HALF:cy + HALF, gx - 15:gx + 15]
            if crop.size:
                worst = max(worst, float(crop.mean()) / 255.0 - floor)
        if worst >= 0.03:
            out.append(i + 1)
    return out


def measure(pdf_bytes, name="(bytes)", source=None, dpi=RENDER_DPI,
           file_name_for_lookup=None, calibration_table=CALIBRATION_TABLE,
           project=None, bq_client=None, use_calibration_table=True):
    """Every measured QC signal for one PDF.

    PRIMARY INPUT: pdf_calibration_profile (notebook 1's BQ table), looked up
    by file_name. When a row exists, its measured rects/ink/geometry are used
    directly rather than re-deriving them from scratch. When no row exists
    (notebook 1 hasn't run on this file, or the read fails), an [ALERT] is
    printed and this function falls back to its own self-sufficient
    measurement - never a silent skip, never a guess."""
    import numpy as np
    pages = render_pages(pdf_bytes, dpi=dpi)

    bq_row = None
    calib_source = "self-measured"
    if use_calibration_table:
        lookup_name = file_name_for_lookup or name
        bq_row = fetch_calibration_row(lookup_name, table=calibration_table,
                                       project=project, client=bq_client)
        if bq_row is None:
            print(f"[ALERT] no pdf_calibration_profile row found for {lookup_name!r} "
                 "- falling back to self-sufficient measurement")
        else:
            calib_source = "pdf_calibration_profile"

    if bq_row is not None:
        profile = profile_from_bq_row(bq_row, pages, name)
    else:
        profile = profile_pdf_bytes(pdf_bytes, name=name, source=source, dpi=dpi)

    rots = [p.get("rotation_deg") for p in profile["pages"] if p.get("rotation_deg") is not None]
    resids = [p.get("fit_residual_px") for p in profile["pages"] if p.get("fit_residual_px") is not None]
    marks_ok = all(p.get("marks_found") for p in profile["pages"]) and len(profile["pages"]) > 0

    covs, conf, tot = [], 0, 0
    for key, e in profile["questions"].items():
        if key in _ABSENT_LAYOUTS:
            continue
        tot += e["boxes_total"]
        conf += e["boxes_confirmed"]
        b = pages[e["page_idx"]]["binary"]
        for rect in e["rects"].values():
            covs.append(_border_coverage(b, tuple(rect)))

    shadow = (0.0, 0)
    for pg in pages:
        m = detect_registration_marks(pg["binary"]) if pg["binary"] is not None else None
        if m:
            c, n = _margin_shadow(pg["binary"], m)
            if c > shadow[0]:
                shadow = (c, n)

    overflow = _grid_overflow_rows(pages[0]["binary"]) if pages else None
    ink = profile.get("ink") or {}
    resids_all = [p.get("fit_residual_px") for p in profile["pages"]]

    return {
        "tilt_deg": round(float(max(abs(r) for r in rots)), 3) if rots else None,
        "fit_residual_max": round(float(max(resids)), 2) if resids else None,
        "marks_found_all_pages": marks_ok,
        "ink_gap": ink.get("gap"), "ink_quality": ink.get("quality"),
        "ink_mark_lo": ink.get("mark_lo"), "ink_threshold": ink.get("threshold"),
        "border_rate": round(conf / tot, 4) if tot else None,
        "border_cov_min": round(float(np.min(covs)), 3) if covs else None,
        "shadow_coverage": round(shadow[0], 4), "shadow_bands": shadow[1],
        "overflow_rows": overflow,
        "page_count": profile.get("page_count"),
        "paper_sizes": sorted({p.get("paper", "?") for p in profile["pages"]}),
        "calibration_source": calib_source,
    }


def classify_measured(m):
    """The measured QC fields, plus the reasons behind each."""
    reasons = []
    if m.get("calibration_source") == "self-measured":
        reasons.append("no pdf_calibration_profile row found for this file - "
                       "measured directly instead of using notebook 1's calibration")
    tilted = "no"
    if m["tilt_deg"] is None:
        tilted = "unknown"
        reasons.append("page tilt could not be measured (registration marks not found)")
    elif m["tilt_deg"] > TILT_DEG:
        tilted = "yes"
        reasons.append(f"page tilted {m['tilt_deg']} deg")

    q = m.get("ink_quality")
    if q == "clear":
        marks_readable = "clear"
    elif q == "tight":
        marks_readable = "questionable"
        reasons.append(f"marked and blank ink nearly touch (gap {m['ink_gap']}) - "
                       "thresholding this scan is unreliable")
    else:
        marks_readable = "unreadable"
        reasons.append("no separation between marked and blank ink could be measured")

    faint = "no"
    if m.get("ink_mark_lo") is not None and m["ink_mark_lo"] < FAINT_MARK_FLOOR:
        faint = "yes"
        reasons.append(f"faintest mark reads {m['ink_mark_lo']} - light checkmark-style marks")

    if m.get("ink_mark_lo") is None:
        mark_style = "unknown"
    elif m["ink_mark_lo"] < INK_MARK_STYLE_BOUNDARY:
        mark_style = "light_checkmark"
    else:
        mark_style = "bold_x"

    ovf = m.get("overflow_rows")
    if ovf is None:
        marks_outside = "unknown"
        reasons.append("checkbox grid not locatable - could not check for marks overflowing their boxes")
    elif ovf:
        marks_outside = "yes"
        reasons.append(f"marks overflow their box on Q{', Q'.join(str(v) for v in ovf)}")
    else:
        marks_outside = "no"

    shadow = "no"
    if m["shadow_coverage"] >= SHADOW_COVERAGE and m["shadow_bands"] >= SHADOW_MIN_BANDS:
        shadow = "yes"
        reasons.append(f"edge shadow band down the margin (coverage {m['shadow_coverage']} "
                       f"across {m['shadow_bands']}/20 bands)")

    if m["border_rate"] is None:
        borders = "no"
        reasons.append("no checkbox borders could be located at all")
    elif m["border_rate"] >= BORDERS_READABLE_RATE:
        borders = "yes"
    else:
        borders = "no"
        reasons.append(f"only {m['border_rate']:.0%} of checkbox borders could be traced")

    faint_border = "no"
    if m["border_cov_min"] is not None and m["border_cov_min"] < FAINT_BORDER_COV:
        faint_border = "yes"
        reasons.append(f"faintest checkbox border is only {m['border_cov_min']:.0%} covered")

    if not m["marks_found_all_pages"]:
        reasons.append("registration marks missing on at least one page")
    if m["fit_residual_max"] is not None and m["fit_residual_max"] > MAX_TRUSTED_RESIDUAL:
        reasons.append(f"registration marks do not form a rigid quad "
                       f"(residual {m['fit_residual_max']}px)")

    return {
        "tilted": tilted, "marks_readable": marks_readable, "faint_marks": faint,
        "ink_mark_style": mark_style, "marks_outside_box": marks_outside,
        "shadow_present": shadow, "box_borders_readable": borders, "faint_border": faint_border,
    }, reasons


# ==========================================================================
# judged fields (vision)
# ==========================================================================
VISION_PROMPT = """\
You are assessing the SCAN QUALITY of one hand-filled "Treatment Perceptions
Survey (Adult)" form. You are NOT extracting answers.

The page's physical properties have ALREADY been measured exactly and are given
below. Do not re-estimate them, do not contradict them, and do not comment on
tilt, ink levels, box borders, shadows or alignment - those are settled numbers,
not opinions.

MEASURED FOR THIS FILE
{measured}

YOUR JOB - judge ONLY the two things that need eyes:

1. HANDWRITING_READABLE. Across every handwritten field, can the writing be
   read with certainty? Answer "clear", "questionable", or "unreadable".

2. TEARS_OR_DAMAGE. Physical damage obscuring content. Answer "yes" or "no".
   Scanner shadow is NOT damage - it is already measured above.

Return ONLY this JSON object:

{{"handwriting_readable": "clear|questionable|unreadable",
  "handwriting_notes": "which field(s) and which characters, if not clear",
  "tears_or_damage": "yes|no",
  "damage_notes": "what and where, if yes"}}
"""


def build_vision_prompt(m, fields):
    lines = [f"  paper: {', '.join(m['paper_sizes'])}, {m['page_count']} pages",
             f"  tilt: {m['tilt_deg']} deg",
             f"  checkbox borders traced: {m['border_rate']}",
             f"  marked-vs-blank ink separation: {m['ink_quality']} (gap {m['ink_gap']})",
             f"  faintest mark ink: {m['ink_mark_lo']}",
             f"  edge shadow: {fields['shadow_present']} "
             f"(coverage {m['shadow_coverage']}, {m['shadow_bands']}/20 bands)"]
    if m.get("overflow_rows"):
        lines.append(f"  marks measured as overflowing their box: "
                     f"Q{', Q'.join(str(v) for v in m['overflow_rows'])}")
    return VISION_PROMPT.format(measured="\n".join(lines))


def judge_with_vision(m, fields, pdf_bytes, model=None, project=None,
                      location="us-central1", dpi=150):
    model = model or VISION_MODEL
    try:
        import vertexai
        from vertexai.generative_models import GenerativeModel, Part
    except ImportError as e:                                    # noqa: BLE001
        return {"_error": f"vertexai not installed ({e}); pip install google-cloud-aiplatform"}
    try:
        import pymupdf
        vertexai.init(project=project or os.environ.get("GOOGLE_CLOUD_PROJECT"), location=location)
        doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
        try:
            images = [p.get_pixmap(dpi=dpi).tobytes("png") for p in doc]
        finally:
            doc.close()
        parts = [Part.from_data(png, mime_type="image/png") for png in images]
        resp = GenerativeModel(model).generate_content(
            parts + [build_vision_prompt(m, fields)],
            generation_config={"temperature": 0, "response_mime_type": "application/json"})
        return json.loads(resp.text)
    except Exception as e:                                      # noqa: BLE001
        return {"_error": f"{type(e).__name__}: {e}"}


# ==========================================================================
# derived fields — ROUTING
# ==========================================================================
def derive(fields, reasons, m, vision=None):
    """overall_quality (describes the SCAN) and recommended_route (describes
    WHAT TO DO) are different questions, decided in that order so they can
    never disagree. Every individual trigger is kept as its own field so a BQ
    reader can see exactly which check fired, not just the aggregate."""
    reasons = list(reasons)
    hw, damage = "unknown", "unknown"
    if vision and not vision.get("_error"):
        hw = vision.get("handwriting_readable") or "unknown"
        damage = vision.get("tears_or_damage") or "unknown"
        if hw == "questionable" and vision.get("handwriting_notes"):
            reasons.append(f"handwriting questionable: {vision['handwriting_notes']}")
        elif hw == "unreadable":
            reasons.append("handwriting could not be read"
                           + (f": {vision['handwriting_notes']}" if vision.get("handwriting_notes") else ""))
        if damage == "yes":
            reasons.append("physical damage on the page"
                           + (f": {vision['damage_notes']}" if vision.get("damage_notes") else ""))
    elif vision and vision.get("_error"):
        reasons.append(f"vision pass failed: {vision['_error']}")

    fields = dict(fields)
    fields["handwriting_readable"] = hw
    fields["tears_or_damage"] = damage

    trig_marks_unreadable = fields["marks_readable"] == "unreadable"
    trig_borders_unreadable = fields["box_borders_readable"] == "no"
    trig_tilted = fields["tilted"] in ("yes", "unknown")
    trig_marks_missing = not m["marks_found_all_pages"]
    trig_residual_high = (m["fit_residual_max"] is not None
                          and m["fit_residual_max"] > MAX_TRUSTED_RESIDUAL)
    pixel_unreadable = (trig_marks_unreadable or trig_borders_unreadable or trig_tilted
                       or trig_marks_missing or trig_residual_high)

    trig_marks_questionable = fields["marks_readable"] == "questionable"
    trig_marks_outside = fields["marks_outside_box"] in ("yes", "unknown")
    trig_handwriting_questionable = hw in ("questionable", "unreadable")
    trig_damage = damage == "yes"
    questionable = (trig_marks_questionable or trig_marks_outside
                   or trig_handwriting_questionable or trig_damage)

    n_flags = len(reasons)
    trig_flag_count = n_flags >= VISION_ROUTE_MIN_FLAGS
    if pixel_unreadable:
        overall, route = "totally_unreadable", "fallback"
    elif questionable or trig_flag_count:
        overall, route = "unclear", "vision"
        if not questionable and trig_flag_count:
            reasons.append(f"{n_flags} quality issues flagged (>= {VISION_ROUTE_MIN_FLAGS}) "
                           "- routed to vision verification")
    elif reasons:
        overall, route = "unclear", "pixel"
    else:
        overall, route = "clear", "pixel"

    fields["overall_quality"] = overall
    fields["recommended_route"] = route
    fields["needs_review"] = route != "pixel"
    fields["error_reasons"] = reasons
    fields["pixel_unreadable"] = pixel_unreadable
    fields["trig_marks_unreadable"] = trig_marks_unreadable
    fields["trig_borders_unreadable"] = trig_borders_unreadable
    fields["trig_tilted"] = trig_tilted
    fields["trig_marks_missing"] = trig_marks_missing
    fields["trig_residual_high"] = trig_residual_high
    fields["questionable_flag"] = questionable
    fields["trig_marks_questionable"] = trig_marks_questionable
    fields["trig_marks_outside"] = trig_marks_outside
    fields["trig_handwriting_questionable"] = trig_handwriting_questionable
    fields["trig_damage"] = trig_damage
    fields["trig_flag_count"] = trig_flag_count
    return fields


def classify_pdf_bytes(pdf_bytes, name="(bytes)", source=None, folder=None, dpi=RENDER_DPI,
                       vision=False, vision_only_flagged=True, model=None,
                       project=None, location="us-central1",
                       calibration_table=CALIBRATION_TABLE, bq_client=None,
                       use_calibration_table=True):
    m = measure(pdf_bytes, name=name, source=source, dpi=dpi,
               file_name_for_lookup=name, calibration_table=calibration_table,
               project=project, bq_client=bq_client,
               use_calibration_table=use_calibration_table)
    fields, reasons = classify_measured(m)
    v = None
    if vision:
        provisional = derive(fields, reasons, m)
        if not vision_only_flagged or provisional["overall_quality"] != "clear":
            v = judge_with_vision(m, fields, pdf_bytes, model=model, project=project, location=location)
    out = derive(fields, reasons, m, vision=v)
    out["_measured"] = m
    out["_vision"] = v
    out["file"] = name
    out["source"] = source or name
    out["folder"] = folder
    return out


def classify_pdf(path, **kw):
    p = Path(path)
    return classify_pdf_bytes(p.read_bytes(), name=p.name, source=str(p),
                              folder=p.parent.name, **kw)


def classify_gcs(uri, root_uri=None, client=None, project=None, **kw):
    bucket, blob = parse_gcs_uri(uri)
    client = client or _gcs_client(project)
    data = client.bucket(bucket).blob(blob).download_as_bytes()
    folder = _gcs_folder(root_uri, uri) if root_uri else os.path.basename(os.path.dirname(blob))
    return classify_pdf_bytes(data, name=os.path.basename(blob), source=uri, folder=folder, **kw)


# ==========================================================================
# GCS
# ==========================================================================
def parse_gcs_uri(uri):
    from urllib.parse import unquote, urlparse
    if uri.startswith("gs://"):
        rest = uri[5:]
    else:
        u = urlparse(uri)
        if u.netloc not in ("storage.googleapis.com", "storage.cloud.google.com"):
            raise ValueError(f"not a recognised GCS reference: {uri!r}")
        rest = u.path.lstrip("/")
    rest = unquote(rest)
    bucket, _, blob = rest.partition("/")
    if not bucket or not blob:
        raise ValueError(f"could not split bucket/object out of {uri!r}")
    return bucket, blob


def _gcs_client(project=None):
    from google.cloud import storage
    return storage.Client(project=project) if project else storage.Client()


def list_gcs_pdfs(root_uri, client=None, project=None):
    from urllib.parse import unquote
    rest = root_uri
    for scheme in ("gs://", "https://storage.googleapis.com/", "https://storage.cloud.google.com/"):
        if rest.startswith(scheme):
            rest = rest[len(scheme):]
            break
    rest = unquote(rest).lstrip("/")
    bucket, _, prefix = rest.partition("/")
    if prefix and not prefix.endswith("/") and not prefix.lower().endswith(".pdf"):
        prefix += "/"
    client = client or _gcs_client(project)
    return sorted(f"gs://{bucket}/{b.name}"
                  for b in client.list_blobs(bucket, prefix=prefix)
                  if b.name.lower().endswith(".pdf") and not b.name.endswith("/"))


def _gcs_folder(root_uri, uri):
    _, root_blob = parse_gcs_uri(root_uri)
    _, blob = parse_gcs_uri(uri)
    root_prefix = root_blob if root_blob.endswith("/") else root_blob.rsplit("/", 1)[0] + "/"
    rel = blob[len(root_prefix):] if blob.startswith(root_prefix) else blob
    parts = rel.split("/")
    return parts[0] if len(parts) > 1 else None


# ==========================================================================
# BigQuery
# ==========================================================================
BQ_SCHEMA = [
    ("gcs_uri", "STRING", "Full gs:// URI (or local path) of the assessed PDF"),
    ("file_name", "STRING", "PDF file name"),
    ("survey_folder", "STRING", "Date folder the PDF sits in, or NULL if none"),
    ("partition_date", "DATE",
     "Date parsed from survey_folder; today's date (this run) when the file "
     "has no folder or the folder name couldn't be parsed. Table is partitioned on this."),
    ("tilted", "STRING", "yes / no / unknown"),
    ("handwriting_readable", "STRING", "clear / questionable / unreadable / unknown - VISION"),
    ("marks_readable", "STRING", "clear / questionable / unreadable - measured ink separation"),
    ("faint_marks", "STRING", "yes / no"),
    ("ink_mark_style", "STRING", "light_checkmark / bold_x / unknown"),
    ("marks_outside_box", "STRING", "yes / no / unknown"),
    ("shadow_present", "STRING", "yes / no"),
    ("tears_or_damage", "STRING", "yes / no / unknown - VISION"),
    ("box_borders_readable", "STRING", "yes / no"),
    ("faint_border", "STRING", "yes / no"),
    ("overall_quality", "STRING", "clear / unclear / totally_unreadable"),
    ("error_reasons", "STRING", "REPEATED: every applicable reason, in plain language"),
    ("recommended_route", "STRING", "pixel / vision / fallback"),
    ("needs_review", "BOOLEAN", "TRUE for any route other than pixel"),
    ("tilt_deg", "FLOAT", None),
    ("ink_gap", "FLOAT", None),
    ("ink_mark_lo", "FLOAT", None),
    ("ink_threshold", "FLOAT", None),
    ("border_rate", "FLOAT", None),
    ("border_cov_min", "FLOAT", None),
    ("shadow_coverage", "FLOAT", None),
    ("shadow_bands", "INTEGER", None),
    ("overflow_questions", "STRING", None),
    ("fit_residual_max", "FLOAT", None),
    ("vision_error", "STRING", None),
    ("pixel_unreadable", "BOOLEAN", None),
    ("trig_marks_unreadable", "BOOLEAN", None),
    ("trig_borders_unreadable", "BOOLEAN", None),
    ("trig_tilted", "BOOLEAN", None),
    ("trig_marks_missing", "BOOLEAN", None),
    ("trig_residual_high", "BOOLEAN", None),
    ("questionable_flag", "BOOLEAN", None),
    ("trig_marks_questionable", "BOOLEAN", None),
    ("trig_marks_outside", "BOOLEAN", None),
    ("trig_handwriting_questionable", "BOOLEAN", None),
    ("trig_damage", "BOOLEAN", None),
    ("trig_flag_count", "BOOLEAN", None),
    ("calibration_source", "STRING",
     "'pdf_calibration_profile' when this file's geometry/ink came from notebook 1's "
     "BQ table; 'self-measured' when no row was found there and this notebook fell "
     "back to measuring the file itself (see error_reasons for the [ALERT])"),
    ("classifier_version", "STRING", None),
    ("assessed_at", "TIMESTAMP", None),
]
BQ_FIELDS = [n for n, _t, _d in BQ_SCHEMA]


def to_row(r, run_date):
    m = r.get("_measured") or {}
    v = r.get("_vision") or {}
    uri = r.get("source") or ""
    ovf = m.get("overflow_rows")
    folder = r.get("folder")
    return {
        "gcs_uri": uri, "file_name": r.get("file"), "survey_folder": folder,
        "partition_date": parse_folder_date(folder, default=run_date),
        "tilted": r.get("tilted"), "handwriting_readable": r.get("handwriting_readable"),
        "marks_readable": r.get("marks_readable"), "faint_marks": r.get("faint_marks"),
        "ink_mark_style": r.get("ink_mark_style"), "marks_outside_box": r.get("marks_outside_box"),
        "shadow_present": r.get("shadow_present"), "tears_or_damage": r.get("tears_or_damage"),
        "box_borders_readable": r.get("box_borders_readable"),
        "faint_border": r.get("faint_border"), "overall_quality": r.get("overall_quality"),
        "error_reasons": r.get("error_reasons") or [],
        "recommended_route": r.get("recommended_route"), "needs_review": r.get("needs_review"),
        "tilt_deg": m.get("tilt_deg"), "ink_gap": m.get("ink_gap"),
        "ink_mark_lo": m.get("ink_mark_lo"), "ink_threshold": m.get("ink_threshold"),
        "border_rate": m.get("border_rate"), "border_cov_min": m.get("border_cov_min"),
        "shadow_coverage": m.get("shadow_coverage"), "shadow_bands": m.get("shadow_bands"),
        "overflow_questions": ",".join(str(x) for x in ovf) if ovf else None,
        "fit_residual_max": m.get("fit_residual_max"),
        "vision_error": (v.get("_error") if v else None),
        "pixel_unreadable": r.get("pixel_unreadable"),
        "trig_marks_unreadable": r.get("trig_marks_unreadable"),
        "trig_borders_unreadable": r.get("trig_borders_unreadable"),
        "trig_tilted": r.get("trig_tilted"),
        "trig_marks_missing": r.get("trig_marks_missing"),
        "trig_residual_high": r.get("trig_residual_high"),
        "questionable_flag": r.get("questionable_flag"),
        "trig_marks_questionable": r.get("trig_marks_questionable"),
        "trig_marks_outside": r.get("trig_marks_outside"),
        "trig_handwriting_questionable": r.get("trig_handwriting_questionable"),
        "trig_damage": r.get("trig_damage"),
        "trig_flag_count": r.get("trig_flag_count"),
        "calibration_source": m.get("calibration_source"),
        "classifier_version": CLASSIFIER_VERSION,
        "assessed_at": _dt.datetime.now(_dt.timezone.utc),
    }


def to_dataframe(results, run_date=None):
    import pandas as pd
    run_date = run_date or _dt.date.today()
    return pd.DataFrame([to_row(r, run_date) for r in results], columns=BQ_FIELDS)


def to_bigquery(results_or_df, table=BQ_TABLE, project=None, client=None,
                write_disposition="WRITE_APPEND", location=None, run_date=None):
    """Loads QC rows into a DATE-partitioned table (partitioned on
    partition_date). Default WRITE_APPEND to match the partitioned design."""
    from google.cloud import bigquery
    import pandas as pd
    df = results_or_df if isinstance(results_or_df, pd.DataFrame) else to_dataframe(results_or_df, run_date=run_date)
    if df.empty:
        raise ValueError("nothing to load - the sweep produced no rows")
    df = df.reindex(columns=BQ_FIELDS)
    for name, typ, _d in BQ_SCHEMA:
        if typ == "FLOAT":
            df[name] = pd.to_numeric(df[name], errors="coerce")
        elif typ == "INTEGER":
            df[name] = pd.to_numeric(df[name], errors="coerce").astype("Int64")
        elif typ == "TIMESTAMP":
            df[name] = pd.to_datetime(df[name], utc=True, errors="coerce")
        elif typ == "DATE":
            df[name] = pd.to_datetime(df[name], errors="coerce").dt.date
        elif typ == "BOOLEAN":
            df[name] = df[name].astype("boolean")
        elif name == "error_reasons":
            df[name] = df[name].apply(lambda v: list(v) if isinstance(v, (list, tuple)) else [])
        else:
            df[name] = df[name].astype("string")

    # table is always "<project>.<dataset>.<table_name>" - parse it explicitly
    # and build every dataset/table reference off that parsed project, never a
    # bare "project.dataset" string. Some environments run with an ambient
    # default GCP project on the BigQuery client (e.g. "common_data_dev", from
    # GOOGLE_CLOUD_PROJECT/ADC) that differs from the project actually named
    # in `table`. A bare-string Dataset/DatasetReference can silently resolve
    # against that ambient default instead, producing
    # "Invalid resource name projects/<ambient>". Explicit
    # DatasetReference(project, dataset_id) removes the ambiguity.
    table_project, table_dataset, table_name = table.split(".")
    project = project or table_project
    client = client or bigquery.Client(project=project)
    dataset_ref = bigquery.DatasetReference(table_project, table_dataset)
    dataset_id = f"{table_project}.{table_dataset}"
    try:
        ds = client.get_dataset(dataset_ref)
        job_location = ds.location
    except Exception:                                           # noqa: BLE001
        ds = bigquery.Dataset(dataset_ref)
        ds.location = location or "US"
        client.create_dataset(ds, exists_ok=True)
        job_location = ds.location
        print(f"created dataset {dataset_id} in {job_location}")

    schema = []
    for n, t, d in BQ_SCHEMA:
        mode = "REPEATED" if n == "error_reasons" else "NULLABLE"
        schema.append(bigquery.SchemaField(n, "STRING" if n == "error_reasons" else t,
                                           mode=mode, description=d))
    try:
        existing = client.get_table(table)
        table_exists = True
    except Exception:                                           # noqa: BLE001
        existing = None
        table_exists = False

    if table_exists:
        # An existing table created before partition_date (or any other new
        # column) existed needs it added BEFORE the load, or BigQuery refuses
        # the job with "Cannot add fields". ALTER the live schema to the
        # union of what's there plus BQ_SCHEMA, additive only.
        existing_names = {f.name for f in existing.schema}
        missing = [f for f in schema if f.name not in existing_names]
        if missing:
            existing.schema = list(existing.schema) + missing
            client.update_table(existing, ["schema"])
            print(f"added column(s) to {table}: {[f.name for f in missing]}")

    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition=write_disposition)
    if not table_exists:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="partition_date")
        print(f"creating {table} partitioned on partition_date (DAY)")

    job = client.load_table_from_dataframe(df, table, location=job_location, job_config=job_config)
    job.result()
    print(f"loaded {len(df)} QC row(s) into {table} ({write_disposition}), "
          f"partitions: {sorted(set(str(d) for d in df['partition_date'].dropna()))}")
    return len(df)


# ==========================================================================
# sweep
# ==========================================================================
def run(root_uri, table=BQ_TABLE, project=None, dpi=RENDER_DPI,
        vision=False, vision_only_flagged=True, model=None,
        write_disposition="WRITE_APPEND", verbose=True, dry_run=False,
        calibration_table=CALIBRATION_TABLE, use_calibration_table=True):
    """Classify every PDF under a prefix (or local directory) and load the
    partitioned BigQuery table.

    PRIMARY INPUT for each file's measurements is notebook 1's
    pdf_calibration_profile table (looked up by file_name). When a file has
    no row there, an [ALERT] is printed and that one file falls back to
    self-sufficient measurement - see measure()."""
    run_date = _dt.date.today()

    bq_client = None
    if use_calibration_table:
        try:
            from google.cloud import bigquery
            bq_client = bigquery.Client(project=project or calibration_table.split(".")[0])
        except Exception as e:                                  # noqa: BLE001
            # No BQ access at all for this run (auth/network) - every file
            # will alert and fall back individually; don't crash the whole
            # sweep over something each file already handles.
            print(f"[ALERT] could not create a BigQuery client for {calibration_table} ({e}) "
                 "- every file in this run will fall back to self-sufficient measurement")
            bq_client = False  # sentinel: "don't retry construction per file"

    if root_uri.startswith(("gs://", "https://")):
        client = _gcs_client(project)
        uris = list_gcs_pdfs(root_uri, client=client)
        getter = lambda u: classify_gcs(u, root_uri=root_uri, client=client, dpi=dpi, vision=vision,  # noqa: E731
                                        vision_only_flagged=vision_only_flagged, model=model, project=project,
                                        calibration_table=calibration_table, bq_client=bq_client,
                                        use_calibration_table=use_calibration_table)
        folder_of = lambda u: _gcs_folder(root_uri, u)                                                # noqa: E731
    else:
        p = Path(root_uri)
        if not p.exists():
            raise FileNotFoundError(f"no such file or directory: {root_uri}")
        if p.is_dir():
            uris = [str(x) for x in sorted(p.rglob("*.pdf"))]
        elif p.suffix.lower() == ".pdf":
            uris = [str(p)]
        else:
            raise ValueError(f"not a PDF or a directory: {root_uri}")
        _root_resolved = Path(root_uri).resolve()
        folder_of = lambda u: (Path(u).parent.name                                                    # noqa: E731
                               if Path(u).resolve().parent != _root_resolved else None)
        # classify_pdf() alone would always set folder=p.parent.name, even when
        # that parent IS the swept root - use folder_of() so a file directly
        # under root_uri correctly gets folder=None, not the root dir's name.
        getter = lambda u: classify_pdf_bytes(                                                        # noqa: E731
            Path(u).read_bytes(), name=Path(u).name, source=str(u), folder=folder_of(u),
            dpi=dpi, vision=vision, vision_only_flagged=vision_only_flagged, model=model,
            project=project, calibration_table=calibration_table, bq_client=bq_client,
            use_calibration_table=use_calibration_table)
    if not uris:
        raise FileNotFoundError(f"no PDFs found under {root_uri}")

    from collections import Counter
    counts = Counter(folder_of(u) or "(no folder)" for u in uris)
    print(f"{len(uris)} PDF(s) across {len(counts)} folder(s) under {root_uri}")
    for folder, n in sorted(counts.items()):
        d = parse_folder_date(None if folder == "(no folder)" else folder, default=run_date)
        print(f"    {folder:<24} {n:>3}  -> partition_date {d}")
    print()

    results, seen_vision_errors = [], set()
    for u in uris:
        try:
            r = getter(u)
        except Exception as e:                                  # noqa: BLE001
            r = {"file": os.path.basename(u), "source": u, "folder": folder_of(u),
                 "overall_quality": "totally_unreadable", "recommended_route": "fallback",
                 "needs_review": True, "error_reasons": [f"could not be assessed: {e}"],
                 "_measured": {}, "_vision": None}
        results.append(r)
        if verbose:
            ve = (r.get("_vision") or {}).get("_error")
            note = ""
            if ve:
                if ve not in seen_vision_errors:
                    seen_vision_errors.add(ve)
                    note = f"\n        vision FAILED: {ve}"
                else:
                    note = "   vision: failed (same reason as above)"
            print(f"  {r['file'][:32]:34} {r.get('overall_quality','?'):<18}"
                  f"-> {r.get('recommended_route','?'):<9}"
                  f"{len(r.get('error_reasons') or []):>2} reason(s){note}")

    df = to_dataframe(results, run_date=run_date)
    print("\nroutes: " + ", ".join(f"{k} {v}" for k, v in
                                   df["recommended_route"].value_counts().items()))
    if dry_run:
        print("[dry-run] not writing to BigQuery")
        return df
    to_bigquery(df, table=table, project=project, write_disposition=write_disposition, run_date=run_date)
    return df


def explain(r):
    m = r.get("_measured") or {}
    print(f"{r['file']}   {r['overall_quality'].upper()} -> route {r['recommended_route']}"
          f"   folder={r.get('folder')}   calibration_source={m.get('calibration_source')}")
    for k in ("tilted", "handwriting_readable", "marks_readable", "faint_marks",
              "ink_mark_style", "marks_outside_box", "shadow_present", "tears_or_damage",
              "box_borders_readable", "faint_border"):
        print(f"    {k:24} {r.get(k)}")
    for reason in r.get("error_reasons") or []:
        print(f"    ! {reason}")


# ==========================================================================
# run directly, e.g. from a notebook cell:
#
#   import step3_pdf_quality_check as step3
#   df = step3.run("gs://tps_survey/TPS_Scanned_2025_Reorgnized/", vision=True)
# ==========================================================================

# df = run("gs://tps_survey/TPS_Scanned_2025_Reorgnized/Nov 23 2025/", vision=True)
