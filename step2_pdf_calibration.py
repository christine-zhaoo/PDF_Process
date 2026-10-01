#!/usr/bin/env python3
"""
Reads every PDF under a folder/GCS prefix, measures its registration-mark transform, per-question box geometry and per-file ink threshold, and loads
one partitioned BigQuery table:

    pdf_calibration_profile

PARTITIONING
------------
Each row's `partition_date` is the DATE parsed from the PDF's parent folder name (e.g. "Nov 10 2025", "Nov10_2025", "2025-11-10" all parse). A file with
no parent folder (sitting directly under the swept prefix) gets today's date - the date this run happened - as its partition date. The table is a
DATE-partitioned table on this column, so a write only ever touches the partitions for the dates present in this run's corpus.

"""
import datetime as _dt
import importlib
import json
import os
import re
import pprint
from pathlib import Path

import pipeline_config

RENDER_DPI = 300
# "<dataset>.<table>" (project is added separately - see to_bigquery()'s own
# table_project, also sourced from pipeline_config.py) - both pulled from
# pipeline_config.py, the shared source of truth across step1-step6.
BQ_TABLE = f"{pipeline_config.BQ_DATASET}.{pipeline_config.BQ_TABLE_CALIBRATION}"

# ==========================================================================
# calibration baseline — measured box geometry
# ==========================================================================
CALIBRATION_SOURCE = "Nov1_7_TPS_4223.pdf"


BASELINE_MARKS = {
    0: ((184.0, 223.0), (2384.0, 224.0), (176.0, 3120.0), (2374.0, 3119.0)),
    1: ((193.0, 211.0), (2380.0, 209.0), (200.0, 3107.0), (2377.0, 3106.0)),
}

INK_THRESHOLD = 165
GRID_COLUMN_CENTERS = (1661.75, 1790.4, 1919.6, 2049.9, 2179.6, 2309.1)
GRID_BOX_EXPECTED_SIZE = 40

_LEGACY_YESNO_BOX_GEOMETRY = {
    "19": (0, {
        "None": (658, 704, 2446, 2490),
        "Very little": (882, 928, 2446, 2490),
        "About half": (1168, 1214, 2446, 2490),
        "Almost all": (1471, 1516, 2446, 2490),
        "All": (1765, 1811, 2446, 2490),
    }),
    "20": (0, {
        "Much better": (659, 703, 2554, 2598),
        "Somewhat better": (987, 1031, 2554, 2598),
        "About the same": (1400, 1444, 2554, 2598),
        "Somewhat worse": (1792, 1837, 2554, 2598),
        "N/A": (2206, 2251, 2554, 2598),
    }),
    "21": (0, {"Yes": (1421, 1465, 2657, 2703), "No": (1701, 1747, 2657, 2702)}),
    "22": (0, {"Yes": (1421, 1465, 2728, 2772), "No": (1701, 1747, 2728, 2774)}),
    "23": (0, {
        "Strongly Agree": (584, 631, 2840, 2886),
        "Agree": (962, 1008, 2840, 2886),
        "I am Neutral": (1196, 1243, 2840, 2886),
        "Disagree": (1532, 1578, 2840, 2886),
        "Strongly Disagree": (1815, 1862, 2840, 2886),
        "N/A": (2240, 2287, 2840, 2886),
    }),
    "25": (1, {
        "First visit/day": (272, 305, 1101, 1133),
        "2 weeks or less": (272, 305, 1147, 1179),
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
        "Male": (274, 307, 2295, 2327),
        "Female": (274, 307, 2343, 2376),
        "Female-to-Male (FTM)/Transgender Male/Trans Man": (274, 307, 2388, 2420),
        "Male-to-Female (MTF)/Transgender Female/Trans Woman": (274, 307, 2434, 2464),
        "Gender Queer/Gender Non-Conforming": (274, 307, 2483, 2512),
        "Other (specify)": (274, 307, 2543, 2571),
        "Prefer not to state": (275, 308, 2618, 2649),
    }),
    "30": (1, {
        "Female": (284, 312, 2776, 2804),
        "Male": (785, 818, 2773, 2806),
        "Other (specify)": (284, 312, 2825, 2853),
        "Prefer not to state": (785, 818, 2822, 2855),
    }),
    "31": (1, {
        "Heterosexual/Straight": (1352, 1386, 1028, 1062),
        "Lesbian (Female)": (1352, 1386, 1076, 1110),
        "Gay (Male)": (1352, 1387, 1122, 1156),
        "Bisexual": (1352, 1386, 1172, 1206),
        "Unsure/Questioning/Don't know": (1352, 1386, 1219, 1253),
        "Pansexual": (1858, 1893, 1028, 1061),
        "Asexual": (1858, 1893, 1074, 1108),
        "Other (specify)": (1858, 1893, 1121, 1155),
        "Queer": (1858, 1893, 1171, 1205),
        "Prefer not to state": (1858, 1893, 1219, 1253),
    }),
    "32": (1, {
        "Yes": (1354, 1389, 1453, 1486),
        "No": (1647, 1680, 1453, 1486),
        "Unknown": (1900, 1931, 1453, 1486),
    }),
    "35": (1, {
        "Post-release Community Supervision (AB109) or on Probation from any federal, state, or local jurisdiction": (1354, 1388, 2642, 2675),
        "Awaiting trial, charges or sentencing": (1354, 1388, 2750, 2779),
        "On parole from any other jurisdiction": (1354, 1388, 2805, 2838),
        "Any other criminal justice involvement": (1354, 1388, 2858, 2891),
        "No criminal justice involvement": (1354, 1388, 2912, 2945),
    }),
}

_LEGACY_MULTISELECT_BOX_GEOMETRY = {
    "33": (1, {
        "American Indian/Alaskan Native": (1357, 1392, 1618, 1652),
        "Asian": (1357, 1392, 1671, 1705),
        "Black/African American": (1357, 1392, 1720, 1755),
        "Native Hawaiian/Pacific Islander": (1357, 1392, 1773, 1807),
        "White/Caucasian": (1357, 1392, 1825, 1859),
        "Other (specify)": (1357, 1392, 1876, 1910),
        "Prefer not to state": (1357, 1392, 1958, 1992),
    }),
    "34": (1, {
        "Physically Disabled": (1360, 1388, 2121, 2149),
        "Visually Impaired/Blind": (1360, 1388, 2169, 2196),
        "Hearing Impaired/Deaf": (1360, 1389, 2217, 2245),
        "Co-occurring Mental Health Condition": (1360, 1389, 2270, 2299),
        "Developmentally or Intellectually Disabled": (1360, 1389, 2321, 2349),
        "Other (specify)": (1360, 1389, 2389, 2418),
        "None": (1360, 1389, 2444, 2473),
    }),
}

H3_CIRCLE_CALIBRATION = (0, {
    "Early Intervention": (687, 715, 304, 332),
    "OP/IOP": (1041, 1070, 303, 332),
    "Residential": (1236, 1265, 304, 332),
    "OTP/NTP": (1492, 1520, 304, 332),
    "Detox/WM": (1715, 1744, 304, 332),
    "Recovery Services": (1956, 1984, 303, 332),
})

# The public calibration contract is semantic: question number, page, and
# answer labels. Pixel coordinates are discovered from each PDF at runtime.
YESNO_BOX_CALIBRATION = {
    "19": {"page": 0, "labels": ["None", "Very little", "About half", "Almost all", "All"]},
    "20": {"page": 0, "labels": ["Much better", "Somewhat better", "About the same", "Somewhat worse", "N/A"]},
    "21": {"page": 0, "labels": ["Yes", "No"]},
    "22": {"page": 0, "labels": ["Yes", "No"]},
    "23": {"page": 0, "labels": ["Strongly Agree", "Agree", "I am Neutral", "Disagree", "Strongly Disagree", "N/A"]},
    "25": {"page": 1, "labels": ["First visit/day", "2 weeks or less", "More than 2 weeks but less than 4 weeks", "4 weeks or more"]},
    "27": {"page": 1, "labels": ["Yes", "No"], "layouts": 2},
    "28": {"page": 1, "labels": ["Yes, I am currently receiving Contingency Management services", "Yes, I received Contingency Management services in the past", "No, I have never received Contingency Management services"]},
    "29": {"page": 1, "labels": ["Male", "Female", "Female-to-Male (FTM)/Transgender Male/Trans Man", "Male-to-Female (MTF)/Transgender Female/Trans Woman", "Gender Queer/Gender Non-Conforming", "Other (specify)", "Prefer not to state"]},
    "30": {"page": 1, "labels": ["Female", "Male", "Other (specify)", "Prefer not to state"]},
    "31": {"page": 1, "labels": ["Heterosexual/Straight", "Lesbian (Female)", "Gay (Male)", "Bisexual", "Unsure/Questioning/Don't know", "Pansexual", "Asexual", "Queer", "Other (specify)", "Prefer not to state"]},
    "32": {"page": 1, "labels": ["Yes", "No", "Unknown"]},
    "35": {"page": 1, "labels": ["Post-release Community Supervision (AB109) or on Probation from any federal, state, or local jurisdiction", "Awaiting trial, charges or sentencing", "On parole from any other jurisdiction", "Any other criminal justice involvement", "No criminal justice involvement"]},
}
MULTISELECT_BOX_CALIBRATION = {
    "33": {"page": 1, "labels": ["American Indian/Alaskan Native", "Asian", "Black/African American", "Native Hawaiian/Pacific Islander", "White/Caucasian", "Other (specify)", "Prefer not to state"]},
    "34": {"page": 1, "labels": ["Physically Disabled", "Visually Impaired/Blind", "Hearing Impaired/Deaf", "Co-occurring Mental Health Condition", "Developmentally or Intellectually Disabled", "Other (specify)", "None"]},
}


def all_box_questions():
    """Every (question, page_idx, candidate_idx, {label: rect}, kind) the
    calibrator should emit a profile for."""
    out = []
    for q, raw in _LEGACY_YESNO_BOX_GEOMETRY.items():
        cands = raw if isinstance(raw, list) else [raw]
        for ci, (pg, boxes) in enumerate(cands):
            out.append((q, pg, ci, boxes, "yesno_box"))
    for q, (pg, boxes) in _LEGACY_MULTISELECT_BOX_GEOMETRY.items():
        out.append((q, pg, 0, boxes, "multiselect"))
    return out


# Per-question baseline correction for questions whose baseline rect was measured
# against a DIFFERENT reference file than CALIBRATION_SOURCE. Measured on a 10-file 
# reference corpus; a property of the printed template, not of any one scan.
QUESTION_BASELINE_CORRECTION = {
    "31": (0.0, 23.0),
    "33": (0.0, 17.0),
    "34": (0.0, 16.0),
}

# Half the tightest inter-row pitch on this form. A correction search must stay inside this or it can align a question onto its own neighbouring row.
SEARCH_BOUND = 22

# Registration-mark detection geometry (same as the pipeline's own marks).
REG_MARK_SIZE = 74
REG_MARK_SIZE_TOL = 28
REG_MARK_FILL = 0.75

# Per-box search pad used when CONFIRMING a transformed position.
CONFIRM_PAD = 8

# A plausible marked-box count per file, and the minimum gap that counts as a "clear" (safe-to-threshold) blank/marked ink split. See _split_clusters().
MIN_PLAUSIBLE_MARKED = 8
MAX_PLAUSIBLE_MARKED = 25
CLEAR_GAP = 0.08

CALIBRATOR_VERSION = "notebook_1-1.0"

# This is deliberately kept separate from the discovered profile.  The
# hand-measured geometry is the safety net used by step4 when a scan cannot be
# registered, and must not be replaced by an automatic run.
MANUAL_GEOMETRY_REFERENCE = {
    "source": CALIBRATION_SOURCE,
    "version": CALIBRATOR_VERSION,
    "yesno_box": YESNO_BOX_CALIBRATION,
    "multiselect": MULTISELECT_BOX_CALIBRATION,
    "h3_circle": H3_CIRCLE_CALIBRATION,
    "grid_column_centers": GRID_COLUMN_CENTERS,
}

# Semantics are intentionally separate from geometry.  This records only
# answers confirmed during review; printed wording remains pending until it is
# supplied by a reviewer.
QUESTION_SEMANTICS = {
    "27": {
        "status": "reviewed",
        "text": "Are you homeless?",
        "answer_type": "single-choice",
        "layouts": [
            {"candidate": 0, "labels": ["Yes", "No"]},
            {"candidate": 1, "labels": ["Yes", "No"]},
        ],
    },
}


def survey_semantics_reference(survey_questions=None):
    """Normalize step4's SURVEY_QUESTIONS for calibration artifacts.

    Geometry remains independent (and the manual geometry remains the runtime
    fallback), but labels and answer kinds must come from the same template
    definition that extraction uses.  Importing lazily keeps this generator
    usable in small calibration/test environments where step4's optional
    runtime dependencies are not installed.
    """
    if survey_questions is None:
        try:
            survey_questions = importlib.import_module(
                "step4_process_pdf").SURVEY_QUESTIONS
        except (ImportError, AttributeError):
            survey_questions = ()
    result = {}
    for number, _group_key, text, choices in survey_questions:
        labels = [part.strip() for part in choices.split(" / ")] if " / " in choices else []
        if labels:
            answer_type = (
                "multi-select" if "mark all" in text.lower() else "single-choice"
            )
        else:
            answer_type = "open-text"
        result[str(number)] = {
            "question": text,
            "answer_type": answer_type,
            "labels": labels,
            "choice_text": choices,
        }
    return result


def manual_geometry_reference():
    """Return a JSON-safe copy of step4's current manual geometry baseline."""
    # Round-tripping also prevents callers from mutating the module constants.
    return json.loads(json.dumps(MANUAL_GEOMETRY_REFERENCE))


def _question_prompt(question, reason, observed=None):
    details = f" Observed: {observed}." if observed else ""
    semantics = QUESTION_SEMANTICS.get(str(question))
    if semantics and semantics.get("status") == "reviewed":
        return (
            f"Confirm question {question} geometry for "
            f"{semantics['text']!r}: verify the page (1-based), the "
            f"{semantics['answer_type']} layouts, and checkbox locations."
            f"{details} Reason it needs review: {reason}."
        )
    return (
        f"Confirm question {question}: identify the printed question text, page "
        f"(1-based), answer type (single-choice, multi-select, or open text), "
        f"choice labels in visual order, and the checkbox/circle locations.{details} "
        f"Reason it needs review: {reason}."
    )


def build_question_prompts(unresolved):
    """Generate bounded human-review prompts for unresolved semantics.

    Geometry can be discovered deterministically, but a box's meaning cannot
    safely be inferred from ink or registration marks alone.  The returned
    prompts are intentionally suitable for a review queue or an LLM request.
    """
    prompts = []
    for item in unresolved or []:
        if isinstance(item, str):
            question, reason, observed = item, "no validated geometry", None
        else:
            question = str(item.get("question", "?"))
            reason = item.get("reason", "no validated geometry")
            observed = item.get("observed")
        prompts.append(_question_prompt(question, reason, observed))
    return prompts


def build_semantics_prompts():
    """Generate a review prompt for every manually referenced question."""
    prompts = []
    for question, page_idx, candidate_idx, boxes, kind in all_box_questions():
        key = f"{question}#{candidate_idx}" if isinstance(
            _LEGACY_YESNO_BOX_GEOMETRY.get(question), list) else question
        semantics = QUESTION_SEMANTICS.get(question)
        if semantics and semantics.get("status") == "reviewed":
            status = "Already reviewed semantics; verify the geometry."
        else:
            status = "Semantics are pending review."
        prompts.append({
            "question": question,
            "key": key,
            "prompt": (
                f"Review question {question}: read the exact printed question "
                f"text, confirm page {page_idx + 1}, classify the answer type "
                f"(single-choice, multi-select, or open text), and confirm "
                f"the choice labels in visual order. The current manual "
                f"reference labels are {list(boxes)} for candidate "
                f"{candidate_idx}. {status}"
            ),
            "page": page_idx + 1,
            "candidate": candidate_idx,
            "kind": kind,
            "reference_labels": list(boxes),
            "status": semantics.get("status", "pending") if semantics else "pending",
        })
    return prompts


def question_semantics_reference():
    """Return reviewed semantics without exposing mutable module state."""
    return json.loads(json.dumps(QUESTION_SEMANTICS))


def validate_geometry(profile, min_confirm_rate=0.8, max_fit_residual=8.0):
    """Validate one profiled PDF without changing the manual baseline."""
    pages = profile.get("pages", [])
    residuals = [p.get("fit_residual_px") for p in pages
                 if p.get("fit_residual_px") is not None]
    rate = ((profile.get("boxes_confirmed", 0) / profile.get("boxes_total"))
            if profile.get("boxes_total") else 0.0)
    registration_ok = bool(pages) and all(
        p.get("marks_found") and
        (p.get("fit_residual_px") is None or
         p["fit_residual_px"] <= max_fit_residual)
        for p in pages
    )
    geometry_ok = registration_ok and rate >= min_confirm_rate
    return {
        "valid": geometry_ok,
        "registration_ok": registration_ok,
        "confirm_rate": round(rate, 4),
        "fit_residual_max": max(residuals) if residuals else None,
        "boxes_confirmed": profile.get("boxes_confirmed", 0),
        "boxes_total": profile.get("boxes_total", 0),
        "warnings": list(profile.get("warnings", [])),
    }


def discover_geometry(pdf_sources, dpi=RENDER_DPI, min_confirm_rate=0.8,
                      max_fit_residual=8.0):
    """Profile supplied PDFs and return validated geometry candidates.

    ``pdf_sources`` may contain paths, PDF bytes, or already-created profile
    dictionaries.  Candidates are retained per file/question rather than
    silently averaged: this makes scan-specific drift visible and leaves
    step4's manual geometry as the fallback of record.
    """
    profiles = []
    for source in pdf_sources:
        if isinstance(source, dict) and "questions" in source:
            profile = source
        elif isinstance(source, (bytes, bytearray)):
            profile = profile_pdf_bytes(bytes(source), dpi=dpi)
        else:
            profile = profile_pdf(str(source), dpi=dpi)
        validation = validate_geometry(profile, min_confirm_rate,
                                       max_fit_residual)
        profiles.append({"file": profile.get("file"), "source": profile.get("source"),
                         "validation": validation,
                         "questions": profile.get("questions", {})})

    candidates = {}
    unresolved = []
    for profile in profiles:
        valid = profile["validation"]["valid"]
        for key, geometry in profile["questions"].items():
            item = dict(geometry)
            item["file"] = profile["file"]
            item["validated"] = valid and geometry.get("boxes_total", 0) > 0 and (
                geometry.get("boxes_confirmed", 0) /
                geometry.get("boxes_total", 1) >= min_confirm_rate)
            candidates.setdefault(key, []).append(item)
    expected = {f"{q}#{ci}" if isinstance(_LEGACY_YESNO_BOX_GEOMETRY.get(q), list)
                else q
                for q, _pg, ci, _boxes, _kind in all_box_questions()}
    for key in sorted(expected | set(candidates)):
        observations = candidates.get(key, [])
        if not observations:
            unresolved.append({
                "question": key.split("#", 1)[0],
                "reason": "no supplied PDF produced a candidate for this question",
                "observed": [],
            })
            continue
        if not any(x["validated"] for x in observations):
            unresolved.append({"question": key.split("#", 1)[0],
                               "reason": "supplied PDFs did not validate the "
                                         "manual candidate geometry",
                               "observed": [x["file"] for x in observations]})

    return {
        "manual_reference": manual_geometry_reference(),
        "question_semantics": question_semantics_reference(),
        "profiles": profiles,
        "candidates": candidates,
        "unresolved": unresolved,
        "question_prompts": build_question_prompts(unresolved),
        "semantics_prompts": build_semantics_prompts(),
        "calibrator_version": CALIBRATOR_VERSION,
    }


def _pdf_text(pdf_bytes):
    """Extract page text without making text a prerequisite for calibration."""
    import pymupdf
    doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
    try:
        pages = []
        for page in doc:
            words = page.get_text("words")
            pages.append({
                "text": page.get_text("text"),
                "words": [
                    {"text": w[4], "bbox": [round(float(v), 2) for v in w[:4]]}
                    for w in words
                ],
            })
        return pages
    finally:
        doc.close()


def _connected_checkbox_rects(binary):
    """Find printed checkbox outlines without consulting the manual geometry.

    The outer contour of the 300-DPI boxes is substantially larger than glyph
    contours.  Nested contours are collapsed by centre proximity so the result
    is useful for templates with either open or lightly filled boxes.
    """
    import cv2
    contours, _ = cv2.findContours(binary, cv2.RETR_LIST, cv2.CHAIN_APPROX_SIMPLE)
    found = []
    for contour in contours:
        x, y, w, h = cv2.boundingRect(contour)
        area = cv2.contourArea(contour)
        if not (24 <= w <= 55 and 24 <= h <= 55 and abs(w - h) <= 16):
            continue
        if area < 350 or area / float(w * h) < 0.30:
            continue
        found.append((x, x + w, y, y + h, area))
    found.sort(key=lambda r: (-r[4], r[2], r[0]))
    selected = []
    for rect in found:
        cx = (rect[0] + rect[1]) / 2
        cy = (rect[2] + rect[3]) / 2
        if any(abs(cx - (r[0] + r[1]) / 2) < 10 and
               abs(cy - (r[2] + r[3]) / 2) < 10 for r in selected):
            continue
        selected.append(rect)
    return [[int(v) for v in r[:4]] for r in
            sorted(selected, key=lambda r: (r[2], r[0]))]


def _cluster_values(values, tolerance=12):
    groups = []
    for value in sorted(float(v) for v in values):
        if not groups or value - groups[-1][-1] > tolerance:
            groups.append([value])
        else:
            groups[-1].append(value)
    return [round(sum(group) / len(group), 2) for group in groups
            if group]


def _generated_page_geometry(page):
    import cv2
    import numpy as np
    rects = _connected_checkbox_rects(page["binary"])
    centers_x = _cluster_values([(r[0] + r[1]) / 2 for r in rects])
    sizes = [(r[1] - r[0], r[3] - r[2]) for r in rects]
    size = round(sum(min(w, h) for w, h in sizes) / len(sizes), 2) if sizes else None
    circles = cv2.HoughCircles(
        page["gray"], cv2.HOUGH_GRADIENT, dp=1.2, minDist=24,
        param1=80, param2=24, minRadius=10, maxRadius=24)
    circle_rects = []
    if circles is not None:
        for x, y, radius in np.round(circles[0]).astype(int):
            if not (12 <= radius <= 20 and y < page["gray"].shape[0] * 0.40):
                continue
            if any(abs(x - (r[0] + r[1]) / 2) < 12 and
                   abs(y - (r[2] + r[3]) / 2) < 12 for r in circle_rects):
                continue
            circle_rects.append([int(x - radius), int(x + radius),
                                 int(y - radius), int(y + radius)])
    return {
        "boxes": rects,
        "box_centers_x": centers_x,
        "box_expected_size": size,
        "circles": circle_rects,
    }


def _normalize_question_text(value):
    return re.sub(r"\s+", " ", (value or "")).strip().lower()


def _cluster_control_groups(rects, y_gap=12.0):
    if not rects:
        return []
    ordered = sorted(rects, key=lambda r: ((r[2] + r[3]) / 2.0, (r[0] + r[1]) / 2.0))
    groups, current = [], [ordered[0]]
    for rect in ordered[1:]:
        y_center = (rect[2] + rect[3]) / 2.0
        prev_center = (current[-1][2] + current[-1][3]) / 2.0
        if y_center - prev_center <= y_gap:
            current.append(rect)
        else:
            groups.append(current)
            current = [rect]
    groups.append(current)
    results = []
    for group in groups:
        xs = [p for rect in group for p in (rect[0], rect[1])]
        ys = [p for rect in group for p in (rect[2], rect[3])]
        x_span = max(xs) - min(xs)
        y_span = max(ys) - min(ys)
        if not group or x_span <= 0 or y_span <= 0:
            continue
        y_center = sum((r[2] + r[3]) / 2.0 for r in group) / len(group)
        x_center = sum((r[0] + r[1]) / 2.0 for r in group) / len(group)
        results.append({
            "count": len(group),
            "rects": group,
            "x_span": x_span,
            "y_span": y_span,
            "y_center": y_center,
            "x_center": x_center,
        })
    return results


def _page_word_anchors(page):
    words = []
    text_data = page.get("text") or {}
    if isinstance(text_data, dict):
        words = text_data.get("words") or []
    anchors = []
    for word in words:
        bbox = word.get("bbox") or []
        if len(bbox) != 4:
            continue
        x0, y0, x1, y1 = [float(v) for v in bbox]
        text = _normalize_question_text(word.get("text"))
        if not text:
            continue
        anchors.append({
            "text": text,
            "bbox": [x0, y0, x1, y1],
            "x": (x0 + x1) / 2.0,
            "y": (y0 + y1) / 2.0,
        })
    return anchors


def _question_rank(question_key):
    key = str(question_key)
    prefix_order = {"H1": 0, "H2": 1, "H3": 2, "H4": 3, "H5": 4, "H6": 5}
    if key in prefix_order:
        return (0, prefix_order[key])
    if key.isdigit():
        return (1, int(key))
    return (2, key)


def _auto_map_question_controls(observations, semantic_reference):
    """Map detected checkbox/circle control rows to survey question numbers.

    The mapping prefers exact label counts and repeated layout order, and only
    upgrades confidence when PyMuPDF word anchors are available. Ambiguous
    matches remain as review records instead of forcing a wrong runtime map.
    """
    question_order = [
        key for key in sorted(semantic_reference.keys(), key=_question_rank)
        if semantic_reference.get(key, {}).get("labels") or key == "H3"
    ]
    groups = []
    for item in observations:
        for page in item["pages"]:
            page_number = int(page.get("page", 1))
            for row in _cluster_control_groups(page.get("boxes", []), y_gap=12.0):
                if row["count"] < 2 or row["x_span"] < 600:
                    continue
                row["page"] = page_number
                row["kind"] = "yesno_box"
                row["page_words"] = _page_word_anchors(page)
                groups.append(row)
            for row in _cluster_control_groups(page.get("circles", []), y_gap=18.0):
                if row["count"] < 2 or row["x_span"] < 180:
                    continue
                row["page"] = page_number
                row["kind"] = "h3_circle"
                row["page_words"] = _page_word_anchors(page)
                groups.append(row)

    groups.sort(key=lambda row: (row["page"], row["y_center"]))
    assigned = {}
    unresolved = []
    seen_questions = set()

    for row in groups:
        candidates = []
        for question in question_order:
            if question in seen_questions:
                continue
            info = semantic_reference.get(question, {})
            labels = list(info.get("labels") or [])
            if not labels and question != "H3":
                continue
            if question == "H3":
                if row["kind"] != "h3_circle":
                    continue
                delta = abs(len(labels) - row["count"])
                if delta <= 1:
                    candidates.append((delta, _question_rank(question), question))
                continue
            if row["kind"] != "yesno_box":
                continue
            if info.get("answer_type") not in {"single-choice", "multi-select"}:
                continue
            delta = abs(len(labels) - row["count"])
            if delta <= 1:
                candidates.append((delta, _question_rank(question), question))
        if not candidates:
            continue
        _, _, question = min(candidates, key=lambda item: (item[0], item[1]))
        info = semantic_reference.get(question, {})
        labels = list(info.get("labels") or [])
        if not labels and question != "H3":
            unresolved.append({
                "question": question,
                "page": row["page"],
                "reason": "no label list available for this question",
                "confidence": 0.0,
            })
            continue

        score = 0.8 if row["count"] == len(labels) else 0.65
        anchor_hits = 0
        for word in row["page_words"]:
            if question == "H3" and word["text"] in {"h3", "setting"}:
                anchor_hits += 1
            if word["text"] == str(question).lower() or word["text"] == str(question):
                anchor_hits += 1
            for label in labels:
                if _normalize_question_text(label) == word["text"]:
                    anchor_hits += 1
        if anchor_hits:
            score = min(0.96, score + 0.12 * min(anchor_hits, 3))
        if row["page_words"]:
            score = min(0.97, score + 0.05)

        if score < 0.75:
            unresolved.append({
                "question": question,
                "page": row["page"],
                "reason": "count/order matched but layout confidence stayed below threshold",
                "confidence": round(score, 3),
                "count": row["count"],
                "expected_count": len(labels),
            })
            continue

        ordered_rects = sorted(row["rects"], key=lambda r: (r[0], r[2]))
        assigned[question] = {
            "page_idx": row["page"] - 1,
            "kind": row["kind"],
            "count": row["count"],
            "confidence": round(score, 3),
            "labels": labels,
            "rects": {
                label: ordered_rects[idx]
                for idx, label in enumerate(labels[:len(ordered_rects)])
            },
        }
        seen_questions.add(question)

    for question in question_order:
        if question in seen_questions or question in assigned:
            continue
        info = semantic_reference.get(question, {})
        labels = list(info.get("labels") or [])
        if labels or question == "H3":
            unresolved.append({
                "question": question,
                "page": None,
                "reason": "no high-confidence geometry match was found for this survey control",
                "confidence": 0.0,
                "expected_count": len(labels),
            })

    return {"maps": assigned, "unresolved": unresolved}



def generate_calibration_artifact(pdf_sources, dpi=RENDER_DPI,
                                  output_path=None, survey_questions=None):
    """Generate calibration data from one or more supplied PDFs.

    This is intentionally an explicit, local-only generator.  Registration
    marks, connected components, rendered geometry, and PyMuPDF text are
    observed from the supplied files; the hand-measured constants are retained
    only under ``manual_reference`` and are never copied into ``generated``.
    Missing labels or ambiguous control semantics are returned as review
    prompts rather than guessed.
    """
    import cv2
    import numpy as np
    semantic_reference = survey_semantics_reference(survey_questions)
    profiles, observations = [], []
    for source in pdf_sources:
        if isinstance(source, (bytes, bytearray)):
            data, name = bytes(source), "(bytes)"
        else:
            path = Path(source)
            data, name = path.read_bytes(), path.name
        pages = render_pages(data, dpi=dpi)
        text = _pdf_text(data)
        page_data, marks = [], []
        for index, page in enumerate(pages):
            detected = detect_registration_marks(page["binary"])
            marks.append([list(map(float, point)) for point in detected] if detected else None)
            page_data.append(_generated_page_geometry(page))
            otsu, _thresholded = cv2.threshold(
                page["gray"], 0, 255, cv2.THRESH_BINARY_INV | cv2.THRESH_OTSU)
            page_data[-1]["otsu_ink_threshold"] = float(otsu)
            page_data[-1]["page"] = index + 1
            page_data[-1]["text"] = text[index] if index < len(text) else {"text": "", "words": []}
        observations.append({"file": name, "marks": marks, "pages": page_data})
        profiles.append(profile_pdf_bytes(data, name=name, dpi=dpi))

    # A baseline must live in one PDF's coordinate system.  Median-averaging
    # marks across scans would combine each scan's translation and scale into
    # coordinates that exist in none of the supplied files.
    baseline = {
        str(index): marks
        for index, marks in enumerate(observations[0]["marks"])
        if marks
    } if observations else {}
    all_boxes = [r for item in observations for page in item["pages"] for r in page["boxes"]]
    widths = [r[1] - r[0] for r in all_boxes]
    heights = [r[3] - r[2] for r in all_boxes]
    grayscale_thresholds = [
        page["otsu_ink_threshold"]
        for item in observations
        for page in item["pages"]
        if page.get("otsu_ink_threshold") is not None
    ]
    generated = {
        "BASELINE_MARKS": baseline,
        "INK_THRESHOLD": round(float(np.median(grayscale_thresholds)), 2)
        if grayscale_thresholds else None,
        "GRID_COLUMN_CENTERS": _cluster_values(
            [(r[0] + r[1]) / 2 for r in all_boxes]),
        "GRID_BOX_EXPECTED_SIZE": round(float(np.median(widths + heights)), 2) if widths else None,
        "YESNO_BOX_CALIBRATION": {},
        "MULTISELECT_BOX_CALIBRATION": {},
        "H3_CIRCLE_CALIBRATION": {},
        # This is descriptive input only; the page_* keys below intentionally
        # retain their historical shape so existing consumers can still use
        # generated geometry when a reviewed mapping is unavailable.
        "QUESTION_SEMANTICS": semantic_reference,
        "page_geometry": [page for item in observations for page in item["pages"]],
    }
    # Labels cannot be safely inferred from pixels. Preserve detected controls
    # in visual order and reference the canonical semantic catalog in the
    # review record rather than inventing a question/box mapping.
    for page in generated["page_geometry"]:
        key = f"page_{page['page']}"
        generated["YESNO_BOX_CALIBRATION"][key] = {
            f"choice_{i + 1}": rect for i, rect in enumerate(page["boxes"])
        }
        generated["MULTISELECT_BOX_CALIBRATION"][key] = {
            f"choice_{i + 1}": rect for i, rect in enumerate(page["boxes"])
        }
        generated["H3_CIRCLE_CALIBRATION"][key] = {
            f"circle_{i + 1}": rect for i, rect in enumerate(page["circles"])
        }
    generated_python = "\n\n".join(
        f"{name} = {pprint.pformat(generated[name], sort_dicts=False)}"
        for name in (
            "BASELINE_MARKS", "INK_THRESHOLD", "GRID_COLUMN_CENTERS",
            "GRID_BOX_EXPECTED_SIZE", "YESNO_BOX_CALIBRATION",
            "MULTISELECT_BOX_CALIBRATION", "H3_CIRCLE_CALIBRATION",
        )
    )
    unresolved = [{
        "prompt": "Assign each generated page/choice control to a printed question "
                  "using QUESTION_SEMANTICS; confirm the listed labels and geometry "
                  "before promoting it to runtime calibration.",
        "pages": [p["page"] for item in observations for p in item["pages"]],
        "reason": "pixel geometry does not establish answer semantics",
        "question_semantics": semantic_reference,
    }, {
        "prompt": "Confirm which detected x-coordinate clusters form the six-column grid "
                  "and which rectangles belong to each question.",
        "reason": "box detection alone cannot distinguish the grid from question controls",
    }]
    artifact = {
        "artifact_version": "semantic-input-2",
        "inputs": [item["file"] for item in observations],
        "dpi": dpi,
        "semantic_source": (
            "step4_process_pdf.SURVEY_QUESTIONS"
            if survey_questions is None else "caller.survey_questions"
        ),
        # Page/choice keys are deliberately not promoted to step4's
        # question-keyed runtime maps until a reviewer maps them. Consumers
        # must continue using their built-in manual geometry as fallback.
        "runtime_compatible": False,
        "generated": generated,
        "generated_python": generated_python,
        "observations": observations,
        "manual_reference": manual_geometry_reference(),
        "question_semantics": semantic_reference,
        "review_prompts": unresolved + build_semantics_prompts(),
        "text_extraction": [p["text"] for item in observations for p in item["pages"]],
    }
    if output_path:
        Path(output_path).write_text(json.dumps(artifact, indent=2), encoding="utf-8")
    return artifact


# ==========================================================================
# folder-name -> partition date
# ==========================================================================
_MONTHS = {m.lower(): i for i, m in enumerate(
    ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]) if m}
_MONTHS.update({
    "january": 1, "february": 2, "march": 3, "april": 4, "june": 6, "july": 7,
    "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
})


def parse_folder_date(folder_name, default=None):
    """Best-effort date parse from a folder name. Handles 'Nov 10 2025',
    'Nov10_2025', 'November 10, 2025', '2025-11-10', '2025_11_10', and a bare
    month+day fragment with no year ('Nov1', 'Nov 10') by assuming the
    CURRENT year. Returns `default` (a date) if nothing recognisable is
    found - the caller supplies today's date as that default, per spec: a
    file with no parseable folder gets the run's own date."""
    if not folder_name:
        return default
    s = folder_name.strip()

    # ISO-ish: 2025-11-10, 2025_11_10, 20251110
    m = re.search(r"(\d{4})[-_/]?(\d{1,2})[-_/](\d{1,2})\b", s)
    if m:
        try:
            return _dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    # Month name + day (+ optional year): "Nov 10 2025", "Nov10_2025",
    # "November 10, 2025", "Nov1" (day 1, no year), "Nov10" (day 10, no year)
    m = re.search(
        r"([A-Za-z]{3,9})\.?\s*[_ ]?(\d{1,2})(?:(?:st|nd|rd|th)?)[,_ ]*\s*(\d{4})?",
        s,
    )
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
# geometry
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
    """The four solid ~74px corner squares, as (TL, TR, BL, BR) centres, or
    None unless exactly one plausible mark is found in each quadrant."""
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
    """Least-squares similarity mapping src->dst, as (z, src_centroid,
    dst_centroid) where z is a complex scale*rotation."""
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
    """Finds one box-sized closed rectangle inside the window, or None."""
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
    """Splits measured ink ratios into a blank cluster and a marked cluster.
    See classify_measured() callers for why the split is constrained to a
    plausible marked-box count rather than taken as the plain widest gap -
    EXCEPT when a genuinely clear (>= CLEAR_GAP) separation exists anywhere
    in the full sorted array, in which case that real gap is used even if
    it falls outside the [MIN_PLAUSIBLE_MARKED, MAX_PLAUSIBLE_MARKED]
    window (bug fix, explicit user request: confirmed on a real file,
    2025_Nov_23_5_TPS_4047.pdf, where only 6 of this file's 72 confirmed
    boxes were genuinely marked - well under MIN_PLAUSIBLE_MARKED=8 - with
    an unmistakable 0.2067 gap separating them from every blank box. The
    old windowed-only search could never see that gap (index 66 sits past
    hi_k=64), so it was forced to pick the best gap it COULD find inside
    the artificial [47, 64] window - a mere 0.0176, which mislabeled 3
    genuinely blank boxes as "marked" just to satisfy the >=8 floor, and
    reported threshold=0.1352 sitting in the middle of the blank
    population instead of near the true ~0.25 boundary. A real, wide gap
    is strong evidence on its own regardless of how many boxes end up on
    either side of it - the plausible-count window exists to avoid
    over-trusting a NARROW, noise-sized gap at an implausible split point,
    not to override an unambiguous one. Only falls back to the original
    windowed search when no gap anywhere in the full array reaches
    CLEAR_GAP, preserving the prior "tight"/forced-split behavior for
    files that genuinely have no clean separation at all."""
    import numpy as np
    r = np.array(sorted(ratios), float)
    n = len(r)
    if n < 12:
        return None

    # First pass: look for a genuinely clear gap ANYWHERE in the full
    # array (k from 1 to n-1, i.e. every possible split point) - not
    # confined to the plausible-count window. Prefer the WIDEST such gap;
    # among ties, the one closest to the plausible window (so a genuinely
    # ambiguous file with two similarly-wide clear gaps still prefers the
    # more plausible split point, rather than an arbitrary earliest-match).
    lo_k = max(1, n - MAX_PLAUSIBLE_MARKED)
    hi_k = min(max(lo_k + 1, n - MIN_PLAUSIBLE_MARKED), n - 1)
    clear_candidates = []
    for k in range(1, n):
        gap = float(r[k] - r[k - 1])
        if gap >= CLEAR_GAP:
            in_window = lo_k <= k <= hi_k
            clear_candidates.append((gap, in_window, k))
    if clear_candidates:
        # Sort by (widest gap first, in-window preferred as tiebreaker).
        clear_candidates.sort(key=lambda c: (-c[0], not c[1]))
        best_gap, _in_window, best_k = clear_candidates[0]
        blank_hi, mark_lo = float(r[best_k - 1]), float(r[best_k])
        n_marked = n - best_k
        return {"threshold": round((blank_hi + mark_lo) / 2.0, 4),
                "blank_hi": round(blank_hi, 4), "mark_lo": round(mark_lo, 4),
                "gap": round(best_gap, 4), "n_marked": n_marked, "quality": "clear"}

    # Fallback: no clear gap anywhere - original windowed-best-gap search,
    # unchanged from before this fix.
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


def profile_pdf_bytes(pdf_bytes, name="(bytes)", source=None, folder=None, dpi=RENDER_DPI):
    """Full calibration profile for one PDF. Never raises on a page that
    doesn't look like this form; it reports what it could and could not
    confirm."""
    import numpy as np
    pages = render_pages(pdf_bytes, dpi=dpi)
    result = {"file": name, "source": source or name, "folder": folder,
              "page_count": len(pages), "pages": [], "questions": {}, "warnings": []}

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
                entry["offset_dx"] = round(marks[0][0] - base[0][0], 1)
                entry["offset_dy"] = round(marks[0][1] - base[0][1], 1)
                entry["fit_residual_px"] = round(transform_residual(xf, base, marks), 2)
                if entry["fit_residual_px"] > 8:
                    result["warnings"].append(
                        f"p{idx+1}: registration fit residual {entry['fit_residual_px']}px is high "
                        "- the marks do not form the expected rigid quad; boxes on this page "
                        "are reported but should be treated with suspicion")
        else:
            result["warnings"].append(
                f"p{idx+1}: registration marks not found - no transform for this page, "
                "so its questions get no profile and fall back to the pipeline's own search")
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
            else:
                rects[label] = [int(ax0), int(ax1), int(ay0), int(ay1)]
        key = f"{q}#{cand_idx}" if isinstance(_LEGACY_YESNO_BOX_GEOMETRY.get(q), list) else q
        result["questions"][key] = {
            "question": q, "page_idx": pg_idx, "candidate": cand_idx, "kind": kind,
            "boxes_total": len(boxes), "boxes_confirmed": confirmed,
            "baseline_correction": [cdx, cdy], "rects": rects,
            "ink_ratios": {l: round(v, 4) for l, v in zip(rects, q_ratios)} if q_ratios else {},
        }

    # Some questions have alternative printed layouts.  A PDF contains one
    # active layout, so count and threshold only the candidate with the
    # strongest confirmed geometry.  Keep inactive candidates in the profile
    # for diagnostics, but exclude them from file-level totals.
    active_keys = set(result["questions"])
    for question, raw in _LEGACY_YESNO_BOX_GEOMETRY.items():
        if not isinstance(raw, list):
            continue
        keys = [f"{question}#{index}" for index in range(len(raw))
                if f"{question}#{index}" in result["questions"]]
        if not keys:
            continue
        active = max(
            keys,
            key=lambda key: (
                result["questions"][key]["boxes_confirmed"],
                result["questions"][key]["boxes_confirmed"] /
                max(result["questions"][key]["boxes_total"], 1),
            ),
        )
        for key in keys:
            result["questions"][key]["active_layout"] = key == active
            if key != active:
                active_keys.discard(key)
        result.setdefault("active_layouts", {})[question] = active
        if len(keys) > 1:
            result["warnings"].append(
                f"q{question}: counted active layout {active}; "
                f"excluded alternative layout(s) "
                f"{', '.join(key for key in keys if key != active)}")

    ratios = [
        ratio
        for key, question in result["questions"].items()
        if key in active_keys
        for ratio in question.get("ink_ratios", {}).values()
    ]
    ink = _split_clusters(ratios) if ratios else None
    result["ink"] = ink
    result["boxes_confirmed"] = sum(
        v["boxes_confirmed"] for key, v in result["questions"].items()
        if key in active_keys)
    result["boxes_total"] = sum(
        v["boxes_total"] for key, v in result["questions"].items()
        if key in active_keys)
    if ink is None and ratios:
        result["warnings"].append(
            "no blank/marked ink separation could be measured on this file - no per-file "
            "threshold emitted; the pipeline keeps its own global defaults")
    elif ink and ink.get("quality") == "tight":
        result["warnings"].append(
            f"blank and marked ink nearly touch (gap {ink['gap']}, {ink['n_marked']} boxes "
            "read as marked) - typical of a light-checkmark scan. Not safe to threshold on.")
    return result


def profile_pdf(path, dpi=RENDER_DPI):
    p = Path(path)
    return profile_pdf_bytes(p.read_bytes(), name=p.name, source=str(p),
                             folder=p.parent.name, dpi=dpi)


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
    """Every .pdf under a prefix, recursively. Anchored at a folder boundary
    so '.../test_pdf' does not also sweep '.../test_pdf_merged/'."""
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
    """Folder name of a GCS blob RELATIVE to the swept prefix, or None if the
    blob sits directly under the prefix with no subfolder."""
    _, root_blob = parse_gcs_uri(root_uri)
    _, blob = parse_gcs_uri(uri)
    root_prefix = root_blob if root_blob.endswith("/") else root_blob.rsplit("/", 1)[0] + "/"
    rel = blob[len(root_prefix):] if blob.startswith(root_prefix) else blob
    parts = rel.split("/")
    return parts[0] if len(parts) > 1 else None


def profile_gcs(uri, root_uri=None, client=None, project=None, dpi=RENDER_DPI):
    bucket, blob_name = parse_gcs_uri(uri)
    client = client or _gcs_client(project)
    data = client.bucket(bucket).blob(blob_name).download_as_bytes()
    folder = _gcs_folder(root_uri, uri) if root_uri else os.path.basename(os.path.dirname(blob_name))
    return profile_pdf_bytes(data, name=os.path.basename(blob_name), source=uri,
                             folder=folder, dpi=dpi)


# ==========================================================================
# BigQuery
# ==========================================================================
BQ_SCHEMA = [
    ("gcs_uri", "STRING", "Full gs:// URI (or local path) of the profiled PDF"),
    ("file_name", "STRING", "PDF file name"),
    ("survey_folder", "STRING", "Date folder the PDF sits in, or NULL if none"),
    ("partition_date", "DATE",
     "Date parsed from survey_folder; today's date (this run) when the file "
     "has no folder or the folder name couldn't be parsed. The table is "
     "partitioned on this column."),
    ("page_count", "INTEGER", None),
    ("paper_sizes", "STRING", "Distinct page sizes, e.g. 'Letter'"),
    ("marks_found_pages", "STRING", "Per page: whether all 4 registration marks were found"),
    ("scale_p1", "FLOAT", "Page 1 uniform scale vs the calibration baseline"),
    ("scale_p2", "FLOAT", None),
    ("rotation_deg_p1", "FLOAT", "Page 1 rotation vs baseline, degrees"),
    ("rotation_deg_p2", "FLOAT", None),
    ("offset_dx_p1", "FLOAT", "Page 1 translation vs baseline, px at 300 DPI"),
    ("offset_dy_p1", "FLOAT", None),
    ("offset_dx_p2", "FLOAT", None),
    ("offset_dy_p2", "FLOAT", None),
    ("fit_residual_p1", "FLOAT", "Max registration-fit residual, px. >8 means distrust this page"),
    ("fit_residual_p2", "FLOAT", None),
    ("paper_level_p1", "FLOAT", "Background brightness 0-255"),
    ("ink_threshold", "FLOAT", "Per-file blank/marked ink cut. NULL when not clearly separable."),
    ("ink_blank_hi", "FLOAT", "Highest ink ratio in the blank cluster"),
    ("ink_mark_lo", "FLOAT", "Lowest ink ratio in the marked cluster"),
    ("ink_gap", "FLOAT", "mark_lo - blank_hi. A small gap means a risky scan."),
    ("ink_quality", "STRING", "'clear' / 'tight' / NULL - see _split_clusters()"),
    ("ink_n_marked", "INTEGER", "Boxes above the threshold - a sanity check on the split"),
    ("boxes_confirmed", "INTEGER", "Checkboxes located at their predicted position"),
    ("boxes_total", "INTEGER", "Checkboxes attempted"),
    ("confirm_rate", "FLOAT", "boxes_confirmed / boxes_total"),
    ("question_rects_json", "STRING",
     "JSON {question_key: {page_idx, boxes_confirmed, boxes_total, rects}} in "
     "this file's own pixel space at 300 DPI."),
    ("warnings", "STRING", "Why any part of this profile is unreliable; ' | ' separated"),
    ("calibrator_version", "STRING", "Which calibrator produced the row"),
    ("profiled_at", "TIMESTAMP", "When this profiling run ran (UTC)"),
]
BQ_FIELDS = [n for n, _t, _d in BQ_SCHEMA]


def to_row(r, run_date):
    pages = {p["page"]: p for p in r["pages"]}
    p1, p2 = pages.get(1, {}), pages.get(2, {})
    ink = r.get("ink") or {}
    qj = {k: {"page_idx": v["page_idx"], "candidate": v["candidate"], "kind": v["kind"],
              "boxes_confirmed": v["boxes_confirmed"], "boxes_total": v["boxes_total"],
              "baseline_correction": v["baseline_correction"], "rects": v["rects"]}
          for k, v in r["questions"].items()}
    uri = r.get("source") or ""
    total = r.get("boxes_total") or 0
    folder = r.get("folder")
    partition_date = parse_folder_date(folder, default=run_date)
    return {
        "gcs_uri": uri, "file_name": r["file"], "survey_folder": folder,
        "partition_date": partition_date,
        "page_count": r.get("page_count"),
        "paper_sizes": "|".join(sorted({p.get("paper", "?") for p in r["pages"]})),
        "marks_found_pages": "|".join("yes" if p.get("marks_found") else "no" for p in r["pages"]),
        "scale_p1": p1.get("scale"), "scale_p2": p2.get("scale"),
        "rotation_deg_p1": p1.get("rotation_deg"), "rotation_deg_p2": p2.get("rotation_deg"),
        "offset_dx_p1": p1.get("offset_dx"), "offset_dy_p1": p1.get("offset_dy"),
        "offset_dx_p2": p2.get("offset_dx"), "offset_dy_p2": p2.get("offset_dy"),
        "fit_residual_p1": p1.get("fit_residual_px"), "fit_residual_p2": p2.get("fit_residual_px"),
        "paper_level_p1": p1.get("paper_level"),
        "ink_threshold": ink.get("threshold"), "ink_blank_hi": ink.get("blank_hi"),
        "ink_mark_lo": ink.get("mark_lo"), "ink_gap": ink.get("gap"),
        "ink_quality": ink.get("quality"), "ink_n_marked": ink.get("n_marked"),
        "boxes_confirmed": r.get("boxes_confirmed"), "boxes_total": total,
        "confirm_rate": round(r.get("boxes_confirmed", 0) / total, 4) if total else None,
        "question_rects_json": json.dumps(qj, separators=(",", ":")),
        "warnings": " | ".join(r.get("warnings", [])),
        "calibrator_version": CALIBRATOR_VERSION,
        "profiled_at": _dt.datetime.now(_dt.timezone.utc),
    }


def to_dataframe(results, run_date=None):
    import pandas as pd
    run_date = run_date or _dt.date.today()
    return pd.DataFrame([to_row(r, run_date) for r in results], columns=BQ_FIELDS)


def to_bigquery(results_or_df, table=BQ_TABLE, project=None, client=None,
                write_disposition="WRITE_APPEND", location=None, run_date=None):
    """Loads profiles into a DATE-partitioned BigQuery table (partitioned on
    partition_date). Default is WRITE_APPEND, not WRITE_TRUNCATE: with a
    partitioned table the natural operation is adding this run's dates, not
    replacing the whole table's history every time."""
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
        else:
            df[name] = df[name].astype("string")

    # table is always "<project>.<dataset>.<table_name>" - parse it explicitly and build every dataset/table reference off that parsed project, never a
    # bare "project.dataset" string. Some environments (this one included) run with an ambient default GCP project on the BigQuery client that differs 
    # from   the project actually named in `table`. A bare-string Dataset/DatasetReference can silently resolve against that ambient default instead of the
    # embedded one, producing "Invalid resource name projects/<ambient>". Explicit DatasetReference(project, dataset_id) removes the ambiguity.
    table_dataset, table_name = table.split(".")
    table_project = pipeline_config.GCP_PROJECT_ID
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

    schema = [bigquery.SchemaField(n, t, description=d) for n, t, d in BQ_SCHEMA]
    try:
        existing = client.get_table(table)
        table_exists = True
    except Exception:                                           # noqa: BLE001
        existing = None
        table_exists = False

    if table_exists:
        # An existing table (created before partition_date/other new columns existed) needs those columns added BEFORE the load, or BigQuery
        # refuses the job with "Cannot add fields". ALTER the live schema to the union of what's there plus BQ_SCHEMA, additive only - never
        # drops a column an older run may still rely on.
        existing_names = {f.name for f in existing.schema}
        missing = [f for f in schema if f.name not in existing_names]
        if missing:
            existing.schema = list(existing.schema) + missing
            client.update_table(existing, ["schema"])
            print(f"added column(s) to {table}: {[f.name for f in missing]}")

    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition=write_disposition)
    if not table_exists:
        # Only set on CREATE - BigQuery rejects time_partitioning on an
        # already-partitioned table's load job.
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="partition_date")
        print(f"creating {table} partitioned on partition_date (DAY)")

    job = client.load_table_from_dataframe(df, table, location=job_location, job_config=job_config)
    job.result()
    print(f"loaded {len(df)} profile(s) into {table} ({write_disposition}), "
          f"partitions: {sorted(set(str(d) for d in df['partition_date'].dropna()))}")
    return len(df)


# ==========================================================================
# sweep
# ==========================================================================
def run(root_uri, table=BQ_TABLE, project=None, dpi=RENDER_DPI,
        write_disposition="WRITE_APPEND", verbose=True, dry_run=False):
    """List every PDF under a prefix (or a local directory), profile them
    all, load the partitioned BigQuery table. Returns the DataFrame."""
    run_date = _dt.date.today()

    if root_uri.startswith(("gs://", "https://")):
        client = _gcs_client(project)
        uris = list_gcs_pdfs(root_uri, client=client)
        if not uris:
            raise FileNotFoundError(f"no PDFs found under {root_uri}")
        getter = lambda u: profile_gcs(u, root_uri=root_uri, client=client, dpi=dpi)  # noqa: E731
        folder_of = lambda u: _gcs_folder(root_uri, u)                               # noqa: E731
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
        if not uris:
            raise FileNotFoundError(f"no PDFs found under {root_uri}")
        _root_resolved = Path(root_uri).resolve()
        folder_of = lambda u: (Path(u).parent.name                                   # noqa: E731
                               if Path(u).resolve().parent != _root_resolved else None)
        # profile_pdf() alone would always set folder=p.parent.name, even when
        # that parent IS the swept root (no real subfolder) - use folder_of()
        # here so a file sitting directly under root_uri correctly gets
        # folder=None instead of the root directory's own name.
        getter = lambda u: profile_pdf_bytes(                                        # noqa: E731
            Path(u).read_bytes(), name=Path(u).name, source=str(u),
            folder=folder_of(u), dpi=dpi)

    from collections import Counter
    counts = Counter(folder_of(u) or "(no folder)" for u in uris)
    print(f"{len(uris)} PDF(s) across {len(counts)} folder(s) under {root_uri}")
    for folder, n in sorted(counts.items()):
        d = parse_folder_date(None if folder == "(no folder)" else folder, default=run_date)
        print(f"    {folder:<24} {n:>3}  -> partition_date {d}")
    print()

    results = []
    for u in uris:
        try:
            r = getter(u)
        except Exception as e:                                  # noqa: BLE001
            r = {"file": os.path.basename(u), "source": u, "folder": folder_of(u),
                 "pages": [], "questions": {}, "page_count": None,
                 "boxes_confirmed": 0, "boxes_total": 0, "ink": None,
                 "warnings": [f"could not be profiled: {e}"]}
        results.append(r)
        if verbose:
            tot = r.get("boxes_total") or 0
            rate = f'{r.get("boxes_confirmed",0)}/{tot}' if tot else "-"
            ink = (r.get("ink") or {}).get("threshold")
            print(f"  {r['file'][:34]:36} boxes {rate:>8}   ink_thresh "
                  f"{ink if ink is not None else '-':>6}"
                  + ("   WARN" if r.get("warnings") else ""))

    df = to_dataframe(results, run_date=run_date)
    if dry_run:
        print("\n[dry-run] not writing to BigQuery")
        return df
    to_bigquery(df, table=table, project=project, write_disposition=write_disposition, run_date=run_date)
    return df


def explain(result):
    """Readable summary of one profile."""
    print(f"{result['file']}   {result.get('boxes_confirmed')}/{result.get('boxes_total')} boxes confirmed"
          f"   folder={result.get('folder')}"
          f"   partition_date={parse_folder_date(result.get('folder'), default=_dt.date.today())}")
    for p in result["pages"]:
        if p.get("marks_found"):
            print(f"  p{p['page']}: {p['paper']}, scale {p.get('scale')}, "
                  f"rot {p.get('rotation_deg')}deg, offset ({p.get('offset_dx')},{p.get('offset_dy')})px, "
                  f"residual {p.get('fit_residual_px')}px")
        else:
            print(f"  p{p['page']}: {p['paper']}, registration marks NOT found")
    ink = result.get("ink")
    if ink:
        print(f"  ink [{ink['quality']}]: threshold {ink['threshold']} "
              f"(blank<={ink['blank_hi']}, mark>={ink['mark_lo']}, gap {ink['gap']}, "
              f"{ink['n_marked']} marked)")
    else:
        print("  ink: no clear separation - pipeline default retained")
    for w in result.get("warnings", []):
        print(f"  ! {w}")


if __name__ == "__main__":
    # Notebook export entry point.  Keeping this out of module import makes
    # the calibration functions usable by validation jobs and tests without
    # contacting GCS or Spark.
    df = run("gs://syntasa-saas/syn-workspace/users/christine.zhao@syntasa.com/notebooks/test_pdf/")
    df["event_partition"] = df["partition_date"]

    spark_df = spark.createDataFrame(df)
    spark_df.show(1, truncate=False)
    writeToEventStore(spark_df, '@OutputTable1', 1, "event_partition")
