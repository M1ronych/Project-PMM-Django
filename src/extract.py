import re
import pandas as pd
import numpy as np


FUEL_RE = re.compile(
    r"\bбензин\b|\bдиз(ель|паливо)\b|\bмасло\b|\bмастил|\bпропан\b|\bбутан\b|\bскраплен|\bА-\s?\d{2}\b|\bгаз\b(?!-)",
    re.IGNORECASE
)

def norm_text(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    return s if s else None

def is_numeric(x):
    return isinstance(x, (int, float, np.integer, np.floating)) and not pd.isna(x)

def numeric_count(row, cols):
    return sum(is_numeric(row[c]) for c in cols)

def looks_like_vehicle(text):
    if not text:
        return False
    patterns = [
        r"\bУАЗ\b",
        r"\bГАЗ-?\d",
        r"\bЗІЛ-?\d",
        r"\bВАЗ\b",
        r"\bPEUGEOT\b|\bRENAULT\b|\bFORD\b|\bMAN\b|\bSCANIA\b",
        r"\b[А-ЯA-Z]{2}\s?\d{2}-\d{2}\s?[А-ЯA-Z]{2}\b",
    ]
    return any(re.search(p, text, re.IGNORECASE) for p in patterns)

def is_info_header(text):
    if not text:
        return False
    return bool(re.search(r"Розрахунок|Миргородводоканал|фактичні витрати|планові витрати", text, re.IGNORECASE))

def extract_pmm_table(
    excel_path: str,
    sheet_name: str,
    section_keywords: list[str] | None = None
) -> pd.DataFrame:
    raw = pd.read_excel(excel_path, sheet_name=sheet_name, header=None)

    section_keywords = section_keywords or []
    section_keywords = [k.lower() for k in section_keywords]

    numeric_cols = list(range(1, raw.shape[1]))

    current_section = None
    current_category = None
    current_fuel = None

    records = []

    for idx in range(raw.shape[0]):
        text = norm_text(raw.iat[idx, 0])
        if not text or is_info_header(text):
            continue

        row = raw.iloc[idx]
        ncount = numeric_count(row, numeric_cols)

        is_heading = (ncount == 0) and (not FUEL_RE.search(text)) and (not looks_like_vehicle(text))
        if is_heading:
            t = text.lower()
            if any(k in t for k in section_keywords):
                current_section = text
                current_category = None
            else:
                current_category = text
            current_fuel = None
            continue

        if FUEL_RE.search(text) and (not looks_like_vehicle(text)):
            current_fuel = text
            vals = [row.get(c, np.nan) for c in range(1, 9)]
            records.append({
                "row_idx": idx,
                "section": current_section,
                "category": current_category,
                "fuel": current_fuel,
                "record_type": "fuel_total",
                "vehicle": None,
                **{f"c{i}": vals[i-1] if i-1 < len(vals) else np.nan for i in range(1, 9)}
            })
            continue

        if ncount >= 1 or looks_like_vehicle(text):
            vehicle = text
            vals = [row.get(c, np.nan) for c in range(1, 9)]
            records.append({
                "row_idx": idx,
                "section": current_section,
                "category": current_category,
                "fuel": current_fuel,
                "record_type": "vehicle",
                "vehicle": vehicle,
                **{f"c{i}": vals[i-1] if i-1 < len(vals) else np.nan for i in range(1, 9)}
            })

    return pd.DataFrame(records)


if __name__ == "__main__":
    df = extract_pmm_table(
        excel_path="data_in/пмм.xlsx",
        sheet_name="ПММ (Мрг)",
        section_keywords=["водопостачання", "паливо", "водовідведення"]
    )

    df.to_csv("data_out/parsed.csv", index=False, encoding="utf-8-sig")
    print(df.head(20))
    print("Rows:", len(df))
    print("Saved:", "data_out/parsed.csv")