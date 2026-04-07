from decimal import Decimal, InvalidOperation
from pathlib import Path
from io import StringIO
import re
import pandas as pd

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from pmm.models import Section, Fuel, Vehicle, ImportBatch, PmmRecord


def to_decimal(x) -> Decimal | None:
    if x is None or pd.isna(x):
        return None
    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return None
    s = s.replace("\xa0", " ").replace(" ", "").replace(",", ".")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None

def _read_bytes(path: str) -> bytes:
    return Path(path).read_bytes()


def _decode_1c_text(raw: bytes) -> str:
    
    # Decode likely 1C export text.

    # Important: cp1251 may "decode" UTF-8 without errors and produce 'РџРµ...'.
    # So we try several encodings and choose the one that *looks* like 1C:
    # contains period+debit+credit keywords.
    
    encoding = ("utf-8-sig", "utf-8", "cp1251", "windows-1251")
    best_text = None
    best_score = -1

    keywords = ("дебет", "кредит", "період", "период", "документ", "рахунку", "счета")

    # 1С майже завжди cp1251/windows-1251, UTF-8 пробуємо потім
    encodings = ("cp1251", "windows-1251", "utf-8-sig", "utf-8")
    for enc in encodings:
        try:
            t = raw.decode(enc, errors="replace")
        except Exception:
            continue

        low = t.lower()
        score = sum(1 for k in keywords if k in low)
        if ("дебет" in low) and ("кредит" in low):
            score += 3
        if ("період" in low) or ("период" in low):
            score += 2

        if score > best_score:
            best_score = score
            best_text = t

    return best_text if best_text is not None else raw.decode("cp1251", errors="replace")

def _detect_source_type(csv_path: str) -> str:
# Detect if input is a 1C account report or a prepared PMM CSV.
# Returns: "1C" or "PMM"
   
    raw = _read_bytes(csv_path)
    text = _decode_1c_text(raw).lower()

    # 1C report markers
    if (("картка рахунку" in text) or ("карточка счета" in text)) and ("дебет" in text) and ("кредит" in text):
        return "1C"

    # PMM: try to read as CSV with different separators/encodings and check required columns
    for enc in ("utf-8-sig", "utf-8", "cp1251", "windows-1251"):
        for sep in (",", ";", "\t"):
            try:
                df = pd.read_csv(StringIO(raw.decode(enc, errors="replace")), sep=sep, engine="python")
                cols = {str(c).strip().lower() for c in df.columns}
                if {"section", "fuel", "vehicle"}.issubset(cols):
                    return "PMM"
            except Exception:
                pass

    # Default to PMM: later we'll fail with a clear "Missing columns" error if it's not PMM
    return "PMM"

def _has_period(s: str) -> bool:
    s = s.lower()
    return ("период" in s) or ("період" in s) or ("перiод" in s)


def _read_1c_report_as_pmm_df(csv_path: str) -> pd.DataFrame:
    """
    Read a 1C account report and convert it into PMM-like dataframe
    with columns:
      section, fuel, vehicle, fact_qty, fact_amount, plan_qty, plan_amount, price, delta
    """
    raw = _read_bytes(csv_path)
    text = _decode_1c_text(raw)
    lines = text.splitlines()

    # Find header line (Period + Debit + Credit)
    header_line = None
    for i, line in enumerate(lines):
        low = line.lower().replace("\ufeff", "").strip()
        if _has_period(low) and ("дебет" in low) and ("кредит" in low):
            header_line = i
            break

    if header_line is None:
        head30 = "\n".join(lines[:30])
        raise ValueError(
            "Не найдена строка заголовка 1C (Період/Период + Дебет + Кредит).\n"
            f"Первые 30 строк файла:\n{head30}"
        )

    # Read table from header onward
    data_text = "\n".join(lines[header_line:])
    df = pd.read_csv(StringIO(data_text), sep=";", engine="python", dtype=str)
    df.columns = [str(c).strip().replace("\ufeff", "") for c in df.columns]
    cols_l = [c.lower() for c in df.columns]

    def find_col_contains(*needles: str) -> str | None:
        needles_l = [n.lower() for n in needles]
        for orig, low in zip(df.columns, cols_l):
            if any(n in low for n in needles_l):
                return orig
        return None

    col_an_dt = find_col_contains("аналітика дт", "аналитика дт")
    col_an_kt = find_col_contains("аналітика кт", "аналитика кт")
    col_debit = find_col_contains("дебет")
    col_credit = find_col_contains("кредит")

    if (col_debit is None) or (col_credit is None):
        raise ValueError(f"Не знайшов колонки Дебет/Кредит. Колонки: {list(df.columns)}")

    def to_num(x):
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return None
        s = str(x).strip()
        if s == "" or s.lower() in ("nan", "none"):
            return None
        s = s.replace("\xa0", " ").replace(" ", "").replace(",", ".")
        try:
            return float(s)
        except Exception:
            return None

    out = pd.DataFrame()
    # ВАЖЛИВО: fillna ДО astype(str), інакше NaN стає строкою "nan".
    out["section"] = df[col_an_dt].fillna("").astype(str).str.strip() if col_an_dt else ""
    out["fuel"] = df[col_an_kt].fillna("").astype(str).str.strip() if col_an_kt else ""

    # Знімаємо типові сміттєві строкові значення
    out["section"] = out["section"].replace(to_replace=r"^\s*(nan|none)\s*$", value="", regex=True)
    out["fuel"] = out["fuel"].replace(to_replace=r"^\s*(nan|none)\s*$", value="", regex=True)

    # vehicle поки нема через 1С-шного звіту, залишаємо порожнім
    out["vehicle"] = ""

    # Map 1C debit/credit into PMM numeric fields (adjust later if you want different meaning)
    out["fact_qty"] = df[col_debit].map(to_num)
    out["fact_amount"] = df[col_credit].map(to_num)

    out["plan_qty"] = None
    out["plan_amount"] = None
    out["price"] = None
    out["delta"] = None

    # Drop empty analytics rows and rows with both debit/credit == 0
    out = out[(out["section"] != "") | (out["fuel"] != "")]
    out = out[(out["fact_qty"].fillna(0) != 0) | (out["fact_amount"].fillna(0) != 0)]

    return out


# -------------------------
# Command
# -------------------------


class Command(BaseCommand):
    help = "Import PMM records from a prepared CSV (PMM) or a 1C account report."

    def add_arguments(self, parser):
        parser.add_argument("csv_path", type=str, help="Path to CSV file")

    @transaction.atomic
    def handle(self, *args, **options):
        csv_path = Path(options["csv_path"])
        if not csv_path.exists():
            raise CommandError(f"CSV not found: {csv_path}")

        # 1) detect source type
        source_type = _detect_source_type(str(csv_path))

        # 2) read dataframe
        if source_type == "1C":
            df = _read_1c_report_as_pmm_df(str(csv_path))
        else:
            df = None
            last_err = None

            for enc in ("utf-8-sig", "utf-8", "cp1251", "windows-1251"):
                for sep in (",", ";", "\t"):
                    try:
                        df = pd.read_csv(csv_path, sep=sep, encoding=enc, engine="python")
                        break
                    except Exception as e:
                        last_err = e
                if df is not None:
                    break

            if df is None:
                raise CommandError(f"Не удалось прочитать CSV как PMM. Последняя ошибка: {last_err}")

            df.columns = [str(c).strip().lower() for c in df.columns]

        # For 1C path, normalize too (it already matches, but keep consistent)
        df.columns = [str(c).strip().lower() for c in df.columns]

        required = {"section", "fuel", "vehicle", "fact_qty", "fact_amount", "plan_qty", "plan_amount", "price", "delta"}
        missing = required - set(df.columns)
        if missing:
            raise CommandError(f"Missing columns in CSV: {sorted(missing)}")

        batch = ImportBatch.objects.create(
            source_filename=csv_path.name,
            total_rows=len(df),
        )

        imported = 0
        errors = 0

        section_cache: dict[str, Section] = {}
        fuel_cache: dict[str, Fuel] = {}
        vehicle_cache: dict[str, Vehicle] = {}

        records: list[PmmRecord] = []

        for i, row in df.iterrows():
            try:
                # Section (optional)
                section_name = row.get("section")
                if section_name is None or pd.isna(section_name) or str(section_name).strip() == "":
                    section_obj = None
                else:
                    section_name = str(section_name).strip()
                    section_obj = section_cache.get(section_name)
                    if section_obj is None:
                        section_obj, _ = Section.objects.get_or_create(name=section_name)
                        section_cache[section_name] = section_obj

                # Fuel (required)
                fuel_name = str(row["fuel"]).strip()
                fuel_obj = fuel_cache.get(fuel_name)
                if fuel_obj is None:
                    fuel_obj, _ = Fuel.objects.get_or_create(name=fuel_name)
                    fuel_cache[fuel_name] = fuel_obj

                # Vehicle (required)
                vehicle_name = str(row["vehicle"]).strip() or "N/A"
                vehicle_obj = vehicle_cache.get(vehicle_name)
                if vehicle_obj is None:
                    vehicle_obj, _ = Vehicle.objects.get_or_create(name=vehicle_name)
                    vehicle_cache[vehicle_name] = vehicle_obj

                rec = PmmRecord(
                    batch=batch,
                    section=section_obj,
                    fuel=fuel_obj,
                    vehicle=vehicle_obj,
                    fact_qty=to_decimal(row.get("fact_qty")),
                    fact_amount=to_decimal(row.get("fact_amount")),
                    plan_qty=to_decimal(row.get("plan_qty")),
                    plan_amount=to_decimal(row.get("plan_amount")),
                    price=to_decimal(row.get("price")),
                    delta=to_decimal(row.get("delta")),
                )
                records.append(rec)
                imported += 1

            except Exception as e:
                errors += 1
                self.stderr.write(f"Row {i} error: {e}")

        PmmRecord.objects.bulk_create(records, batch_size=1000)

        batch.imported_rows = imported
        batch.errors = errors
        batch.save(update_fields=["imported_rows", "errors"])

        self.stdout.write(self.style.SUCCESS(f"Imported rows: {imported}, errors: {errors}"))
        self.stdout.write(self.style.SUCCESS(f"Batch id: {batch.id}"))



