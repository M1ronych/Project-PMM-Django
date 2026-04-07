from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, Optional

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from pmm.models import ImportBatch, PmmRecord
from pmm.models import Fuel, Vehicle, Section


def _smart_decimal(s: str) -> Optional[Decimal]:
    s = (s or "").strip()
    if not s:
        return None
    s = s.replace(" ", "").replace("\u00A0", "")  # звичайні/нерозривні пробіли
    s = s.replace(".", "").replace(",", ".")      # 1С любить 1 234,56
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def _merge_quoted_lines(path: Path, encoding: str) -> list[str]:
    """
    1С може засунути переклад строки всередну кавичок.
    Це ломає звичайний CSV парсер. Ми склеюємо такі строки.
    """
    lines: list[str] = []
    buf: list[str] = []
    quote_open = False

    with path.open("r", encoding=encoding, errors="ignore") as f:
        for raw in f:
            line = raw.rstrip("\n")
            # зчитуємо кавички у строці, але грубо: якщо непарне число - "переключаемося"
            q = line.count('"')
            if not quote_open:
                buf = [line]
                if q % 2 == 1:
                    quote_open = True
                else:
                    lines.append(line)
            else:
                buf.append(line)
                if q % 2 == 1:
                    quote_open = False
                    lines.append(" ".join(buf))
                    buf = []

    if buf:
        lines.append(" ".join(buf))
    return lines


def _detect_encoding(path: Path) -> str:
    """
    Мінімально корисна эвристика.
    """
    try:
        path.read_text(encoding="utf-8-sig")
        return "utf-8-sig"
    except UnicodeDecodeError:
        return "cp1251"


def _find_context(lines: Iterable[str]) -> tuple[str, str]:
    """
    Намагаємось витягнути “Section/Vehicle” з заголовка звіту.
    Це не критично. Якщо не знашли — ставимо дефолти.
    """
    section = "1C Report"
    vehicle = "N/A"

    for line in lines:
        if "Підрозділи" in line or "Подраздел" in line:
            # наприклад: ... Підрозділи Дорівнює "ТВС транспортування тепла (мережі)Миргород"
            m = re.search(r'Підрозділи\s+Дорівнює\s+"([^"]+)"', line)
            if m:
                vehicle = m.group(1).strip()
        if "Статті витрат" in line or "Статьи затрат" in line:
            m = re.search(r'Статті витрат\s+Дорівнює\s+"([^"]+)"', line)
            if m:
                section = m.group(1).strip()

    return section, vehicle


@dataclass
class ParsedRow:
    doc_date: Optional[str]
    doc_text: str
    debit: Optional[Decimal]
    credit: Optional[Decimal]
    item: Optional[str]


FUEL_KEYWORDS = (
    "бензин", "дизель", "дп", "антифриз", "тосол", "масло", "мастило",
    "олива", "паливо", "wd 40", "wd40"
)


def _guess_item(text: str) -> Optional[str]:
    t = (text or "").strip()
    if not t:
        return None
    low = t.lower()
    for kw in FUEL_KEYWORDS:
        if kw in low:
            return t
    return None


def _parse_1c_semicolon_csv(lines: list[str]) -> list[list[str]]:
    """
    Парсим як CSV с delimiter=';' и quotechar='"'
    """
    data: list[list[str]] = []
    reader = csv.reader(lines, delimiter=";", quotechar='"')
    for row in reader:
        data.append(row)
    return data


def _find_debit_credit_header(data: list[list[str]]) -> tuple[int, int, int]:
    """
    Шукаємо строку заголовка, де є “Дебет” и “Кредит”.
    Повертаємо: (header_row_index, debit_col_index, credit_col_index)
    """
    for i, row in enumerate(data[:200]):  # перших 200 строк достатньо
        joined = " ".join(x.strip() for x in row if x).lower()
        if "дебет" in joined and "кредит" in joined:
            # знайдем точні індекси колонок
            debit_idx = None
            credit_idx = None
            for j, cell in enumerate(row):
                c = (cell or "").strip().lower()
                if c == "дебет":
                    debit_idx = j
                if c == "кредит":
                    credit_idx = j
            if debit_idx is not None and credit_idx is not None:
                return i, debit_idx, credit_idx
    raise CommandError("Не знайшов строку заголовка з колонками 'Дебет'/'Кредит'.")


def _extract_rows(data: list[list[str]], header_i: int, debit_i: int, credit_i: int) -> list[ParsedRow]:
    out: list[ParsedRow] = []
    # дані зазвичай після заголовка
    for row in data[header_i + 1:]:
        # пропускаем пустоту
        if not any((c or "").strip() for c in row):
            continue

        # дата зазвичай  в первій ячійці (але не завжди)
        doc_date = (row[0].strip() if len(row) > 0 else "") or None
        doc_text = (row[1].strip() if len(row) > 1 else "") if len(row) > 1 else ""

        debit = _smart_decimal(row[debit_i]) if len(row) > debit_i else None
        credit = _smart_decimal(row[credit_i]) if len(row) > credit_i else None

        # іноді “товар/топливо” іде окромою строкой без сум
        # тому намагаємось витягнути item з тексту строки цілком
        full_text = " ".join(x.strip() for x in row if x)
        item = _guess_item(full_text)

        # відсікаємо явне сміття: строки без суми і без предмету
        if debit is None and credit is None and item is None:
            continue

        out.append(ParsedRow(doc_date=doc_date, doc_text=full_text, debit=debit, credit=credit, item=item))
    return out


def _get_or_create_cached(model, cache: dict, name: str):
    name = (name or "").strip()
    if not name:
        name = "N/A"
    if name in cache:
        return cache[name]
    obj, _ = model.objects.get_or_create(name=name)
    cache[name] = obj
    return obj


class Command(BaseCommand):
    help = "Import 1C report (semicolon CSV with Debit/Credit) into PMM unified structure"

    def add_arguments(self, parser):
        parser.add_argument("csv_path", type=str, help="Path to 1C semicolon CSV file")

    @transaction.atomic
    def handle(self, *args, **options):
        csv_path = Path(options["csv_path"])
        if not csv_path.exists():
            raise CommandError(f"File not found: {csv_path}")

        enc = _detect_encoding(csv_path)
        lines = _merge_quoted_lines(csv_path, encoding=enc)

        section_name, vehicle_name = _find_context(lines)

        data = _parse_1c_semicolon_csv(lines)
        header_i, debit_i, credit_i = _find_debit_credit_header(data)
        parsed = _extract_rows(data, header_i, debit_i, credit_i)

        if not parsed:
            raise CommandError("Не знайшов ні однієї строки з даними післе заголовка.")

        batch = ImportBatch.objects.create(
            source_filename=csv_path.name,
            total_rows=len(parsed),
        )

        section_cache: dict[str, Section] = {}
        vehicle_cache: dict[str, Vehicle] = {}
        fuel_cache: dict[str, Fuel] = {}

        section = _get_or_create_cached(Section, section_cache, section_name)
        vehicle = _get_or_create_cached(Vehicle, vehicle_cache, vehicle_name)

        created = 0

        for r in parsed:
            # нас цікавлять тільки строки, де є топливо/матеріал (або потім збільшиш список ключових слів)
            if not r.item:
                continue

            fuel = _get_or_create_cached(Fuel, fuel_cache, r.item)

            # логіка суми: дебет = витрати, кредит = повернення/корекція
            # у твою "фінальну" структуру кладемо в fact_amount (витрати)
            amount = None
            if r.debit is not None:
                amount = r.debit
            elif r.credit is not None:
                amount = -r.credit  # щоб було видно як  мінус

            PmmRecord.objects.create(
                batch=batch,
                section=section,
                fuel=fuel,
                vehicle=vehicle,
                fact_qty=None,
                fact_amount=amount,
                plan_qty=None,
                plan_amount=None,
                price=None,
                delta=None,
            )
            created += 1

        self.stdout.write(self.style.SUCCESS(f"OK: batch={batch.id}, parsed={len(parsed)}, created={created}"))
