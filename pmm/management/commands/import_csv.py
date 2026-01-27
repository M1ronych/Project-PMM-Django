from decimal import Decimal, InvalidOperation
from pathlib import Path

import pandas as pd
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from pmm.models import Section, Fuel, Vehicle, ImportBatch, PmmRecord


def to_decimal(x):
    if pd.isna(x) or x == "" or str(x).lower() == "nan":
        return None
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return None


class Command(BaseCommand):
    help = "Import PMM records from a prepared CSV (final_vehicle.csv)."

    def add_arguments(self, parser):
        parser.add_argument("csv_path", type=str, help="Path to CSV file, e.g. data_out/final_vehicle.csv")

    @transaction.atomic
    def handle(self, *args, **options):
        csv_path = Path(options["csv_path"])
        if not csv_path.exists():
            raise CommandError(f"CSV not found: {csv_path}")

        df = pd.read_csv(csv_path)

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

        # Кеш, чтобы не долбить БД на каждой строке
        section_cache = {}
        fuel_cache = {}
        vehicle_cache = {}

        records = []

        for i, row in df.iterrows():
            try:
                section_name = row.get("section")
                if pd.isna(section_name) or str(section_name).lower() in ("nan", "none", ""):
                    section_obj = None
                else:
                    section_name = str(section_name).strip()
                    section_obj = section_cache.get(section_name)
                    if section_obj is None:
                        section_obj, _ = Section.objects.get_or_create(name=section_name)
                        section_cache[section_name] = section_obj

                fuel_name = str(row["fuel"]).strip()
                fuel_obj = fuel_cache.get(fuel_name)
                if fuel_obj is None:
                    fuel_obj, _ = Fuel.objects.get_or_create(name=fuel_name)
                    fuel_cache[fuel_name] = fuel_obj

                vehicle_name = str(row["vehicle"]).strip()
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

            except Exception:
                errors += 1

        PmmRecord.objects.bulk_create(records, batch_size=1000)

        batch.imported_rows = imported
        batch.errors = errors
        batch.save(update_fields=["imported_rows", "errors"])

        self.stdout.write(self.style.SUCCESS(f"Imported rows: {imported}, errors: {errors}"))
        self.stdout.write(self.style.SUCCESS(f"Batch id: {batch.id}"))
