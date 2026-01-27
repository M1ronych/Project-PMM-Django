from django.shortcuts import render
from pathlib import Path

from django.conf import settings
from django.contrib import messages
from django.core.files.storage import FileSystemStorage
from django.shortcuts import render, redirect

from django.core.management import call_command

from pmm.models import ImportBatch

def upload_view(request):
    if request.method == "POST" and request.FILES.get("file"):
        uploaded = request.FILES["file"]

        fs = FileSystemStorage(location=Path(settings.MEDIA_ROOT) / "uploads")
        saved_name = fs.save(uploaded.name, uploaded)
        saved_path = fs.path(saved_name)

        # Импортируем через нашу management command
        try:
            call_command("import_csv", saved_path)
            messages.success(request, f"Импорт выполнен: {uploaded.name}")
        except Exception as e:
            messages.error(request, f"Ошибка импорта: {e}")

        return redirect("upload")

    batches = ImportBatch.objects.order_by("-id")[:20]
    return render(request, "pmm/upload.html", {"batches":batches})

from io import BytesIO
from datetime import datetime

from django.http import HttpResponse
from django.db.models import Sum

from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.utils import get_column_letter

from pmm.models import PmmRecord


def _autosize_columns(ws):
    for col in ws.columns:
        max_len = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            v = cell.value
            if v is None:
                continue
            max_len = max(max_len, len(str(v)))
        ws.column_dimensions[col_letter].width = min(max_len + 2, 60)

from django.shortcuts import get_object_or_404
from pmm.models import ImportBatch, PmmRecord

def export_xlsx_view(request):
    batch_param = request.GET.get("batch")

    if batch_param:
        batch = get_object_or_404(ImportBatch, id=int(batch_param))
    else:
        batch = ImportBatch.objects.order_by("-id").first()

    if batch is None:
        return HttpResponse(
            "Нема імпортів (ImportBatch). Спочатку загрузи файл.",
            content_type="text/plain"
        )

    qs = (
        PmmRecord.objects
        .filter(batch=batch)
        .select_related("section", "fuel", "vehicle", "batch")
        .order_by("fuel__name", "vehicle__name", "id")
    )

    wb = Workbook()
    ws = wb.active
    ws.title = "Records"

    headers = [
        "id",
        "batch",
        "section",
        "fuel",
        "vehicle",
        "fact_qty",
        "fact_amount",
        "plan_qty",
        "plan_amount",
        "price",
        "delta",
    ]
    ws.append(headers)

    for r in qs:
        ws.append([
            r.id,
            str(r.batch) if r.batch_id else "",
            r.section.name if r.section_id else "",
            r.fuel.name if r.fuel_id else "",
            r.vehicle.name if r.vehicle_id else "",
            float(r.fact_qty) if r.fact_qty is not None else None,
            float(r.fact_amount) if r.fact_amount is not None else None,
            float(r.plan_qty) if r.plan_qty is not None else None,
            float(r.plan_amount) if r.plan_amount is not None else None,
            float(r.price) if r.price is not None else None,
            float(r.delta) if r.delta is not None else None,
        ])

    _autosize_columns(ws)

    ws2 = wb.create_sheet("Summary by fuel")
    ws2.append(["fuel", "fact_qty_sum", "fact_amount_sum", "plan_qty_sum", "plan_amount_sum"])

    fuel_summary = (
        PmmRecord.objects
        .select_related("fuel")
        .values("fuel__name")
        .annotate(
            fact_qty_sum=Sum("fact_qty"),
            fact_amount_sum=Sum("fact_amount"),
            plan_qty_sum=Sum("plan_qty"),
            plan_amount_sum=Sum("plan_amount"),
        )
        .order_by("fuel__name")
    )

    for row in fuel_summary:
        ws2.append([
            row["fuel__name"] or "",
            float(row["fact_qty_sum"]) if row["fact_qty_sum"] is not None else 0,
            float(row["fact_amount_sum"]) if row["fact_amount_sum"] is not None else 0,
            float(row["plan_qty_sum"]) if row["plan_qty_sum"] is not None else 0,
            float(row["plan_amount_sum"]) if row["plan_amount_sum"] is not None else 0,
        ])

    _autosize_columns(ws2)

    ws3 = wb.create_sheet("Summary by vehicle")
    ws3.append(["vehicle", "fuel", "fact_qty_sum", "fact_amount_sum"])

    vehicle_summary = (
        PmmRecord.objects
        .select_related("vehicle", "fuel")
        .values("vehicle__name", "fuel__name")
        .annotate(
            fact_qty_sum=Sum("fact_qty"),
            fact_amount_sum=Sum("fact_amount"),
        )
        .order_by("vehicle__name", "fuel__name")
    )

    for row in vehicle_summary:
        ws3.append([
            row["vehicle__name"] or "",
            row["fuel__name"] or "",
            float(row["fact_qty_sum"]) if row["fact_qty_sum"] is not None else 0,
            float(row["fact_amount_sum"]) if row["fact_amount_sum"] is not None else 0,
        ])

    _autosize_columns(ws3)

    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)

    stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"pmm_report_batch_{batch.id}_{stamp}.xlsx"

    resp = HttpResponse(
        buf.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    resp["Content-Disposition"] = f'attachment; filename="{filename}"'
    return resp
