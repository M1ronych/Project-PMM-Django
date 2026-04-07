from decimal import Decimal
from django.test import TestCase, Client
from django.urls import reverse
from django.contrib.auth.models import User

from pmm.models import Section, Fuel, Vehicle, ImportBatch, PmmRecord


class UploadPageTests(TestCase):
    def setUp(self):
        self.client = Client()

    # на відкриття сторінки завантаження

    def test_upload_page_opens(self):
        response = self.client.get(reverse("upload"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Загрузка файла")

class ClearDatabaseViewTests(TestCase):
    def setUp(self):
        self.client = Client()

        self.admin = User.objects.create_superuser(
            username="admin",
            email="admin@example.com",
            password="admin12345",
        )

        self.section = Section.objects.create(name="Загальновиробничі витрати")
        self.fuel = Fuel.objects.create(name="Бензин А-95")
        self.vehicle = Vehicle.objects.create(name="ГАЗ 33023")
        self.batch = ImportBatch.objects.create(
            source_filename="test.csv",
            total_rows=1,
            imported_rows=1,
            errors=0,
        )

        PmmRecord.objects.create(
            batch=self.batch,
            section=self.section,
            fuel=self.fuel,
            vehicle=self.vehicle,
            fact_qty=Decimal("10"),
            fact_amount=Decimal("100"),
            plan_qty=Decimal("0"),
            plan_amount=Decimal("0"),
            price=Decimal("0"),
            delta=Decimal("100"),
        )

    # сторонній не може очистити базу даних 

    def test_clear_database_requires_login(self):
        response = self.client.post(reverse("clear_db"))
        self.assertNotEqual(response.status_code, 200)

    # адмін реально чистить PmmRecord та ImportBatch

    def test_clear_database_works_for_admin(self):
        self.client.login(username="admin", password="admin12345")

        response = self.client.post(reverse("clear_db"))

        self.assertEqual(response.status_code, 302)
        self.assertEqual(PmmRecord.objects.count(), 0)
        self.assertEqual(ImportBatch.objects.count(), 0)


class ExportExcelTests(TestCase):
    def setUp(self):
        self.client = Client()

        self.section = Section.objects.create(name="Загальновиробничі витрати")
        self.fuel = Fuel.objects.create(name="Бензин А-95")
        self.vehicle = Vehicle.objects.create(name="ГАЗ 33023 ВІ 1606 ВО")
        self.batch = ImportBatch.objects.create(
            source_filename="test.csv",
            total_rows=1,
            imported_rows=1,
            errors=0,
        )

        PmmRecord.objects.create(
            batch=self.batch,
            section=self.section,
            fuel=self.fuel,
            vehicle=self.vehicle,
            fact_qty=Decimal("0"),
            fact_amount=Decimal("2487.67"),
            plan_qty=Decimal("0"),
            plan_amount=Decimal("0"),
            price=Decimal("0"),
            delta=Decimal("2487.67"),
        )

    # експорт повертає excel-файл

    def test_export_returns_xlsx_file(self):
        response = self.client.get("/export/")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response["Content-Type"],
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        self.assertIn("attachment;", response["Content-Disposition"])

    # файл не пустий

    def test_export_contains_data_bytes(self):
        response = self.client.get("/export/")
        self.assertTrue(len(response.content) > 0)


class ExportSkipsPersonNamesTests(TestCase):
    def setUp(self):
        self.client = Client()

        self.section = Section.objects.create(name="Загальновиробничі витрати")
        self.fuel = Fuel.objects.create(name="Антифриз")
        self.vehicle = Vehicle.objects.create(name="Дубина Юрій Борисович")
        self.batch = ImportBatch.objects.create(
            source_filename="test_people.csv",
            total_rows=1,
            imported_rows=1,
            errors=0,
        )

        PmmRecord.objects.create(
            batch=self.batch,
            section=self.section,
            fuel=self.fuel,
            vehicle=self.vehicle,
            fact_qty=Decimal("0"),
            fact_amount=Decimal("5000"),
            plan_qty=Decimal("0"),
            plan_amount=Decimal("0"),
            price=Decimal("0"),
            delta=Decimal("5000"),
        )

    # якщо до vehicle попаде людина, експорт не впаде

    def test_export_still_builds_file_when_vehicle_is_person_name(self):
        response = self.client.get("/export/")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(len(response.content) > 0)
