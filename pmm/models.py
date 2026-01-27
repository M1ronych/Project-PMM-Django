from django.db import models


class Section(models.Model):
    name = models.CharField(max_length=255, unique=True)

    def __str__(self):
        return self.name


class Fuel(models.Model):
    name = models.CharField(max_length=255, unique=True)

    def __str__(self):
        return self.name


class Vehicle(models.Model):
    name = models.CharField(max_length=255, unique=True)

    def __str__(self):
        return self.name


class ImportBatch(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    source_filename = models.CharField(max_length=255)

    total_rows = models.IntegerField(default=0)
    imported_rows = models.IntegerField(default=0)
    errors = models.IntegerField(default=0)

    def __str__(self):
        return f"#{self.id} {self.source_filename} ({self.created_at:%Y-%m-%d %H:%M})"


class PmmRecord(models.Model):
    batch = models.ForeignKey(ImportBatch, on_delete=models.CASCADE, related_name="records")

    section = models.ForeignKey(Section, on_delete=models.SET_NULL, null=True, blank=True)
    fuel = models.ForeignKey(Fuel, on_delete=models.PROTECT)
    vehicle = models.ForeignKey(Vehicle, on_delete=models.PROTECT)

    fact_qty = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    fact_amount = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    plan_qty = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    plan_amount = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    price = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    delta = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)

    def str(self):
        return f"{self.fuel} / {self.vehicle}"


