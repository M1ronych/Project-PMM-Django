from django.contrib import admin
from .models import Section, Fuel, Vehicle, ImportBatch, PmmRecord

admin.site.register(Section)
admin.site.register(Fuel)
admin.site.register(Vehicle)
admin.site.register(ImportBatch)
admin.site.register(PmmRecord)
