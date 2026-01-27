from django.urls import path
from .views import upload_view, export_xlsx_view

urlpatterns = [
    path('upload/',upload_view,name='upload'),
    path('export/',export_xlsx_view,name='export'),
]

