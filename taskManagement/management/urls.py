
# URI FOLDER FOR APP MANAGEMENT

from django.urls import path
from . import views

urlpatterns = [
    path('management/', views.management)
]