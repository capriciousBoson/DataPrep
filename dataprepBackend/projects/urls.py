from django.urls import path
from . import views

urlpatterns = [
    path('jobsapi/', views.dataProcApi.as_view())
    
]