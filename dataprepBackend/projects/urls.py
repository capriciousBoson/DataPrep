from django.urls import path
from . import views

urlpatterns = [
    path('jobsapi/', views.dfJobsapi.as_view())
]