from django.urls import path
from . import views

urlpatterns = [
    path('jobsapi/', views.DataprocJobView.as_view())
    
]