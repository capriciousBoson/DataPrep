from django.db import models

class Project(models.Model):
    # our class inherits from the default django class of models to get lots of features.
    project_name =  models.CharField(max_length=120)
    description =   models.TextField(blank=True, null=True)
    project_id =    models.CharField(primary_key=True, blank=False, null=False)
    dataset_name =  models.CharField(blank=False, null=False)
    userame =       models.CharField(max_length=120, blank=False, null=False)
