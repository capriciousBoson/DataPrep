# serializers.py
from rest_framework import serializers

class dfJobSerializer(serializers.Serializer):
    
    # Add more fields as needed
    #project_name =  serializers.CharField()
    #description =   serializers.CharField()
    project_id =    serializers.CharField()
    dataset_name =  serializers.CharField()
    userame =       serializers.CharField()
