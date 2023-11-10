from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .serializers import dfJobSerializer
from .Scripts import dfJobs

class dfJobs(APIView):
    def post(self, request, *args, **kwargs):
        serializer = dfJobSerializer(data=request.data)
        if serializer.is_valid():
            metadata = serializer.validated_data
            # Extract metadata values as needed
            project_name =  metadata.get('project_name')
            project_id =    metadata.get('project_id')
            dataset_name =  metadata.get('dataset_name')
            userame =       metadata.get('userame')

            try:
                # Your script execution logic here, using metadata values if needed
                result = dfJobs.runDfJob()
                return Response({'success': True, 'result': result})
            except Exception as e:
                return Response({'success': False, 'message': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        else:
            return Response({'success': False, 'message': 'Invalid metadata'}, status=status.HTTP_400_BAD_REQUEST)

