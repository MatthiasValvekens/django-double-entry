from django.urls import include, path
from .views import test_pipeline_api

urlpatterns = [
    path('api/', include(test_pipeline_api.endpoint_urls))
]
