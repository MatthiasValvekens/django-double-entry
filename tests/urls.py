from django.conf import settings
from django.conf.urls.static import static
from django.urls import include, path
from . import views

urlpatterns = [
    path('api/', include(views.test_pipeline_api.endpoint_urls)),
    path('kbcupload/', views.TestTransferFormView.as_view(), name='kbc_upload'),
    path('kbcupload/auto', views.TestTransferFormView.hook_simple_lookup_test)
] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
