from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class DoubleEntryAppConfig(AppConfig):
    name = 'double_entry'
    verbose_name = _('Double-entry accounting & GnuCash integration')
