from django.apps import AppConfig
from django.utils.translation import ugettext_lazy as _


class DoubleEntryAppConfig(AppConfig):
    name = 'double_entry'
    verbose_name = _('Double-entry accounting & GnuCash integration')
