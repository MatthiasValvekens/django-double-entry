from django.forms import TextInput
from djmoney.forms import MoneyWidget as BaseMoneyWidget


class DatalistInputWidget(TextInput):
    template_name = 'widgets/datalistinput.html'

    def __init__(self, attrs=None, choices=()):
        super().__init__(attrs)
        if callable(choices):
            self.choices = choices
        else:
            # assume that it's an iterable,
            # and consume it so we can render the widget
            # multiple times
            self.choices = list(choices)

    def get_choices(self):
        return self.choices

    def fmt_datalist_name(self, id_):
        return 'dlist__' + str(id_)

    def get_context(self, name, value, attrs):
        context = super().get_context(name, value, attrs)
        attrs = context['widget']['attrs']
        attrs['autocomplete'] = 'off'
        id_ = attrs.get('id')
        datalist_name = self.fmt_datalist_name(id_)
        attrs['list'] = datalist_name
        context['widget']['datalist_name'] = datalist_name
        context['widget']['datalist_entries'] = self.get_choices()
        return context


class AjaxDatalistInputWidget(DatalistInputWidget):

    class Media:
        js = ('js/ajax-datalist.js',)

    def __init__(self, attrs=None, endpoint=None):
        super().__init__(attrs)
        self.endpoint = endpoint

    def get_endpoint(self):
        return self.endpoint

    def get_choices(self):
        return ()

    def get_context(self, name, value, attrs):
        context = super().get_context(name, value, attrs)
        context['widget']['attrs']['data-endpoint'] = self.endpoint
        return context


class MoneyWidget(BaseMoneyWidget):
    template_name = 'widgets/money.html'