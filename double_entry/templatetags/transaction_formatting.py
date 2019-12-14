import dataclasses
import datetime
import re

from django import template
from django.utils.safestring import mark_safe
from djmoney.money import Money

from double_entry.forms.bulk_utils import ResolvedTransaction

register = template.Library()

ATTR_NAME_REGEX = re.compile('[a-z_]+')
def normalise_attr_name(key):
    if not ATTR_NAME_REGEX.fullmatch(key):
        raise ValueError(
            'Attribute name must consist of only lowercase '
            'characters and underscores'
        )
    return key.replace('_', '-')


@register.filter
def rt_html_tags(resolved_transaction: ResolvedTransaction):
    attr_dict = dataclasses.asdict(resolved_transaction)

    for attr in resolved_transaction.html_ignore():
        del attr_dict[attr]

    def attrs():
        for name, val in attr_dict.items():
            name = normalise_attr_name(name)
            if isinstance(val, Money):
                yield 'money-amt-' + name, val.amount
                yield 'money-cur-' + name, val.currency
            elif isinstance(val, datetime.datetime):
                yield 'dt-' + name, val.isoformat()
            else:
                yield name, val

    # having cleanly serialisable attr_values is the reponsibility
    # of the subclass
    result = ' '.join(
        'data-%s="%s"' % (attr_name, attr_value)
        for attr_name, attr_value in attrs()
    )
    return mark_safe(result)
