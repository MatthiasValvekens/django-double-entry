from django.db import models
from django.http import Http404
from double_entry import models as base


class Customer(models.Model):
    name = models.CharField(max_length=100)
