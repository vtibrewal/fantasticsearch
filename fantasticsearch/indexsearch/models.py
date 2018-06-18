from __future__ import unicode_literals

from django.db import models

class Document(models.Model):
	idx = models.IntegerField(null=False)
	title = models.CharField(max_length=50, blank=False)
	data = models.TextField()
	
