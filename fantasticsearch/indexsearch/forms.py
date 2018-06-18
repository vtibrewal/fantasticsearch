from django import forms
  
class IndexForm(forms.Form):
        idx = forms.IntegerField()
	title = forms.CharField()
	content = forms.CharField()
