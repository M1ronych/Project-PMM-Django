from django import forms

class UploadFileForm(form.Form):
    file = forms.FileField()

