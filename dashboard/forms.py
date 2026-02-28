from django import forms

from dashboard.models import MeetingPoint


# class UploadExcelForm(forms.Form):
#     excel_file = forms.FileField(label="Excel File", required=True)


class ExcelUploadForm(forms.Form):
    excel_file = forms.FileField(
        label="Select Excel file (e.g. all sheet.xlsm)",
        required=True,
        widget=forms.ClearableFileInput(attrs={
            "class": "form-control",
            "accept": ".xlsx,.xlsm",
        })
    )




class MeetingPointForm(forms.ModelForm):
    class Meta:
        model = MeetingPoint
        fields = ['description']
        widgets = {
            'description': forms.Textarea(attrs={
                'class': 'form-control',
                'rows': 2,
                'placeholder': 'اكتب النقطة أو الإجراء هنا...',
            }),
        }
