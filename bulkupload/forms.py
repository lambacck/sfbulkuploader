from django.utils.translation import ugettext_lazy as _
from django import forms

class DatabaseConnectionStringForm(forms.Form):
    connectionstring = forms.CharField(
        max_length=500, required=True,
        label=_('Database Connection String'))

class DatabaseTableForm(forms.Form):

    def __init__(self, **kwargs):
        tables = kwargs.pop('tablelist')
        super(DatabaseTableForm, self).__init__(**kwargs)

        self.fields['table'] = forms.ChoiceField(
            choices=[(x,x) for x in tables],
            label=_('table'))
