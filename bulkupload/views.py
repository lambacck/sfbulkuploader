import logging
from urlparse import urlparse

from django.conf import settings
from django.core.urlresolvers import reverse, reverse_lazy
from django.core.exceptions import ImproperlyConfigured
from django.utils.translation import ugettext_lazy as _
from django.views.generic import RedirectView, TemplateView, FormView

from braces.views import SetHeadlineMixin
from braces.views._access import AccessMixin

from salesforce_oauth2 import SalesforceOAuth2
from salesforce_bulk import SalesforceBulk, CsvDictsAdapter

from sqlalchemy import create_engine
from sqlalchemy.sql import text

from .forms import DatabaseConnectionStringForm, DatabaseTableForm

log = logging.getLogger(__name__)


def get_oauth_handler(request):
    try:
        client_id = settings.SALESFORCE_CLIENT_ID
        client_secret = settings.SALESFORCE_CLIENT_SECRET
    except AttributeError:
        raise ImproperlyConfigured('Salesforce settings "SALESFORCE_CLIENT_ID" or "SALESFORCE_CLIENT_SECRET" not set.')

    return SalesforceOAuth2(
        client_id,
        client_secret,
        request.build_absolute_uri(reverse('bulkupload:authreturn')),
        sandbox=settings.SALESFORCE_SANDBOX)


def field_modifier(value):
    if hasattr(value, 'isoformat'):
        return value.isoformat()

    if value is None:
        return ''

    return unicode(value).encode('utf-8')


def row_modifier(row):
    return [field_modifier(x) for x in row]


class RootView(RedirectView):
    permanent = False

    def get_redirect_url(self, *args, **kwargs):
        if 'access_token' in self.request.session:
            return reverse('bulkupload:database')

        return reverse('bulkupload:login')


class LoginPageView(SetHeadlineMixin, TemplateView):
    headline = _('Login to Salesforce')
    template_name = "login.html"


class AuthRedirect(RedirectView):
    permanent = False

    def get_redirect_url(self, *args, **kwargs):
        handler = get_oauth_handler(self.request)
        url = handler.authorize_url(scope='api')

        return url


class AuthReturn(RedirectView):
    permanent = False

    def get_redirect_url(self, *args, **kwargs):
        code = self.request.GET.get('code')
        if not code:
            return reverse('bulkupload:login')

        handler = get_oauth_handler(self.request)
        response = handler.get_token(code)
        if 'access_token' in response:
            self.request.session['access_token'] = response['access_token']
            self.request.session['instance_url'] = response['instance_url']
            return reverse('bulkupload:database')

        return reverse('bulkupload:login')


class LogoutView(RedirectView):
    permanent = False
    redirect_url = reverse_lazy('bulkupload:login')

    def get_redirect_url(self, *args, **kwargs):
        session = self.request
        if 'access_token' in session:
            handler = get_oauth_handler(self.request)
            handler.revoke_token(session['access_token'])

            try:
                del session['access_token']
                del session['instance_url']
            except KeyError:
                pass

        return super(LogoutView, self).get_redirect_url(*args, **kwargs)


class SalesforceLoginRequiredMixin(AccessMixin):
    """
    View mixin which verifies that the user is authenticated to salesforce.
    NOTE:
        This should be the left-most mixin of a view, except when
        combined with CsrfExemptMixin - which in that case should
        be the left-most mixin.
    """

    login_url = reverse_lazy('bulkupload:login')

    def dispatch(self, request, *args, **kwargs):
        if 'access_token' not in self.request.session:
            return self.handle_no_permission(request)

        return super(SalesforceLoginRequiredMixin, self).dispatch(
            request, *args, **kwargs)


class DatabaseStringView(SalesforceLoginRequiredMixin, SetHeadlineMixin, FormView):
    template_name = 'database.html'
    form_class = DatabaseConnectionStringForm
    headline = _('Set Connection String')
    success_url = reverse_lazy('bulkupload:select_table')

    def get_initial(self):
        if 'connectionstring' not in self.request.session:
            return {}

        return {'connectionstring': self.request.session['connectionstring']}

    def form_valid(self, form):
        self.request.session['connectionstring'] = form.cleaned_data['connectionstring']

        return super(DatabaseStringView, self).form_valid(form)


class DatabaseTableView(SalesforceLoginRequiredMixin, SetHeadlineMixin, FormView):
    template_name = 'database_tables.html'
    form_class = DatabaseTableForm
    headline = _('Select Table to Upload')
    success_url = reverse_lazy('bulkupload:select_table')

    def get_engine(self):
        if not hasattr(self, 'sqlengine'):
            self.sqlengine = create_engine(self.request.session['connectionstring'])

        return self.sqlengine

    def get_form_kwargs(self):
        retval = super(DatabaseTableView, self).get_form_kwargs()

        engine = self.get_engine()
        tables = [u'.'.join(x) for x in engine.execute('''select table_schema, table_name FROM information_schema.tables where table_schema <> 'information_schema' and table_schema <> 'pg_catalog' ''')]

        retval['tablelist'] = tables
        log.debug('form kwargs: %s', retval)

        return retval

    def form_valid(self, form):
        tablename = form.cleaned_data['table']

        schema, table = tablename.split('.')
        session = self.request.session

        bulk = SalesforceBulk(
            sessionId=session['access_token'],
            host=urlparse(session['instance_url']).hostname)

        engine = self.get_engine()
        result = engine.execute(text('select column_name from information_schema.columns where table_name = :table and table_schema = :schema'), {'table': table, 'schema': schema})
        exclude = ['sfid', 'id', 'systemmodstamp', 'isdeleted']
        columns = [x[0] for x in result if not x[0].startswith('_') and x[0].lower() not in exclude]

        log.debug('columns: %s', columns)
        column_select = ','.join('"%s"' % x for x in columns)

        result = engine.execute('select %s from %s' % (column_select, tablename))

        dict_iter = (dict(zip(columns, row_modifier(row))) for row in result)
        dict_iter = list(dict_iter)
        log.debug('Sending rows: %s', [x['name'] for x in dict_iter])
        csv_iter = CsvDictsAdapter(iter(dict_iter))

        job = bulk.create_insert_job(table.capitalize(), contentType='CSV')
        batch = bulk.post_bulk_batch(job, csv_iter)

        bulk.wait_for_batch(job, batch)

        bulk_result = []

        def save_results(rows, failed, remaining):
            bulk_result[:] = [rows, failed, remaining]

        flag = bulk.get_upload_results(job, batch, callback=save_results)
        log.debug('results: %s, %s', flag, bulk_result)

        bulk.close_job(job)

        return super(DatabaseTableView, self).form_valid(form)
