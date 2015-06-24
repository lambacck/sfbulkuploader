import logging
from urlparse import urlparse
from cgi import escape

from django.conf import settings
from django.core.urlresolvers import reverse, reverse_lazy
from django.core.exceptions import ImproperlyConfigured
from django.utils.translation import ugettext_lazy as _
from django.views.generic import RedirectView, TemplateView, FormView, View

from braces.views import SetHeadlineMixin, JSONResponseMixin
from braces.views._access import AccessMixin

from salesforce_oauth2 import SalesforceOAuth2

from sqlalchemy import create_engine
from celery.result import AsyncResult

from .forms import DatabaseConnectionStringForm, DatabaseTableForm
from .tasks import upload_table

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

    def get_redirect_url(self, *args, **kwargs):
        session = self.request.session
        if 'access_token' in session:
            handler = get_oauth_handler(self.request)
            handler.revoke_token(session['access_token'])

        session.clear()

        return reverse('bulkupload:login')


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

    def get_form_kwargs(self):
        retval = super(DatabaseTableView, self).get_form_kwargs()

        engine = create_engine(self.request.session['connectionstring'])
        tables = [u'.'.join(x) for x in engine.execute('''select table_schema, table_name FROM information_schema.tables where table_schema <> 'information_schema' and table_schema <> 'pg_catalog' ''')]

        retval['tablelist'] = tables
        log.debug('form kwargs: %s', retval)

        return retval

    def get_success_url(self):

        return reverse('bulkupload:progress', kwargs={'taskid': self.result.id})

    def form_valid(self, form):
        tablename = form.cleaned_data['table']

        schema, table = tablename.split('.')
        session = self.request.session

        sessionId = session['access_token']
        host = urlparse(session['instance_url']).hostname
        connection_string = session['connectionstring']

        # call task
        self.result = upload_table.delay(sessionId, host, tablename, connection_string)

        return super(DatabaseTableView, self).form_valid(form)


class ProgressView(SalesforceLoginRequiredMixin, SetHeadlineMixin, TemplateView):
    template_name = 'progress.html'
    headline = _('Request Status')

    def get_context_data(self, **kwargs):
        context = super(ProgressView, self).get_context_data(**kwargs)
        context['taskid'] = kwargs['taskid']
        return context


class StatusView(JSONResponseMixin, View):
    def get(self, request, *args, **kwargs):

        result = AsyncResult(kwargs['taskid'])
        log_html = u''

        if result.ready():
            if result.successful():
                rows, failed, remaining = result.result
                log_html = []
                has_error = False
                for i, row in enumerate(rows):
                    if i:
                        has_error = has_error or not not row[-1]
                        row_tmpl = u'<tr><td>%s</td></tr>'
                        col_join = u'</td><td>'
                    else:
                        row_tmpl = u'<tr><th>%s</th></tr>'
                        col_join = u'</th><th>'

                    log_html.append(row_tmpl % col_join.join(escape(x) for x in row))

                log_html = u'<table class="table">%s</table>' % u''.join(log_html)

                if has_error:
                    log_html = u'<div class="alert alert-danger" role="alert">At least one row was not transferred. Please see log below for details.</div>' + log_html
                else:
                    log_html = u'<div class="alert alert-success" role="alert">All rows successfully added.</div>' + log_html
            else:
                log_html = u'<div class="alert alert-danger" role="alert">%s</div>' % escape(unicode(result.result))
        context_dict = {
            'completed': result.ready(),
            'log_html': log_html,
        }

        return self.render_json_response(context_dict)
