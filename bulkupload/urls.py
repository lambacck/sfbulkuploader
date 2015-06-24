from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^auth-redirect$', views.AuthRedirect.as_view(), name='authredirect'),
    url(r'^login$', views.LoginPageView.as_view(), name='login'),
    url(r'^logout$', views.LogoutView.as_view(), name='logout'),
    url(r'^login-result$', views.AuthReturn.as_view(), name='authreturn'),
    url(r'^database$', views.DatabaseStringView.as_view(), name='database'),
    url(r'^tables$', views.DatabaseTableView.as_view(), name='select_table'),
    url(r'^progress/(?P<taskid>[A-Fa-f0-9]{8}-(?:[A-Fa-f0-9]{4}-){3}[A-Fa-f0-9]{12})$', views.ProgressView.as_view(), name='progress'),
    url(r'^status/(?P<taskid>[A-Fa-f0-9]{8}-(?:[A-Fa-f0-9]{4}-){3}[A-Fa-f0-9]{12})$', views.StatusView.as_view(), name='status'),
]
