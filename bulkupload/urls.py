from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^auth-redirect$', views.AuthRedirect.as_view(), name='authredirect'),
    url(r'^login$', views.LoginPageView.as_view(), name='login'),
    url(r'^logout$', views.LogoutView.as_view(), name='logout'),
    url(r'^login-result$', views.AuthReturn.as_view(), name='authreturn'),
    url(r'^database$', views.DatabaseStringView.as_view(), name='database'),
    url(r'^tables$', views.DatabaseTableView.as_view(), name='select_table'),
]
