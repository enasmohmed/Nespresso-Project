"""
URL configuration for project project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.conf.urls.static import static

from django.urls import path, include
from django.http import HttpResponse

from project import settings


def home_light(request):
    """
    صفحة رئيسية خفيفة جدًا لفتح الموقع بسرعة على السيرفر.
    تعرض رسالة بسيطة ورابط إلى الداشبورد الكامل.
    """
    return HttpResponse(
        """
        <html>
            <head>
                <title>Nespresso Dashboard</title>
                <style>
                    body { font-family: sans-serif; background:#f5f5f5; display:flex; align-items:center; justify-content:center; height:100vh; margin:0; }
                    .card { background:#fff; padding:30px 40px; border-radius:10px; box-shadow:0 4px 10px rgba(0,0,0,0.08); text-align:center; }
                    .btn { display:inline-block; margin-top:15px; padding:10px 18px; border-radius:6px; background:#9F8170; color:#fff; text-decoration:none; font-weight:600; }
                    .btn:hover { background:#81613E; }
                </style>
            </head>
            <body>
                <div class="card">
                    <h2>Nespresso KPI Dashboard</h2>
                    <p>Site is up and running. Click below to open the full dashboard.</p>
                    <a href="/app/" class="btn">Open Dashboard</a>
                </div>
            </body>
        </html>
        """
    )


urlpatterns = [
    path("", home_light, name="home"),
    path("admin/", admin.site.urls),
    path("", include("dashboard.urls")),
] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
