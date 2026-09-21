"""Isolated SQLite settings for the automatic quote tests; never uses live data."""
SECRET_KEY = "automatic-quote-test-only"
INSTALLED_APPS = ["django.contrib.auth", "django.contrib.contenttypes", "warehouse"]
DATABASES = {"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}}
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
USE_TZ = True
TIME_ZONE = "UTC"
MIGRATION_MODULES = {"warehouse": None}
ROOT_URLCONF = "warehouse.auto_quote_test_urls"
TEMPLATES = [{"BACKEND": "django.template.backends.django.DjangoTemplates", "APP_DIRS": True}]
STATIC_URL = "/static/"
