from os.path import dirname, abspath, join

SECRET_KEY = 'fake-key'

DEBUG = True

DEFAULT_AUTO_FIELD = 'django.db.models.AutoField'

INSTALLED_APPS = [
    'django.contrib.auth',
    'django.contrib.sessions',
    'django.contrib.contenttypes',
    'django.contrib.staticfiles',
    'double_entry', 'double_entry.tests',
]

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3'
    }
}

ROOT_URLCONF = 'double_entry.tests.urls'

TEMPLATES = [{
    'BACKEND': 'django.template.backends.django.DjangoTemplates',
    'DIRS': ['templates'],
    'OPTIONS': {
        'context_processors': (
            'django.contrib.auth.context_processors.auth',
            'django.template.context_processors.i18n',
            'django.template.context_processors.static',
            'django.template.context_processors.request',
        ),
        'loaders': (
            'django.template.loaders.filesystem.Loader',
            'django.template.loaders.app_directories.Loader',
        )
    }
}]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.locale.LocaleMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

SESSION_ENGINE = 'django.contrib.sessions.backends.file'
PASSWORD_HASHERS  = [
    'django.contrib.auth.hashers.MD5PasswordHasher'
]

USE_TZ = True
DEFAULT_CURRENCY = 'EUR'

STATIC_URL = '/static/'

BASE_DIR = dirname(dirname(abspath(__file__)))
STATICFILES_DIRS = (
    join(BASE_DIR, 'double_entry', 'static', 'double_entry'),
)
