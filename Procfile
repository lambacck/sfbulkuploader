web: gunicorn sfbulkuploader.wsgi --log-file -
worker: celery -A sfbulkuploader  worker --loglevel=info
