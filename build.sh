#!/usr/bin/env bash
set -o errexit

pip install -r requirements.txt

python manage.py makemigrations taxonomy_ui
python manage.py migrate --noinput

python manage.py collectstatic --no-input
