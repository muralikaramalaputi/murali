




#!/usr/bin/env bash
set -o errexit

pip install -r requirements.txt
python manage.py migrate --run-syncdb
python manage.py migrate --fake-initial
python manage.py migrate

python manage.py collectstatic --noinput
python background_stage1.py
