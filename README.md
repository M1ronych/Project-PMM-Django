# PMM Django Project WSL

Django app for importing PMM CSV batches into DB and exporting Excel reports.

## Setup (WSL/Linux)

`bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 manage.py migrate
python3 manage.py createsuperuser
python3 manage.py runserver
