# PMM Django Project

Учебный проект по импорту CSV и экспорту Excel отчётов.

## Функционал
- Загрузка CSV файла
- Импорт данных в базу
- Просмотр через Django admin
- Экспорт Excel отчётов

## Установка

`bash
git clone https://github.com/M1ronych/Project-PMM-Django.git
cd Project-PMM-Django
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
python manage.py migrate
python manage.py runserver
