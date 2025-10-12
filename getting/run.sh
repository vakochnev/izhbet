# Запуск тестов с покрытием
pytest --cov=.

# Форматирование кода
black getting.py datahandler.py download.py

# Линтинг
flake8 getting.py datahandler.py download.py

# Проверка типов
mypy getting.py datahandler.py download.py