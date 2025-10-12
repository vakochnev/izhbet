pip install pytest
pytest tests/
pytest --cov=.

# Рекомендации
# Для более полного покрытия добавьте моки для db.storage.getting функций (save_sport, save_country и др.)
# Добавьте тесты на ошибки: JSONDecodeError, RequestException
# Используйте pytest-xdist для параллельного запуска тестов.