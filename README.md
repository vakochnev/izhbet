## IZHBET — Платформа прогнозирования спортивных событий

Проект состоит из пяти модулей, каждый отвечает за свой участок работ:

- `getting/`: получение данных о спортивных событиях (API `stat-api.baltbet.ru`) и загрузка в БД
- `calculation/`: расчет турнирных таблиц, эмбеддингов и фичей для моделей
- `processing/`: обучение нейросетей на истории и создание базовых прогнозов (таблица `predictions`)
- `forecast/`: конформное прогнозирование и отбор качественных прогнозов (таблицы `outcomes`, `statistics`)
- `publisher/`: публикация отчетов и прогнозов из `statistics`


### Установка и окружение

1) Создайте и активируйте окружение
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

2) Настройте доступ к БД в `config.py` (пул сессий `Session_pool`) и выполните миграции Alembic:
```
alembic upgrade head
```


### Единый запуск пайплайна

Полная цепочка (processing → forecast → publisher) запускается через:
```
python run_pipeline.py today      # прогнозы на сегодня + итоги вчера
python run_pipeline.py all_time   # полная обработка истории и quality-outcomes
```

Проверка статуса компонентов:
```
python run_pipeline.py status
```

Отдельные этапы:
```
python run_pipeline.py processing
python run_pipeline.py forecast
python run_pipeline.py publisher
```

Скрипт с интегрированным запуском: `run_integrated.sh`.


### Границы ответственности модулей

- `processing`: формирует и сохраняет базовые прогнозы в `predictions`
- `forecast`: на основе `predictions` выполняет конформное прогнозирование и формирует `outcomes`/`statistics`
- `publisher`: читает `statistics` и публикует отчеты в каналы/файлы


### База данных и ORM

Используется SQLAlchemy ORM. Доступ к данным разделен:
- `db/models/` — декларативные модели
- `db/queries/` — готовые ORM-запросы (без SQL)
- `db/storage/` — операции сохранения

Таблица `statistics` наполняется на этапе `forecast` и используется для публикаций.


### Логи и мониторинг

Логирование настраивается централизованно. Мониторинг качества прогнозов доступен через отчеты `publisher` и таблицу `statistics`.


### Полезные команды Alembic

```
alembic revision --autogenerate -m "describe changes"
alembic upgrade head
```

Подробнее: `https://alembic.sqlalchemy.org/`.