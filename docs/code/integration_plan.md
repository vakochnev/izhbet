# План интеграции данных в таблицу statistics_optimized

## 🎯 Цель
Интегрировать данные в новую таблицу `statistics_optimized` без нарушения существующей функциональности.

## 📊 Текущий поток данных

### 1. Конформный предиктор
```
processing/conformal_predictor.py
    ↓
db/storage/forecast.py::save_conformal_outcome()
    ↓
outcomes (таблица)
```

### 2. Keras модели
```
processing/prediction_keras.py
    ↓
db/storage/processing.py::save_prediction()
    ↓
predictions (таблица)
```

### 3. Publisher
```
publisher/conformal_publication.py
    ↓
db/storage/publisher.py::save_conformal_report()
    ↓
Файлы отчетов
```

## 🔄 Новый поток данных

### 1. Конформный предиктор (с интеграцией)
```
processing/conformal_predictor.py
    ↓
db/storage/statistics_integration.py::integrate_conformal_outcome_save()
    ↓
outcomes (таблица) + statistics_optimized (таблица)
```

### 2. Keras модели (с интеграцией)
```
processing/prediction_keras.py
    ↓
db/storage/statistics_integration.py::integrate_prediction_save()
    ↓
predictions (таблица) + statistics_optimized (таблица)
```

## 🛠️ Шаги интеграции

### Шаг 1: Применение миграции
```bash
# Применить миграцию для создания новых таблиц
alembic upgrade head
```

### Шаг 2: Интеграция с конформным предиктором

#### 2.1. Обновить `processing/conformal_predictor.py`
```python
# Заменить импорт
from db.storage.forecast import save_conformal_outcome

# На
from db.storage.statistics_integration import integrate_conformal_outcome_save

# Заменить вызов
if save_conformal_outcome(db_session, result):

# На
if integrate_conformal_outcome_save(db_session, result):
```

#### 2.2. Обновить `db/storage/forecast.py`
```python
# Добавить функцию интеграции
def save_conformal_outcome_with_statistics(db_session, result):
    """Расширенная версия с интеграцией в statistics_optimized."""
    # Существующая логика сохранения в outcomes
    success = save_conformal_outcome(db_session, result)
    
    if success:
        # Интеграция в statistics_optimized
        integration_service = StatisticsIntegrationService()
        # ... логика интеграции
    
    return success
```

### Шаг 3: Интеграция с Keras моделями

#### 3.1. Обновить `db/storage/processing.py`
```python
# Добавить интеграцию после сохранения prediction
def save_prediction_with_statistics(db_session, predictions):
    """Расширенная версия с интеграцией в statistics_optimized."""
    # Существующая логика сохранения в predictions
    save_prediction(db_session, predictions)
    
    # Интеграция в statistics_optimized
    integration_service = StatisticsIntegrationService()
    for match_id in predictions:
        # ... логика интеграции
```

### Шаг 4: Массовая интеграция существующих данных

#### 4.1. Создать скрипт миграции данных
```python
# migration_data_to_statistics.py
def migrate_existing_data():
    """Мигрирует все существующие данные в statistics_optimized."""
    integration_service = StatisticsIntegrationService()
    
    # Интегрируем все outcomes
    outcomes_result = integration_service.integrate_existing_data()
    
    # Интегрируем все predictions
    predictions_result = integration_service.integrate_existing_predictions()
    
    return {
        'outcomes': outcomes_result,
        'predictions': predictions_result
    }
```

### Шаг 5: Обновление после завершения матчей

#### 5.1. Создать сервис обновления результатов
```python
# db/storage/match_results_updater.py
class MatchResultsUpdater:
    def update_match_results(self, match_id: int, goal_home: int, goal_away: int):
        """Обновляет статистику после завершения матча."""
        with Session_pool() as db_session:
            # Находим записи статистики
            statistics_records = (
                db_session.query(StatisticsOptimized)
                .filter(StatisticsOptimized.match_id == match_id)
                .all()
            )
            
            # Обновляем каждую запись
            for stats in statistics_records:
                self._update_statistics_record(stats, goal_home, goal_away)
            
            db_session.commit()
```

## 🔧 Технические детали

### 1. Маппинг типов прогнозов
```python
FEATURE_MAPPING = {
    1: ('win_draw_loss', 'classification'),
    2: ('oz', 'classification'),
    3: ('goal_home', 'classification'),
    4: ('goal_away', 'classification'),
    5: ('total', 'classification'),
    6: ('total_home', 'classification'),
    7: ('total_away', 'classification'),
    8: ('total_amount', 'regression'),
    9: ('total_home_amount', 'regression'),
    10: ('total_away_amount', 'regression')
}
```

### 2. Вычисление качества прогнозов
```python
def calculate_prediction_quality(outcome, actual_result, actual_value):
    """Вычисляет качество прогноза."""
    if outcome.feature in [1, 2, 3, 4, 5, 6, 7]:  # Классификация
        prediction_correct = outcome.outcome == actual_result
        prediction_accuracy = float(outcome.probability)
    elif outcome.feature in [8, 9, 10]:  # Регрессия
        prediction_correct = outcome.lower_bound <= actual_value <= outcome.upper_bound
        prediction_accuracy = 1.0 if prediction_correct else 0.0
        prediction_error = abs(float(outcome.forecast) - actual_value)
    
    return prediction_correct, prediction_accuracy, prediction_error
```

### 3. Представление для фронтенда
```sql
CREATE VIEW statistics_full AS
SELECT 
    s.*,
    o.probability,
    o.confidence,
    o.uncertainty,
    o.lower_bound,
    o.upper_bound,
    m.numOfHeadsHome,
    m.numOfHeadsAway,
    th.teamName as team_home_name,
    ta.teamName as team_away_name,
    ch.championshipName,
    sp.sportName
FROM statistics_optimized s
LEFT JOIN outcomes o ON s.outcome_id = o.id
LEFT JOIN matchs m ON s.match_id = m.id
LEFT JOIN teams th ON m.teamHome_id = th.id
LEFT JOIN teams ta ON m.teamAway_id = ta.id
LEFT JOIN championships ch ON s.championship_id = ch.id
LEFT JOIN sports sp ON s.sport_id = sp.id;
```

## 📈 Мониторинг и тестирование

### 1. Проверка целостности данных
```python
def verify_data_integrity():
    """Проверяет целостность данных между таблицами."""
    with Session_pool() as db_session:
        # Проверяем, что все outcomes имеют соответствующие записи в statistics
        orphaned_outcomes = (
            db_session.query(Outcome)
            .outerjoin(StatisticsOptimized, Outcome.id == StatisticsOptimized.outcome_id)
            .filter(StatisticsOptimized.outcome_id.is_(None))
            .count()
        )
        
        return {
            'orphaned_outcomes': orphaned_outcomes,
            'total_outcomes': db_session.query(Outcome).count(),
            'total_statistics': db_session.query(StatisticsOptimized).count()
        }
```

### 2. Тестирование производительности
```python
def test_query_performance():
    """Тестирует производительность запросов."""
    import time
    
    with Session_pool() as db_session:
        # Тест запроса через представление
        start_time = time.time()
        result = db_session.execute("SELECT * FROM statistics_full LIMIT 1000").fetchall()
        view_time = time.time() - start_time
        
        # Тест запроса через JOIN
        start_time = time.time()
        result = db_session.query(StatisticsOptimized).join(Outcome).limit(1000).all()
        join_time = time.time() - start_time
        
        return {
            'view_query_time': view_time,
            'join_query_time': join_time,
            'performance_ratio': join_time / view_time
        }
```

## 🚀 План внедрения

### Фаза 1: Подготовка (1-2 дня)
1. ✅ Создание миграции
2. ✅ Создание моделей
3. ✅ Создание сервисов интеграции
4. ✅ Тестирование на тестовых данных

### Фаза 2: Интеграция (2-3 дня)
1. Обновление `processing/conformal_predictor.py`
2. Обновление `db/storage/forecast.py`
3. Обновление `db/storage/processing.py`
4. Создание скрипта миграции данных

### Фаза 3: Тестирование (1-2 дня)
1. Тестирование на существующих данных
2. Проверка целостности данных
3. Тестирование производительности
4. Исправление ошибок

### Фаза 4: Внедрение (1 день)
1. Применение миграции в продакшене
2. Миграция существующих данных
3. Мониторинг работы системы
4. Документирование изменений

## ⚠️ Риски и митигация

### Риск 1: Нарушение существующей функциональности
**Митигация**: Постепенное внедрение с сохранением обратной совместимости

### Риск 2: Производительность запросов
**Митигация**: Использование индексов и представлений для оптимизации

### Риск 3: Потеря данных при миграции
**Митигация**: Резервное копирование и поэтапная миграция

### Риск 4: Несовместимость с существующими запросами
**Митигация**: Создание представлений для совместимости

## 📋 Чек-лист внедрения

- [ ] Применить миграцию `optimize_statistics_table.py`
- [ ] Создать представление `statistics_full`
- [ ] Обновить `processing/conformal_predictor.py`
- [ ] Обновить `db/storage/forecast.py`
- [ ] Обновить `db/storage/processing.py`
- [ ] Создать скрипт миграции данных
- [ ] Протестировать на тестовых данных
- [ ] Проверить целостность данных
- [ ] Протестировать производительность
- [ ] Внедрить в продакшен
- [ ] Мигрировать существующие данные
- [ ] Мониторить работу системы
- [ ] Обновить документацию
