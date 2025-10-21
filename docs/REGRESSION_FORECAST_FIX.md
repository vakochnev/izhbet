# Исправление предупреждения при проверке регрессионных прогнозов

## 🔴 Проблема

При запуске модулей появлялось предупреждение:

```
[WARNING] prediction_validator.py->is_prediction_correct_from_target():109 - 
Не удалось определить правильность прогноза: feature=10, outcome=3.27
```

## 🔍 Причина

### Как работают регрессионные модели (feature 8, 9, 10)

| Feature | Тип | Прогноз | Пример значения |
|---------|-----|---------|-----------------|
| 8 | `TOTAL_AMOUNT` | Общий тотал (регрессия) | `"5.2"` |
| 9 | `TOTAL_HOME_AMOUNT` | Тотал хозяев (регрессия) | `"2.7"` |
| 10 | `TOTAL_AWAY_AMOUNT` | Тотал гостей (регрессия) | `"3.27"` |

### Проблемный сценарий

1. **Сохранение прогноза в таблицу `outcomes`:**
   ```python
   outcome.feature = 10  # TOTAL_AWAY_AMOUNT
   outcome.outcome = "3.27"  # Числовое значение регрессии
   ```

2. **Интеграция в таблицу `statistics` (db/storage/statistic.py:180):**
   ```python
   forecast_subtype = outcome.outcome or 'unknown'
   # forecast_subtype = "3.27" ❌
   ```

3. **Проверка правильности прогноза (core/prediction_validator.py):**
   ```python
   def is_prediction_correct_from_target(feature: int, outcome: str, target: Target) -> bool:
       # feature = 10
       # outcome = "3.27" ❌
       
       elif feature == 10:
           if outcome_lower == 'ит2м':  # Ожидает категорию!
               return target.target_total_away_under == 1
           elif outcome_lower == 'ит2б':  # Ожидает категорию!
               return target.target_total_away_over == 1
       
       # НЕ НАЙДЕНО СОВПАДЕНИЕ!
       logger.warning(f"Не удалось определить правильность прогноза: feature={feature}, outcome={outcome}")
       return False
   ```

### Суть проблемы

- **Регрессионная модель** выдает **числовое значение** (3.27)
- **Валидатор** ожидает **категорию** ('ит2б' или 'ит2м')
- **Несоответствие типов** → предупреждение

## ✅ Решение

Преобразовывать регрессионные значения в категории **при сохранении в таблицу `statistics`** и использовать преобразованное значение при проверке правильности прогноза.

### Изменения в `db/storage/statistic.py`

#### 1. Преобразование при сохранении (строки 181-201)

```python
# Для регрессионных моделей преобразуем числовое значение в категорию
if model_type == 'regression' and outcome.outcome:
    try:
        forecast_value = float(outcome.outcome)  # 3.27
        
        # Определяем пороговое значение в зависимости от типа спорта
        if forecast_type == 'total_amount':
            threshold = 2.5 if sport.name == 'Soccer' else 4.5
            forecast_subtype = 'тб' if forecast_value > threshold else 'тм'
        
        elif forecast_type == 'total_home_amount':
            threshold = 1.5 if sport.name == 'Soccer' else 2.5
            forecast_subtype = 'ит1б' if forecast_value > threshold else 'ит1м'
        
        elif forecast_type == 'total_away_amount':
            threshold = 1.5 if sport.name == 'Soccer' else 2.5
            forecast_subtype = 'ит2б' if forecast_value > threshold else 'ит2м'
            # Для 3.27 > 2.5 (Хоккей) → 'ит2б' ✅
        
        else:
            forecast_subtype = outcome.outcome or 'unknown'
    
    except (ValueError, TypeError):
        logger.warning(f"Не удалось преобразовать регрессионное значение: {outcome.outcome}")
        forecast_subtype = outcome.outcome or 'unknown'
else:
    forecast_subtype = outcome.outcome or 'unknown'
```

### Пороговые значения (из `core/constants.py`)

```python
SIZE_TOTAL = {'Soccer': 2.5, 'Ice Hockey': 4.5}  # Общий тотал
SIZE_ITOTAL = {'Soccer': 1.5, 'Ice Hockey': 2.5}  # Индивидуальный тотал
```

## 📊 Результат

### До исправления

```
[WARNING] prediction_validator.py - Не удалось определить правильность прогноза: feature=10, outcome=3.27
```

В таблице `statistics`:
```sql
forecast_type       | forecast_subtype | prediction_correct
--------------------|------------------|-------------------
total_away_amount   | 3.27             | NULL
```

### После исправления

✅ **Предупреждений нет**

В таблице `statistics`:
```sql
forecast_type       | forecast_subtype | prediction_correct
--------------------|------------------|-------------------
total_away_amount   | ит2б             | 1 (или 0)
```

## 🎯 Преимущества решения

1. **Совместимость:** `forecast_subtype` теперь в том же формате, что ожидает валидатор
2. **Сохранение информации:** Числовое значение остается в `outcomes.outcome` (3.27)
3. **Корректная валидация:** `prediction_validator.py` может определить правильность прогноза
4. **Унификация:** Все типы прогнозов (классификация и регрессия) проверяются единообразно

#### 2. Использование преобразованного значения (строка 426)

При обновлении результатов матча в методе `update_match_results()`:

```python
# БЫЛО (строка 424):
is_success = is_prediction_correct_from_target(feature, outcome.outcome, target)
#                                                       ^^^^^^^^^^^^^^
#                                                       "4.54" - числовое значение!

# СТАЛО (строка 426):
is_success = is_prediction_correct_from_target(feature, statistic.forecast_subtype, target)
#                                                       ^^^^^^^^^^^^^^^^^^^^^^^^
#                                                       "ит1б" - уже преобразовано!
```

**Почему это важно:**
- `outcome.outcome` хранит исходное регрессионное значение (`"4.54"`)
- `statistic.forecast_subtype` хранит преобразованную категорию (`"ит1б"`)
- `prediction_validator` ожидает категорию, а не число

#### 3. Преобразование в forecast.py (строки 187-201)

При проверке правильности прогнозов в модуле `forecast`:

```python
# forecast/forecast.py, в is_forecast_correct()

# БЫЛО:
feature = forecast_type_to_feature.get(forecast_type)
return is_prediction_correct_from_target(feature, outcome, target)
#                                                 ^^^^^^^
#                                                 "4.79" - числовое значение!

# СТАЛО:
feature, model_type = forecast_type_to_feature.get(forecast_type)

# Для регрессионных моделей преобразуем числовое значение в категорию
if model_type == 'regression' and outcome:
    try:
        forecast_value = float(outcome)
        sport_name = match.get('sportName', 'Soccer')
        
        if forecast_type == 'total_amount':
            threshold = 2.5 if sport_name == 'Soccer' else 4.5
            outcome = 'тб' if forecast_value > threshold else 'тм'
        elif forecast_type == 'total_home_amount':
            threshold = 1.5 if sport_name == 'Soccer' else 2.5
            outcome = 'ит1б' if forecast_value > threshold else 'ит1м'
        elif forecast_type == 'total_away_amount':
            threshold = 1.5 if sport_name == 'Soccer' else 2.5
            outcome = 'ит2б' if forecast_value > threshold else 'ит2м'
    except (ValueError, TypeError) as e:
        logger.warning(f"Не удалось преобразовать регрессионное значение {outcome}: {e}")
        return False

return is_prediction_correct_from_target(feature, outcome, target)
#                                                 ^^^^^^^
#                                                 "ит2б" - категория!
```

**Почему это важно:**
- В модуле `forecast` прогнозы проверяются через `is_forecast_correct()`
- `forecast_data['outcome']` может содержать регрессионное значение (`"4.79"`)
- Без преобразования валидатор выдает предупреждение
- Теперь преобразование происходит **перед** вызовом валидатора

## 📝 Примечание

Числовое значение регрессии **НЕ ТЕРЯЕТСЯ**:
- Сохраняется в `outcomes.outcome` = "3.27"
- Преобразуется в категорию только для `statistics.forecast_subtype` = "ит2б"
- При необходимости можно получить через JOIN с таблицей `outcomes`

### Три места исправления

1. **`db/storage/statistic.py::integrate_outcome_to_statistics()`** (строки 181-201) - преобразует при сохранении в таблицу `statistics`
2. **`db/storage/statistic.py::update_match_results()`** (строка 426) - использует преобразованное значение при проверке
3. **`forecast/forecast.py::is_forecast_correct()`** (строки 187-201) - преобразует перед передачей в валидатор

