# Исправление исторической точности в regular прогнозах

## 🔴 Проблема

В **regular прогнозах** (из таблицы `outcomes`) историческая точность показывала `0/0 (0.0%)` для всех прогнозов:

```
• WIN_DRAW_LOSS: П1: п1
  📉 Историческая точность: 0/0 (0.0%) | ❄️ Последние 10: 0/10 (0.0%)

• OZ (Обе забьют): ДА: обе забьют - да
  📉 Историческая точность: 0/0 (0.0%) | ❄️ Последние 10: 0/10 (0.0%)
```

## 🔍 Причина

### Архитектура получения статистики

**Было:**
```python
# format_daily_forecasts_regular()
for forecast in forecasts:
    feature = forecast.get('feature', 0)      # feature = 1 (WIN_DRAW_LOSS)
    outcome = forecast.get('outcome', '')     # outcome = 'п1'
    
    # ❌ ПРОБЛЕМА: outcome не передается!
    hist_stats = self._get_extended_statistics_for_feature(feature)
    
# _get_extended_statistics_for_feature(feature)
def _get_extended_statistics_for_feature(self, feature: int):
    forecast_type = FEATURE_TYPE_MAPPING.get(feature)  # 'WIN_DRAW_LOSS'
    
    # ❌ ПРОБЛЕМА: пустой forecast_subtype!
    stats = get_complete_statistics(forecast_type, forecast_subtype='')
    #                                                      ^^^^^
    #                                              Ищет статистику для
    #                                              WIN_DRAW_LOSS + ''
    #                                              вместо
    #                                              WIN_DRAW_LOSS + 'п1'
```

### Почему это проблема?

В таблице `statistics`:
- `forecast_type` = `'WIN_DRAW_LOSS'` (или `'win_draw_loss'`)
- `forecast_subtype` = `'п1'`, `'х'`, `'п2'`

Запрос с `forecast_subtype=''` **не находит** записи, потому что:
```sql
SELECT * FROM statistics 
WHERE forecast_type = 'WIN_DRAW_LOSS' 
  AND forecast_subtype = ''  -- ❌ Нет таких записей!
```

Нужно:
```sql
SELECT * FROM statistics 
WHERE forecast_type = 'WIN_DRAW_LOSS' 
  AND forecast_subtype = 'п1'  -- ✅ Найдет записи!
```

## ✅ Решение

### 1. Передача `outcome` в метод статистики

**Файл:** `publisher/formatters/forecast_formatter.py`

```python
# БЫЛО (строка 51):
hist_stats = self._get_extended_statistics_for_feature(feature)

# СТАЛО:
hist_stats = self._get_extended_statistics_for_feature(feature, outcome)
#                                                               ^^^^^^^^
#                                                        Передаем outcome!
```

### 2. Обновление сигнатуры метода

**Файл:** `publisher/formatters/forecast_formatter.py`

```python
# БЫЛО:
def _get_extended_statistics_for_feature(self, feature: int) -> Dict:
    forecast_type = FEATURE_TYPE_MAPPING.get(feature, 'Unknown')
    stats = get_complete_statistics(forecast_type, forecast_subtype='')
    #                                                      ^^^^^
    #                                                     Пусто!

# СТАЛО:
def _get_extended_statistics_for_feature(self, feature: int, outcome: str = '') -> Dict:
    forecast_type = FEATURE_TYPE_MAPPING.get(feature, 'Unknown')
    
    # Нормализуем outcome для использования в БД (lowercase)
    forecast_subtype = outcome.lower().strip() if outcome else ''
    #                  ^^^^^^^^^^^^^^^^^^^^^^^^
    #                  'п1' -> 'п1'
    #                  'обе забьют - да' -> 'обе забьют - да'
    
    stats = get_complete_statistics(forecast_type, forecast_subtype=forecast_subtype)
    #                                                      ^^^^^^^^^^^^^^^^
    #                                                      Правильный подтип!
```

### 3. Аналогичное исправление в `statistics_publisher.py`

**Файл:** `publisher/statistics_publisher.py`

Метод `_publish_daily_outcomes_regular()`:

```python
# БЫЛО (строка 290):
hist_stats = self._get_extended_statistics_for_feature(feature)

# СТАЛО:
hist_stats = self._get_extended_statistics_for_feature(feature, forecast_value)
#                                                               ^^^^^^^^^^^^^^^
#                                                        Передаем прогноз!
```

И обновление метода `_get_extended_statistics_for_feature()` аналогично.

## 📊 Результат

### До исправления

```
• WIN_DRAW_LOSS: П1: п1
  📉 Историческая точность: 0/0 (0.0%) | ❄️ Последние 10: 0/10 (0.0%)
```

### После исправления

```
• WIN_DRAW_LOSS: П1: п1
  📊 Историческая точность: 4509/10716 (42.1%) | ❄️ Последние 10: 7/10 (70.0%)
```

## 🎯 Преимущества

1. **Корректная статистика:** Теперь показывается реальная точность для конкретного типа прогноза
2. **Согласованность:** Regular и quality прогнозы используют одинаковую логику
3. **Кеширование:** Благодаря LRU кешу повторные запросы для одного типа мгновенные

## 📝 Изменённые файлы

| Файл | Метод | Изменения |
|------|-------|-----------|
| `publisher/formatters/forecast_formatter.py` | `format_daily_forecasts_regular()` | ✅ Передает `outcome` в `_get_extended_statistics_for_feature()` |
| `publisher/formatters/forecast_formatter.py` | `_get_extended_statistics_for_feature()` | ✅ Принимает `outcome`, использует как `forecast_subtype` |
| `publisher/statistics_publisher.py` | `_publish_daily_outcomes_regular()` | ✅ Передает `forecast_value` в `_get_extended_statistics_for_feature()` |
| `publisher/statistics_publisher.py` | `_get_extended_statistics_for_feature()` | ✅ Принимает `outcome`, использует как `forecast_subtype` |

## ⚠️ Важно

### Нормализация outcome

`outcome` нормализуется в lowercase перед использованием:
```python
forecast_subtype = outcome.lower().strip() if outcome else ''
```

Это соответствует тому, как данные хранятся в таблице `statistics`:
- `'П1'` → `'п1'`
- `'ТБ'` → `'тб'`
- `'Обе забьют - да'` → `'обе забьют - да'`

### Обратная совместимость

Параметр `outcome` опционален (`outcome: str = ''`), поэтому старые вызовы без второго параметра продолжат работать (вернут агрегированную статистику для всего типа прогноза).

