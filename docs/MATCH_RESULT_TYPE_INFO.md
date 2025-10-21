# Добавление информации о типе окончания матча в отчеты

## 🔴 Проблема

В отчетах об итогах матчей отсутствовала информация о **типе окончания** (`typeOutcome`).

### Пример проблемы

**Матч 32588206:**
- Основное время: 3:3 (ничья)
- Итоговый счет: 3:4 (победа гостей)
- `typeOutcome` = `'ap'` (послематчевые пенальти)

**Было в отчете:**
```
🆔 Match ID: 32588206
📊 Результат: 3:4
```

❌ **Проблема:** Непонятно, что матч закончился пенальти, а не в основное время.

## ✅ Решение

Добавлена информация о типе окончания матча в строку с результатом.

### Типы окончания матча (`typeOutcome`)

| Код | Значение | Отображение |
|-----|----------|-------------|
| `None` | Основное время | (ничего не добавляется) |
| `'ot'` | Овертайм | `(Овертайм)` |
| `'ap'` | Послематчевые пенальти | `(Пенальти)` |
| `'so'` | Буллиты (хоккей) | `(Буллиты)` |
| `'et'` | Дополнительное время | `(Доп. время)` |

### Реализация

#### 1. Создана функция форматирования

**Файл:** `publisher/formatters/outcome_formatter.py`

```python
def _format_match_result_type(type_outcome: str) -> str:
    """
    Форматирует тип окончания матча.
    
    Args:
        type_outcome: Тип окончания (ot, ap, или None)
        
    Returns:
        str: Форматированная строка
    """
    if not type_outcome:
        return ""
    
    type_mapping = {
        'ot': ' (Овертайм)',
        'ap': ' (Пенальти)',
        'so': ' (Буллиты)',
        'et': ' (Доп. время)',
    }
    
    return type_mapping.get(type_outcome.lower(), f' ({type_outcome.upper()})')
```

#### 2. Обновлены форматтеры итогов

**Файл:** `publisher/formatters/outcome_formatter.py`

**Метод:** `format_daily_outcomes_regular()`

```python
# БЫЛО:
report += f"📊 Счет: {match.get('numOfHeadsHome', '-')} : {match.get('numOfHeadsAway', '-')}\n\n"

# СТАЛО:
result_type = _format_match_result_type(match.get('typeOutcome'))
report += f"📊 Счет: {match.get('numOfHeadsHome', '-')} : {match.get('numOfHeadsAway', '-')}{result_type}\n\n"
```

**Метод:** `format_daily_outcomes_quality()` - аналогично.

#### 3. Обновлены методы в `statistics_publisher.py`

**Файл:** `publisher/statistics_publisher.py`

Добавлен метод `_format_match_result_type()` (аналогичный форматтеру).

Обновлены методы:
- `_publish_daily_outcomes_regular()` 
- `_publish_daily_outcomes_quality()`

```python
# Форматируем тип окончания матча
result_type = self._format_match_result_type(match.get('typeOutcome'))

report += f"📊 Результат: {home_goals}:{away_goals}{result_type}\n"
```

## 📊 Результат

### До изменений

```
🆔 Match ID: 32588206
🏆 Ice Hockey - OHL
⚽ Saginaw vs Flint
📊 Результат: 3:4
```

### После изменений

```
🆔 Match ID: 32588206
🏆 Ice Hockey - OHL
⚽ Saginaw vs Flint
📊 Результат: 3:4 (Пенальти)
```

### Примеры для разных типов

#### Матч в основное время
```
📊 Результат: 2:1
```

#### Матч с овертаймом
```
📊 Результат: 3:2 (Овертайм)
```

#### Матч с пенальти
```
📊 Результат: 3:4 (Пенальти)
```

#### Матч с буллитами (хоккей)
```
📊 Результат: 2:3 (Буллиты)
```

## 📝 Изменённые файлы

| Файл | Изменения |
|------|-----------|
| `publisher/formatters/outcome_formatter.py` | ✅ Добавлена функция `_format_match_result_type()` |
| `publisher/formatters/outcome_formatter.py` | ✅ Обновлен `format_daily_outcomes_regular()` |
| `publisher/formatters/outcome_formatter.py` | ✅ Обновлен `format_daily_outcomes_quality()` |
| `publisher/statistics_publisher.py` | ✅ Добавлен метод `_format_match_result_type()` |
| `publisher/statistics_publisher.py` | ✅ Обновлен `_publish_daily_outcomes_regular()` |
| `publisher/statistics_publisher.py` | ✅ Обновлен `_publish_daily_outcomes_quality()` |

## 🎯 Применимость

Изменения затрагивают **только итоги матчей** (outcomes):
- ✅ Regular итоги
- ✅ Quality итоги
- ❌ Прогнозы (там нет результата матча)

## ⚠️ Важно

### Обработка неизвестных типов

Если встречается неизвестный код `typeOutcome`, он будет отображен в верхнем регистре:

```python
# Для typeOutcome = 'abc'
return f' ({type_outcome.upper()})'  # → ' (ABC)'
```

### Пустое значение

Если `typeOutcome` = `None` или пустая строка, ничего не добавляется:

```python
if not type_outcome:
    return ""
```

## 📋 Влияние на прогнозы

Прогнозы **НЕ затронуты**, так как:
1. Матч еще не состоялся → нет `typeOutcome`
2. В прогнозах нет строки с результатом матча

