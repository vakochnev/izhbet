"""
Конфигурация порогов качества для отбора прогнозов в таблицу `statistics`.

Менять пороги можно здесь. Используется модулем `forecast.quality_selector`.
"""

# Пороговые значения по типам прогнозов
QUALITY_THRESHOLDS = {
    # Трёхисходка: домой/ничья/в гости
    'win_draw_loss': {
        'min_probability': .0, #0.55,
        'min_confidence': .0, #0.80,
    },
    # Обе забьют: да/нет
    'oz': {
        'min_probability': .0, #0.55,
        'min_confidence': .0, #0.80,
    },
    # Тоталы (категоризированные как классы)
    'total': {
        'min_probability': .0, #0.55,
        'min_confidence': .0, #0.80,
    },
    'total_home': {
        'min_probability': .0, #0.55,
        'min_confidence': .0, #0.80,
    },
    'total_away': {
        'min_probability': .0, #0.55,
        'min_confidence': .0, #0.80,
    },
    # Регрессии (например, total_amount) — как правило, не публикуем напрямую
    # Можно включить позднее с иным правилом, пока отключено
    'total_amount': None,
    'total_home_amount': None,
    'total_away_amount': None,
}

# Ограничение Top-N (на дату/турнир) можно использовать в публикации
TOP_N_PER_SCOPE = 20


