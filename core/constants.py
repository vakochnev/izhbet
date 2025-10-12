# izhbet/core/constants.py
"""
Константы проекта.
"""

import datetime as dt
import time

# Временные константы
today = (dt.datetime.utcnow())
today_begin = dt.datetime(
    today.year, today.month, today.day,
    hour=0, minute=0, second=0
)
today_end = dt.datetime(
    today.year, today.month, today.day,
    hour=23, minute=59, second=59
)

yesterday = dt.datetime.utcnow() - dt.timedelta(hours=24)
yesterday_begin = dt.datetime(
    yesterday.year, yesterday.month, yesterday.day,
    hour=0, minute=0, second=0
)
yesterday_end = dt.datetime(
    yesterday.year, yesterday.month, yesterday.day,
    hour=23, minute=59, second=59
)
today_begin_sec = time.mktime(
    dt.datetime(
        today.year, today.month, today.day,
        hour=0, minute=0
    ).timetuple()
)
today_end_sec = time.mktime(
    dt.datetime(
        today.year, today.month, today.day,
        hour=23, minute=59, second=59
    ).timetuple()
)
yesterday_begin_sec = time.mktime(
    dt.datetime(
        yesterday.year, yesterday.month, yesterday.day,
        hour=0, minute=0
    ).timetuple()
)
yesterday_end_sec = time.mktime(
    dt.datetime(
        yesterday.year, yesterday.month, yesterday.day,
        hour=23, minute=59, second=59
    ).timetuple()
)

URL_CATEGORIES = 'https://stat-api.baltbet.ru/api/categories'
URL_COUNTRYS = 'https://stat-api.baltbet.ru/api/sports/%s/tournaments'
URL_TOURNAMENTS = 'https://stat-api.baltbet.ru/api/tournaments/%s/seasons'
URL_MATCHES = 'https://stat-api.baltbet.ru/api/seasons/%s/matches'
URL_COEFS = 'https://stat-api.baltbet.ru/api/coefs/%s'

URL_TABLES = 'https://stat-api.baltbet.ru/api/seasons/%s/league-tables'
# URL_EVENTS = 'https://stat-api.baltbet.ru/api/matches/%s/events?
# eventTypeId=15&eventTypeId=16&eventTypeId=64&eventTypeId=65&eventTypeId=25'
URL_DETAILS = 'https://stat-api.baltbet.ru/api/matches/%s/statistic-details'
# https://stat-api.baltbet.ru/api/matches/%s/schema

# Действия и операции
# Способ построение турнирной таблицы - отбор команд
# LATELY - Отбор игр, для построение вектора, он пойдет на вход модели
# ALL_TIME - Отбор всех их в чемпионате за всю историю для создания модели.
OPERATIONS = ['INIT_DB', 'UPDATE_DB']
TIME_FRAME = ['TODAY', 'ALL_TIME', 'LATELY', 'QUALITY', 'QUALITY_OUTCOMES', 'FUNNEL']
ACTION_MODEL = ['CREATE_MODEL', 'CREATE_PROGNOZ']
#FORECAST = ['CURRENT_YEAR', 'FUTURE']
FORECAST = ['TODAY', 'ALL_TIME']

BATCH_SIZE = 5

# Типы данных
MATCH_TYPE = {
    'sport_id': 'int32', 'country_id': 'int32', 'tournament_id': 'int32',
    #'gameData': datetime,
    'teamHome_id': 'int32', 'teamAway_id': 'int32',
    'tour': 'int32', 'numOfHeadsHome': 'int32', 'numOfHeadsAway': 'int32',
    'winner': 'string', 'typeOutcome': 'string', 'gameComment': 'string',
    'isCanceled': 'bool', 'season_id': 'int32', 'stages_id': 'int32'
}

# Спортивные константы
SPR_SPORTS = {1: 'Soccer', 4: 'Ice Hockey'}
SPR_SPORTS_RU = {'Soccer': '⚽ Футбол', 'Ice Hockey': 'Хоккей'}
SIZE_TOTAL = {'Soccer': 2.5, 'Ice Hockey': 4.5}
SIZE_ITOTAL = {'Soccer': 1.5, 'Ice Hockey': 2.5}

# One-Hot encoding для категориальных переменных
# Win/Draw/Loss: [home_win, draw, away_win]
WIN_DRAW_LOSS_ONEHOT = {
    'home_win': [1, 0, 0],  # Победа домашней команды
    'draw':     [0, 1, 0],  # Ничья
    'away_win': [0, 0, 1]   # Победа гостевой команды
}

# Обе забьют: [both_score, not_both_score]
BOTH_SCORE_ONEHOT = {
    'both_score':    [1, 0],  # Обе команды забили
    'not_both_score': [0, 1]  # Не обе забили
}

# Тотал больше/меньше: [total_over, total_under]
TOTAL_ONEHOT = {
    'total_over':  [1, 0],  # Тотал больше
    'total_under': [0, 1]   # Тотал меньше
}

# Индивидуальный тотал домашней команды: [home_over, home_under]
HOME_TOTAL_ONEHOT = {
    'home_over':  [1, 0],  # Тотал домашней больше
    'home_under': [0, 1]   # Тотал домашней меньше
}

# Индивидуальный тотал гостевой команды: [away_over, away_under]
AWAY_TOTAL_ONEHOT = {
    'away_over':  [1, 0],  # Тотал гостевой больше
    'away_under': [0, 1]   # Тотал гостевой меньше
}

# Забьет ли домашняя команда: [home_scores, home_no_score]
HOME_GOAL_ONEHOT = {
    'home_scores':   [1, 0],  # Домашняя забила
    'home_no_score': [0, 1]   # Домашняя не забила
}

# Забьет ли гостевая команда: [away_scores, away_no_score]
AWAY_GOAL_ONEHOT = {
    'away_scores':   [1, 0],  # Гостевая забила
    'away_no_score': [0, 1]   # Гостевая не забила
}

# Обратная совместимость (для постепенного перехода)
CATEGORICAL_VARIABLE_P1 = 2
CATEGORICAL_VARIABLE_X = 1
CATEGORICAL_VARIABLE_P2 = 0

CATEGORICAL_VARIABLE_OZY = 1
CATEGORICAL_VARIABLE_OZN = 0

CATEGORICAL_VARIABLE_TB = 1
CATEGORICAL_VARIABLE_TM = 0

CATEGORICAL_VARIABLE_TBH = 1
CATEGORICAL_VARIABLE_TMH = 0

CATEGORICAL_VARIABLE_TBG = 1
CATEGORICAL_VARIABLE_TMG = 0

CATEGORICAL_VARIABLE_GYH = 1
CATEGORICAL_VARIABLE_GNH = 0

CATEGORICAL_VARIABLE_GYG = 1
CATEGORICAL_VARIABLE_GNG = 0

STRATEGY_RECORD = {
    'generaltablestrategy': 'general',
    'homegamestablestrategy': 'home',
    'awaygamestablestrategy': 'away',
    'strongopponentstablestrategy': 'strong',
    'mediumopponentstablestrategy': 'medium',
    'weakopponentstablestrategy': 'weak',
    'homegamesstrongopponentstablestrategy': 'home_strong',
    'homegamesmediumopponentstablestrategy': 'home_medium',
    'homegamesweakopponentstablestrategy': 'home_weak',
    'awaygamesstrongopponentstablestrategy': 'away_strong',
    'awaygamesmediumopponentstablestrategy': 'away_medium',
    'awaygamesweakopponentstablestrategy': 'away_weak'
}

# Поля данных
NOT_IN_FEATURE = [
    '_sa_instance_state', 'id', 'sport_id', 'country_id',
    'tournament_id', 'team_id', 'match_id', 'gameData'
]

TARGET_FIELDS = [
    # One-Hot encoded категориальные переменные
    'target_win_draw_loss_home_win', 'target_win_draw_loss_draw', 'target_win_draw_loss_away_win',
    'target_oz_both_score', 'target_oz_not_both_score',
    'target_goal_home_yes', 'target_goal_home_no',
    'target_goal_away_yes', 'target_goal_away_no',
    'target_total_over', 'target_total_under',
    'target_total_home_over', 'target_total_home_under',
    'target_total_away_over', 'target_total_away_under',
    # Регрессионные переменные
    'target_total_amount', 'target_total_home_amount', 'target_total_away_amount',
]

DROP_FIELD_EMBEDDING = [
    'match_id', 'updated_at', 'created_at', 'id'
]

DROP_FIELD_BLOWOUTS = [
    'general_elo_rating', 'general_vo_rating', 'general_dif_rating',
    'general_potemkin_rating', 'general_power_rating',
    'home_elo_rating', 'home_vo_rating', 'home_dif_rating',
    'home_potemkin_rating', 'home_power_rating', 'away_elo_rating',
    'away_vo_rating', 'away_dif_rating', 'away_potemkin_rating',
    'away_power_rating', 'strong_elo_rating', 'strong_vo_rating',
    'strong_dif_rating', 'strong_potemkin_rating',
    'strong_power_rating', 'medium_elo_rating', 'medium_vo_rating',
    'medium_dif_rating', 'medium_potemkin_rating',
    'medium_power_rating', 'weak_elo_rating', 'weak_vo_rating',
    'weak_dif_rating', 'weak_potemkin_rating', 'weak_power_rating',
    'home_strong_elo_rating', 'home_strong_vo_rating',
    'home_strong_dif_rating', 'home_strong_potemkin_rating',
    'home_strong_power_rating', 'home_medium_elo_rating',
    'home_medium_vo_rating', 'home_medium_dif_rating',
    'home_medium_potemkin_rating', 'home_medium_power_rating',
    'home_weak_elo_rating', 'home_weak_vo_rating',
    'home_weak_dif_rating', 'home_weak_potemkin_rating',
    'home_weak_power_rating', 'away_strong_elo_rating',
    'away_strong_vo_rating', 'away_strong_dif_rating',
    'away_strong_potemkin_rating', 'away_strong_power_rating',
    'away_medium_elo_rating', 'away_medium_vo_rating',
    'away_medium_dif_rating', 'away_medium_potemkin_rating',
    'away_medium_power_rating', 'away_weak_elo_rating',
    'away_weak_vo_rating', 'away_weak_dif_rating',
    'away_weak_potemkin_rating', 'away_weak_power_rating'
]

ATTR_NOT_AVERAGE = ['gameData', 'games_played']

ATTR_AVERAGE = [
    'games_wins', 'games_draws', 'games_losses', 'overtime_wins',
    'overtime_losses', 'goals_scored', 'goals_conceded',
    'goals_difference', 'goals_ratio', 'points', 'victory_dry',
    'lossing_dry', 'tb_points', 'tm_points', 'itb_points',
    'itm_points', 'tb05_points', 'tb15_points', 'tb25_points',
    'tb35_points', 'tb45_points', 'tb55_points', 'itb05_points',
    'itb15_points', 'itb25_points', 'itb35_points', 'itb45_points',
    'itb55_points', 'tm05_points', 'tm15_points', 'tm25_points',
    'tm35_points', 'tm45_points', 'tm55_points', 'itm05_points',
    'itm15_points', 'itm25_points', 'itm35_points', 'itm45_points',
    'itm55_points', 'oz_points', 'ozn_points'
]

PREFIX_AVERAGE = [
    'general', 'home', 'away', 'strong', 'medium', 'weak',
    'home_strong', 'home_medium', 'home_weak',
    'away_strong', 'away_medium', 'away_weak'
]

CONVERT_TYPE_EMBEDDING = [
    'away_goals_conceded', 'general_itm55_points', 'general_itm35_points',
    'strong_tb05_points', 'general_itm35_points'
]

POWER = {
    '00': 100, '01': 80, '02': 61, '03': 46, '04': 32,
    '05': 22, '06': 13, '07': 6, '08': 0, '09': 0,
    '10': 120, '11': 100, '12': 81, '13': 64, '14': 49,
    '15': 36, '16': 25, '17': 16, '18': 9, '19': 0,
    '20': 139, '21': 119, '22': 100, '23': 82, '24': 66,
    '25': 51, '26': 39, '27': 28, '28': 19, '29': 12,
    '30': 154, '31': 136, '32': 118, '33': 100, '34': 83,
    '35': 68, '36': 54, '37': 42, '38': 31, '39': 22,
    '40': 168, '41': 151, '42': 134, '43': 117, '44': 100,
    '45': 84, '46': 70, '47': 56, '48': 44, '49': 34,
    '50': 178, '51': 164, '52': 149, '53': 132, '54': 116,
    '55': 100, '56': 85, '57': 71, '58': 58, '59': 47,
    '60': 187, '61': 175, '62': 161, '63': 146, '64': 130,
    '65': 115, '66': 100, '67': 86, '68': 74, '69': 60,
    '70': 194, '71': 184, '72': 172, '73': 158, '74': 144,
    '75': 129, '76': 113, '77': 100, '78': 87, '79': 74,
    '80': 200, '81': 191, '82': 181, '83': 169, '84': 156,
    '85': 142, '86': 128, '87': 113, '88': 100, '89': 87,
    '90': 200, '91': 197, '92': 188, '93': 178, '94': 166,
    '95': 153, '96': 140, '97': 126, '98': 113, '99': 100
}

SPR_COUNTRY_TOP = [
    {'id': 826, 'sId': 4, 'catId': 65}, # Хоккей-Австрия-Национальная Лига
    {'id': 1370, 'sId': 4, 'catId': 275}, # Хоккей-Беларусь-Экстралига
    {'id': 225, 'sId': 4, 'catId': 41}, # Хоккей-Германия-DEL
    {'id': 267, 'sId': 4, 'catId': 41}, # Хоккей-Германия-DEL2
    {'id': 257, 'sId': 4, 'catId': 64}, # Хоккей-Дания-Метал Лиген
    {'id': 30156, 'sId': 4, 'catId': 810}, # Хоккей-Казахстан-Чемпионат
    {'id': 1454, 'sId': 4, 'catId': 176}, # Хоккей-Канада-OHL
    {'id': 30398, 'sId': 4, 'catId': 176}, # Хоккей-Канада-QMJHL
    {'id': 14361, 'sId': 4, 'catId': 176}, # Хоккей-Канада-WHL
    {'id': 19452, 'sId': 4, 'catId': 306}, # Хоккей-Латвия-Чемпионат
    {'id': 828, 'sId': 4, 'catId': 38}, # Хоккей-Норвегия-1-й дивизион
    {'id': 260, 'sId': 4, 'catId': 38}, # Хоккей-Норвегия-Фьордкрафт Лиген
    {'id': 243, 'sId': 4, 'catId': 115}, # Хоккей-Польша-PHL
    {'id': 1141, 'sId': 4, 'catId': 101}, # Хоккей-Россия-ВХЛ
    {'id': 268, 'sId': 4, 'catId': 101}, # Хоккей-Россия-КХЛ
    {'id': 1159, 'sId': 4, 'catId': 101}, # Хоккей-Россия-МХЛ
    {'id': 1490, 'sId': 4, 'catId': 98}, # Хоккей-Словакия-1-я лига
    {'id': 236, 'sId': 4, 'catId': 98}, # Хоккей-Словакия-Экстралига
    {'id': 844, 'sId': 4, 'catId': 37}, # Хоккей-США-AHL
    {'id': 234, 'sId': 4, 'catId': 37}, # Хоккей-США-НХЛ
    {'id': 259, 'sId': 4, 'catId': 40}, # Хоккей-Финляндия-Местис
    {'id': 134, 'sId': 4, 'catId': 40}, # Хоккей-Финляндия-Лиига
    {'id': 599, 'sId': 4, 'catId': 242}, # Хоккей-Франция-Лига Магнуса
    {'id': 735, 'sId': 4, 'catId': 42}, # Хоккей-Чехия-Ченс лига
    {'id': 237, 'sId': 4, 'catId': 42}, # Хоккей-Чехия-Экстралига
    {'id': 128, 'sId': 4, 'catId': 54}, # Хоккей-Швейцария-Национальная лига
    {'id': 129, 'sId': 4, 'catId': 54}, # Хоккей-Швейцария-Суисс лига
    {'id': 416, 'sId': 4, 'catId': 39}, # Хоккей-Швеция-Аллсвенскан
    {'id': 261, 'sId': 4, 'catId': 39}, # Хоккей-Швеция-SHL
    {'id': 714, 'sId': 4, 'catId': 39}, # Хоккей-Швеция-АллЭттан
    {'id': 171, 'sId': 1, 'catId': 102}, # Футбол-Кипр-Чемпионат
    {'id': 649, 'sId': 1, 'catId': 99}, # Футбол-Китай-Чемпионат
    {'id': 27070, 'sId': 1, 'catId': 274}, # Футбол-Колумбия-Чемпионат
    {'id': 14189, 'sId': 1, 'catId': 565}, # Футбол-Косово-Суперлига
    {'id': 1002, 'sId': 1, 'catId': 331}, # Футбол-Кувейт-Премьер Лига
    {'id': 197, 'sId': 1, 'catId': 163}, # Футбол-Латвия-Чемпионат
    {'id': 2386, 'sId': 1, 'catId': 428}, # Футбол-Ливан-Премьер Лига
    {'id': 198, 'sId': 1, 'catId': 160}, # Футбол-Литва-Чемпионат
    {'id': 690, 'sId': 1, 'catId': 197}, # Футбол-Люксембург-Национальный дивизион
    {'id': 19226, 'sId': 1, 'catId': 85}, # Футбол-Малайзия-Премьер Лига
    {'id': 1000, 'sId': 1, 'catId': 85}, # Футбол-Малайзия-Суперлига
    {'id': 629, 'sId': 1, 'catId': 134}, # Футбол-Мальта-Чемпионат
    {'id': 937, 'sId': 1, 'catId': 303}, # Футбол-Марокко-Чемпионат
    {'id': 685, 'sId': 1, 'catId': 279}, # Футбол-Молдова-Чемпионат
    {'id': 2112, 'sId': 1, 'catId': 532}, # Футбол-Нигерия-Премьер-Лига
    {'id': 19242, 'sId': 1, 'catId': 389}, # Футбол-Никарагуа-Примера Дивизион
    {'id': 594, 'sId': 1, 'catId': 148}, # Футбол-Новая Зеландия-Чемпионат
    {'id': 22, 'sId': 1, 'catId': 5}, # Футбол-Норвегия-1-й дивизион
    {'id': 20, 'sId': 1, 'catId': 5}, # Футбол-Норвегия-Чемпионат
    {'id': 971, 'sId': 1, 'catId': 299}, # Футбол-Объединенные Арабские Эмираты-Чемпионат
    {'id': 965, 'sId': 1, 'catId': 415}, # Футбол-Оман-Чемпионат
    {'id': 406, 'sId': 1, 'catId': 20}, # Футбол-Перу-Чемпионат
    {'id': 202, 'sId': 1, 'catId': 47}, # Футбол-Польша-Чемпионат
    {'id': 239, 'sId': 1, 'catId': 44}, # Футбол-Португалия-2-й дивизион
    {'id': 238, 'sId': 1, 'catId': 44}, # Футбол-Португалия-Чемпионат
    {'id': 410, 'sId': 1, 'catId': 291}, # Футбол-Республика Корея-Чемпионат
    {'id': 879, 'sId': 1, 'catId': 21}, # Футбол-Россия-Турнир молодежных команд
    {'id': 203, 'sId': 1, 'catId': 21}, # Футбол-Россия-Премьер Лига
    {'id': 204, 'sId': 1, 'catId': 21}, # Футбол-Россия-1-я лига
    {'id': 20162, 'sId': 1, 'catId': 951}, # Футбол-Руанда-Премьер Лига
    {'id': 152, 'sId': 1, 'catId': 77}, # Футбол-Румыния-Чемпионат
    {'id': 955, 'sId': 1, 'catId': 310}, # Футбол-Саудовская аравия-Чемпионат
    {'id': 200, 'sId': 1, 'catId': 130}, # Футбол-Северная Ирландия-Премьер-лига
    {'id': 701, 'sId': 1, 'catId': 130}, # Футбол-Северная Ирландия-Чемпионшип
    {'id': 1226, 'sId': 1, 'catId': 886}, # Футбол-Сенегал-Лига 1
    {'id': 210, 'sId': 1, 'catId': 152}, # Футбол-Сербия-Чемпионат
    {'id': 634, 'sId': 1, 'catId': 45}, # Футбол-Сингапур-Чемпионат
    {'id': 211, 'sId': 1, 'catId': 23}, # Футбол-Словакия-Чемпионат
    {'id': 212, 'sId': 1, 'catId': 24}, # Футбол-Словения-Чемпионат
    {'id': 242, 'sId': 1, 'catId': 26}, # Футбол-США-MLS
    {'id': 1032, 'sId': 1, 'catId': 485}, # Футбол-Таиланд-Чемпионат
    {'id': 984, 'sId': 1, 'catId': 378}, # Футбол-Тунис-Чемпионат
    {'id': 98, 'sId': 1, 'catId': 46}, # Футбол-Турция-1-я Лига
    {'id': 14864, 'sId': 1, 'catId': 824}, # Футбол-Уганда-Премьер-лига
    {'id': 772, 'sId': 1, 'catId': 385}, # Футбол-Узбекистан-Суперлига
    {'id': 892, 'sId': 1, 'catId': 86}, # Футбол-Украина-Молодежная лига
    {'id': 218, 'sId': 1, 'catId': 86}, # Футбол-Украина-Чемпионат
    {'id': 1908, 'sId': 1, 'catId': 57}, # Футбол-Уругвай-Сегунда дивизион
    {'id': 278, 'sId': 1, 'catId': 57}, # Футбол-Уругвай-Чемпионат
    {'id': 254, 'sId': 1, 'catId': 131}, # Футбол-Уэльс-Премьер Лига
    {'id': 673, 'sId': 1, 'catId': 201}, # Футбол-Фарерские острова-Чемпионат
    {'id': 1654, 'sId': 1, 'catId': 847}, # Футбол-Филиппины-Чемпионат
    {'id': 41, 'sId': 1, 'catId': 19}, # Футбол-Финляндия-Чемпионат
    {'id': 55, 'sId': 1, 'catId': 19}, # Футбол-Финляндия-2-й дивизион
    {'id': 170, 'sId': 1, 'catId': 14}, # Футбол-Хорватия-Чемпионат
    {'id': 724, 'sId': 1, 'catId': 14}, # Футбол-Хорватия-2-й дивизион
    {'id': 717, 'sId': 1, 'catId': 386}, # Футбол-Черногория-2-й дивизион
    {'id': 154, 'sId': 1, 'catId': 386}, # Футбол-Черногория-Чемпионат
    {'id': 172, 'sId': 1, 'catId': 18}, # Футбол-Чехия-Чемпионат
    {'id': 27665, 'sId': 1, 'catId': 49}, # Футбол-Чили-Чемпионат
    {'id': 215, 'sId': 1, 'catId': 25}, # Футбол-Швейцария-Чемпионат
    {'id': 216, 'sId': 1, 'catId': 25}, # Футбол-Швейцария-1-й дивизион
    {'id': 40, 'sId': 1, 'catId': 9}, # Футбол-Швеция-Чемпионат
    {'id': 46, 'sId': 1, 'catId': 9}, # Футбол-Швеция-Суперэттан
    {'id': 207, 'sId': 1, 'catId': 22}, # Футбол-Шотландия-1-я Лига
    {'id': 36, 'sId': 1, 'catId': 22}, # Футбол-Шотландия-Премьер-Лига
    {'id': 206, 'sId': 1, 'catId': 22}, # Футбол-Шотландия-Чемпионшип
    {'id': 178, 'sId': 1, 'catId': 92}, # Футбол-Эстония-Чемпионат
    {'id': 358, 'sId': 1, 'catId': 322}, # Футбол-Южная Африка-Чемпионат
    {'id': 1892, 'sId': 1, 'catId': 502}, # Футбол-Ямайка-Премьер-лига
    {'id': 196, 'sId': 1, 'catId': 52}, # Футбол-Япония-Чемпионат
    {'id': 33656, 'sId': 1, 'catId': 914}, # Футбол-Эфиопия-Премьер-лига
    {'id': 33980, 'sId': 1, 'catId': 379}, # Футбол-Боливия-Чемпионат
    {'id': 27072, 'sId': 1, 'catId': 274}, # Футбол-Колумбия-Чемпионат
    {'id': 1900, 'sId': 1, 'catId': 352}, # Футбол-Индия-Суперлига
    {'id': 1015, 'sId': 1, 'catId': 368}, # Футбол-Индонезия-Лига 1
    {'id': 929, 'sId': 1, 'catId': 329}, # Футбол-Иордания-Чемпионат
    {'id': 915, 'sId': 1, 'catId': 301}, # Футбол-Иран-Чемпионат
    {'id': 193, 'sId': 1, 'catId': 51}, # Футбол-Ирландия-1-й дивизион
    {'id': 192, 'sId': 1, 'catId': 51}, # Футбол-Ирландия-Чемпионат
    {'id': 675, 'sId': 1, 'catId': 10}, # Футбол-Исландия-1-й дивизион
    {'id': 188, 'sId': 1, 'catId': 10}, # Футбол-Исландия-Чемпионат
    {'id': 785, 'sId': 1, 'catId': 278}, # Футбол-Казахстан-1-й дивизион
    {'id': 682, 'sId': 1, 'catId': 278}, # Футбол-Казахстан-Чемпионат
    {'id': 19250, 'sId': 1, 'catId': 852}, # Футбол-Камбоджа-С-Лига
    {'id': 1006, 'sId': 1, 'catId': 391}, # Футбол-Камерун-Чемпионат
    {'id': 28432, 'sId': 1, 'catId': 388}, # Футбол-Канада-Премьер Лига
    {'id': 482, 'sId': 1, 'catId': 388}, # Футбол-Канада-CSL
    {'id': 825, 'sId': 1, 'catId': 353}, # Футбол-Катар-Чемпионат
    {'id': 1644, 'sId': 1, 'catId': 805}, # Футбол-Кения-Премьер Лига
    {'id': 727, 'sId': 1, 'catId': 66}, # Футбол-Израиль-2-й дивизион
    {'id': 266, 'sId': 1, 'catId': 66}, # Футбол-Израиль-Премьер Лигаl
    {'id': 848, 'sId': 1, 'catId': 352}, # Футбол-Индия-Чемпионат
    {'id': 808, 'sId': 1, 'catId': 305}, # Футбол-Египет-Чемпионат
    {'id': 65, 'sId': 1, 'catId': 8}, # Футбол-Дания-2-й дивизион
    {'id': 47, 'sId': 1, 'catId': 8}, # Футбол-Дания-1-й дивизион
    {'id': 704, 'sId': 1, 'catId': 270}, # Футбол-Грузия-Национальная лига
    {'id': 185, 'sId': 1, 'catId': 67}, # Футбол-Греция-Чемпионат
    {'id': 947, 'sId': 1, 'catId': 339}, # Футбол-Гонконг, Китай-Премьер-лига
    {'id': 37, 'sId': 1, 'catId': 35}, # Футбол-Голландия (Нидерланды)-Чемпионат
    {'id': 131, 'sId': 1, 'catId': 35}, # Футбол-Голландия (Нидерланды)-1-й дивизион
    {'id': 1191, 'sId': 1, 'catId': 542}, # Футбол-Гана-Премьер-лига
    {'id': 231, 'sId': 1, 'catId': 281}, # Футбол-Венесуэла-Чемпионат
    {'id': 187, 'sId': 1, 'catId': 11}, # Футбол-Венгрия-Чемпионат
    {'id': 325, 'sId': 1, 'catId': 13}, # Футбол-Бразилия-Серия A'
    {'id': 390, 'sId': 1, 'catId': 13}, # Футбол-Бразилия-Ceрия B
    {'id': 92, 'sId': 1, 'catId': 13}, # Футбол-Бразилия-Кариока
    {'id': 382, 'sId': 1, 'catId': 13}, # Футбол-Бразилия-Паранаэнсе
    {'id': 247, 'sId': 1, 'catId': 78}, # Футбол-Болгария-Чемпионат
    {'id': 38, 'sId': 1, 'catId': 33}, # Футбол-Бельгия-Чемпионат
    {'id': 169, 'sId': 1, 'catId': 91}, # Футбол-Беларусь-Чемпионат
    {'id': 671, 'sId': 1, 'catId': 296}, # Футбол-Армения-Чемпионат
    {'id': 155, 'sId': 1, 'catId': 48}, # Футбол-Аргентина-Чемпионат
    {'id': 841, 'sId': 1, 'catId': 304}, # Футбол-Алжир-Чемпионат
    {'id': 720, 'sId': 1, 'catId': 257}, # Футбол-Албания-Чемпионат
    {'id': 709, 'sId': 1, 'catId': 297}, # Футбол-Азербайджан-Чемпионат
    {'id': 136, 'sId': 1, 'catId': 34}, # Футбол-Австралия-А-Лига
    {'id': 182, 'sId': 1, 'catId': 7},  # Футбол-Франция-Лига 2
    {'id': 34, 'sId': 1, 'catId': 7},  # Футбол-Франция-Лига 1
    {'id': 53, 'sId': 1, 'catId': 31}, # Футбол-Италия-Серия B
    {'id': 23, 'sId': 1, 'catId': 31}, # Футбол-Италия-Серия A
    {'id': 35, 'sId': 1, 'catId': 30}, # Футбол-Германия-Бундеслига
    {'id': 44, 'sId': 1, 'catId': 30}, # Футбол-Германия-2-я Бундеслига
    {'id': 491, 'sId': 1, 'catId': 30}, # Футбол-Германия-3-я Бундеслига
    {'id': 8, 'sId': 1, 'catId': 32}, # Футбол-Испания-Ла Лига
    {'id': 54, 'sId': 1, 'catId': 32}, # Футбол-Испания-Ла Лига 2
    {'id': 173, 'sId': 1, 'catId': 1}, # Футбол-Англия-Национальная лига
    {'id': 17, 'sId': 1, 'catId': 1}, # Футбол-Англия-Премьер-Лига'
    {'id': 18, 'sId': 1, 'catId': 1}, # Футбол-Англия-Чемпионшип
    {'id': 24, 'sId': 1, 'catId': 1},  # Футбол-Англия-1-я Лига
    {'id': 25, 'sId': 1, 'catId': 1}, # Футбол-Англия-2-я Лига
    {'id': 203, 'sId': 1, 'catId': 21}, # Футбол-РФ-Премьер Лига
    {'id': 879, 'sId': 1, 'catId': 21}, # Футбол-РФ-Турнир молодежных команд
    {'id': 204, 'sId': 1, 'catId': 21}, # Футбол-РФ-1-я лига
]

FIELDS_ANALIZ_P1XP2 = [
    'game_data', 'country_name', 'championship_name', 'tour',
    'team_home_name', 'team_away_name', 'goal_home_team',
    'goal_away_team', 'typeOutcome', 'win_draw_loss_p1',
    'win_draw_loss_x', 'win_draw_loss_p2',
    'forecast_win_draw_loss', 'forecast_probability_win_draw_loss',
    'forecast_confidence_win_draw_loss',
    'outcome_forecast_win_draw_loss'
]

FIELD_PREDICTS = [
    'gameData', 'tournament_id', 'sport_id', 'country_id', 'match_id'
]

FORECAST_TO_NUMERIC = {
    'п1': CATEGORICAL_VARIABLE_P1, 'ф0(1)': CATEGORICAL_VARIABLE_P1,
    'х': CATEGORICAL_VARIABLE_X,
    'п2': CATEGORICAL_VARIABLE_P2, 'ф0(2)': CATEGORICAL_VARIABLE_P2,
    'П1': CATEGORICAL_VARIABLE_P1,
    'Х': CATEGORICAL_VARIABLE_X,
    'П2': CATEGORICAL_VARIABLE_P2,
    'обе забьют - да': CATEGORICAL_VARIABLE_OZY,
    'ОБЕ ЗАБЬЮТ - ДА': CATEGORICAL_VARIABLE_OZY,
    'обе забьют - нет': CATEGORICAL_VARIABLE_OZN,
    'ОБЕ ЗАБЬЮТ - НЕТ': CATEGORICAL_VARIABLE_OZN,
    'тб': CATEGORICAL_VARIABLE_TB, 'ТБ': CATEGORICAL_VARIABLE_TB,
    'тм': CATEGORICAL_VARIABLE_TM, 'ТМ': CATEGORICAL_VARIABLE_TM,
    'ит1б': CATEGORICAL_VARIABLE_TBH, 'ИТ1Б': CATEGORICAL_VARIABLE_TBH,
    'ит1м': CATEGORICAL_VARIABLE_TMH, 'ИТ1М': CATEGORICAL_VARIABLE_TMH,
    'ит2б': CATEGORICAL_VARIABLE_TBG, 'ИТ2Б': CATEGORICAL_VARIABLE_TBG,
    'ит2м': CATEGORICAL_VARIABLE_TMG, 'ИТ2М': CATEGORICAL_VARIABLE_TMG,
    '1 забьет - да': CATEGORICAL_VARIABLE_GYH, '1 ЗАБЬЕТ - ДА': CATEGORICAL_VARIABLE_GYH,
    '1 забьет - нет': CATEGORICAL_VARIABLE_GNH, '1 ЗАБЬЕТ - НЕТ': CATEGORICAL_VARIABLE_GNH,
    '2 забьет - да': CATEGORICAL_VARIABLE_GYG, '2 ЗАБЬЕТ - ДА': CATEGORICAL_VARIABLE_GYG,
    '2 забьет - нет': CATEGORICAL_VARIABLE_GNG, '2 ЗАБЬЕТ - НЕТ': CATEGORICAL_VARIABLE_GNG
}

FORECAST_TO_TYPE = {
    'п1': 'win_draw_loss', 'ф0(1)': 'win_draw_loss',
    'п2': 'win_draw_loss', 'ф0(2)': 'win_draw_loss',
    'х': 'win_draw_loss',
    'обе забьют - да': 'oz', 'обе забьют - нет': 'oz',
    '1 забьет - да': 'goal_home', '1 забьет - нет': 'goal_home',
    '2 забьет - да': 'goal_away', '2 забьет - нет': 'goal_away',
    'тб': 'total', 'тм': 'total',
    'ит1б': 'total_home', 'ит1м': 'total_home',
    'ит2б': 'total_away', 'ит2м': 'total_away',
    'ТБ': 'total_amount', 'ТМ': 'total_amount',
    'ИТ1Б': 'total_home_amount', 'ИТ1М': 'total_home_amount',
    'ИТ2Б': 'total_away_amount', 'ИТ2М': 'total_away_amount'
}

FILTER_CONFIG = [
    {
        'type': 'outcome_home',
        'params': {
            'min_probability': 0.,  # .6-.8 9/1=0.9
            'min_confidence': 0.,   # .6-.6 17/6=0.73
            'min_accuracy': 0.7
        }
    },
    {
        'type': 'outcome_draw',
        'params': {
            'min_probability': 0.,  # .4-.4 1/0=1
            'min_confidence': 0.,    # .44-.3 4/1=0.8
            'min_accuracy': 0.7
        }
    },
    {
        'type': 'outcome_away',
        'params': {
            'min_probability': 0.,  # .4-.6 7/4=0.63
            'min_confidence': 0.,
            'min_accuracy': 0.7
        }
    },
    {
        'type': 'oz_yes',
        'params': {
            'min_probability': 0.,  # .6-.8 34/30=0.53
            'min_confidence': 0.,    # .59-.9 54/40=0.57
            'min_accuracy': 0.7
        }
    },
    {
        'type': 'oz_no',
        'params': {
            'min_probability': 0.,     # .6-.6 4/2=0.66
            'min_confidence': 0.,       # .55-.6 48/28=0.63
            'min_accuracy': 0.7
        }
    },
    {
        'type': 'total_b',
        'params': {
            'min_probability': 0.,    # .66-0. 6/3=0.66
            'min_confidence': .0,
            'min_accuracy': 0.7
        }
    },
    {
        'type': 'total_m',
        'params': {
            'min_probability': 0.,
            'min_confidence': 0.,
            'min_accuracy': 0.74
        }
    },
    {
        'type': 'total_home_b',
        'params': {
            'min_probability': 0.,    # .65 45/25=0.64
            'min_confidence': 0.,     # .64 53/31=0.63
            'min_accuracy': 0.7
        }
    },
    {
        'type': 'total_home_m',
        'params': {
            'min_probability': 0.,     # .7 86/41=0.67
            'min_confidence': 0.,      # .75 42/17=0.71
            'min_accuracy': 0.7
        }
    },
    {
        'type': 'total_away_b',
        'params': {
            'min_probability': 0.,      # .8 6/2=0.75
            'min_confidence': 0.,
            'min_accuracy': 0.7
        }
    },
    {
        'type': 'total_away_m',
        'params': {
            'min_probability': 0.,     # .65 7/1=0.87
            'min_confidence': 0.,
            'min_accuracy': 0.7
        }
    },
    {
        'type': 'total_amount_b',
        'params': {
            'min_probability': 0.,
            'min_confidence': 0.,
            'min_accuracy': 0.7
        }
    },
    {
        'type': 'total_amount_m',
        'params': {
            'min_probability': 0.,
            'min_confidence': 0.,
            'min_accuracy': 0.7
        }
    },
    {
        'type': 'total_home_amount_b',
        'params': {
            'min_probability': 0.,
            'min_confidence': 0.,
            'min_accuracy': 0.7
        }
    },
    {
        'type': 'total_home_amount_m',
        'params': {
            'min_probability': 0.,
            'min_confidence': 0.,
            'min_accuracy': 0.7
        }
    },
    {
        'type': 'total_away_amount_b',
        'params': {
            'min_probability': 0.,
            'min_confidence': 0.,
            'min_accuracy': 0.7
        }
    },
    {
        'type': 'total_away_amount_m',
        'params': {
            'min_probability': 0.,
            'min_confidence': 0.,
            'min_accuracy': 0.7
        }
    }
]

FIELDS_RENAME_FILTER = {
    'outcome_forecast_win_draw_loss': 'outcome',
    'forecast_probability_win_draw_loss': 'probability',
    'forecast_confidence_win_draw_loss': 'confidence',
    'forecast_win_draw_loss': 'forecast',
    'accuracy_home_win_draw_loss': 'accuracy_home',
    'accuracy_away_win_draw_loss': 'accuracy_away',

    'outcome_forecast_oz': 'outcome',
    'forecast_probability_oz': 'probability',
    'forecast_confidence_oz': 'confidence',
    'forecast_oz': 'forecast',
    'accuracy_home_oz': 'accuracy_home',
    'accuracy_away_oz': 'accuracy_away',

    'outcome_forecast_total': 'outcome',
    'forecast_probability_total': 'probability',
    'forecast_confidence_total': 'confidence',
    'forecast_total': 'forecast',
    'accuracy_home_total': 'accuracy_home',
    'accuracy_away_total': 'accuracy_away',

    'outcome_forecast_total_home': 'outcome',
    'forecast_probability_total_home': 'probability',
    'forecast_confidence_total_home': 'confidence',
    'forecast_total_home': 'forecast',
    'accuracy_home_total_home': 'accuracy_home',
    'accuracy_away_total_home': 'accuracy_away',

    'outcome_forecast_total_away': 'outcome',
    'forecast_probability_total_away': 'probability',
    'forecast_confidence_total_away': 'confidence',
    'forecast_total_away': 'forecast',
    'accuracy_home_total_away': 'accuracy_home',
    'accuracy_away_total_away': 'accuracy_away',

    'target_total_amount': 'target',
    'outcome_forecast_total_amount': 'outcome',
    'forecast_probability_total_amount': 'probability',
    'forecast_confidence_total_amount': 'confidence',
    'forecast_total_amount': 'forecast',
    'accuracy_home_total_amount': 'accuracy_home',
    'accuracy_away_total_amount': 'accuracy_away',

    'target_total_home_amount': 'target',
    'outcome_forecast_total_home_amount': 'outcome',
    'forecast_probability_total_home_amount': 'probability',
    'forecast_confidence_total_home_amount': 'confidence',
    'forecast_total_home_amount': 'forecast',
    'accuracy_home_total_home_amount': 'accuracy_home',
    'accuracy_away_total_away_amount': 'accuracy_away',

    'target_total_away_amount': 'target',
    'outcome_forecast_total_away_amount': 'outcome',
    'forecast_probability_total_away_amount': 'probability',
    'forecast_confidence_total_away_amount': 'confidence',
    'forecast_total_away_amount': 'forecast'
}

FIELDS_FORECAST_WIN_DRAW_LOSS = [
    'match_id',
    'teamHome_id',
    'teamAway_id',
    'forecast_win_draw_loss',
    'forecast_probability_win_draw_loss',
    'forecast_confidence_win_draw_loss',
    'outcome_forecast_win_draw_loss',
    'accuracy_home_win_draw_loss',
    'accuracy_away_win_draw_loss'
]
FIELDS_FORECAST_OZ = [
    'match_id',
    'teamHome_id',
    'teamAway_id',
    'forecast_oz',
    'forecast_probability_oz',
    'forecast_confidence_oz',
    'outcome_forecast_oz',
    'accuracy_home_oz',
    'accuracy_away_oz'
]
FIELDS_FORECAST_TOTAL = [
    'match_id',
    'teamHome_id',
    'teamAway_id',
    'forecast_total',
    'forecast_probability_total',
    'forecast_confidence_total',
    'outcome_forecast_total',
    'accuracy_home_total',
    'accuracy_away_total'
]
FIELDS_FORECAST_TOTAL_HOME = [
    'match_id',
    'teamHome_id',
    'teamAway_id',
    'forecast_total_home',
    'forecast_probability_total_home',
    'forecast_confidence_total_home',
    'outcome_forecast_total_home',
    'accuracy_home_total_home',
    'accuracy_away_total_home'
]
FIELDS_FORECAST_TOTAL_AWAY = [
    'match_id',
    'teamHome_id',
    'teamAway_id',
    'forecast_total_away',
    'forecast_probability_total_away',
    'forecast_confidence_total_away',
    'outcome_forecast_total_away',
    'accuracy_home_total_away',
    'accuracy_away_total_away'
]
FIELDS_FORECAST_TOTAL_AMOUNT = [
    'match_id',
    'teamHome_id',
    'teamAway_id',
    'forecast_total_amount',
    'target_total_amount',
    'forecast_probability_total_amount',
    'forecast_confidence_total_amount',
    'outcome_forecast_total_amount',
    'accuracy_home_total_amount',
    'accuracy_away_total_amount'
]
FIELDS_FORECAST_TOTAL_HOME_AMOUNT = [
    'match_id',
    'teamHome_id',
    'teamAway_id',
    'forecast_total_home_amount',
    'target_total_home_amount',
    'forecast_probability_total_home_amount',
    'forecast_confidence_total_home_amount',
    'outcome_forecast_total_home_amount',
    'accuracy_home_total_home_amount',
    'accuracy_away_total_away_amount'
]
FIELDS_FORECAST_TOTAL_AWAY_AMOUNT = [
    'match_id',
    'teamHome_id',
    'teamAway_id',
    'forecast_total_away_amount',
    'target_total_away_amount',
    'forecast_probability_total_away_amount',
    'forecast_confidence_total_away_amount',
    'outcome_forecast_total_away_amount',
    'accuracy_home_total_home_amount',
    'accuracy_away_total_away_amount'
]

FIELD_OUTCOME = [
    'outcome_forecast_win_draw_loss',
    'outcome_forecast_oz',
    'outcome_forecast_total',
    'outcome_forecast_total_home',
    'outcome_forecast_total_away',
    'outcome_forecast_total_amount',
    'outcome_forecast_total_home_amount',
    'outcome_forecast_total_away_amount'
]

OUTCOME_YES = [
    'п1', 'обе забьют - да', 'тб', 'ит1б', 'ит2б'
]

OUTCOME_NO = [
    'х', 'обе забьют - нет', 'тм', 'ит1м', 'ит2м'
]

OUTCOME_ORDER = [
    'п1', 'х', 'п2',
    'обе забьют - да', 'обе забьют - нет',
    'тб', 'тм', 'ТБ', 'ТМ',
    'ит1б', 'ит1м', 'ит2б', 'ит2м',
    'ИТ1Б', 'ИТ1М', 'ИТ2Б', 'ИТ2М'
]

STRING_FORECAST_REPORT = """
Дата матча: {data_game}, тур: {tour}
{sport} {championship} {country} 
{team1}-{team2} (ID: {match_id}) прогноз: {outcome}, вероятность: {probability} {confidence} {accuracy_home} {accuracy_away}
"""

STRING_OUTCAST_REPORT = """
Дата матча: {data_game}, тур: {tour}
{sport} {championship} {country} 
{team1}-{team2} (ID: {match_id}) ({goals_team1}:{goals_team2}) {ext} прогноз: {feature} {outcome} p={probability} c={confidence} ah={accuracy_home} aa={accuracy_away}
Комментарий: {comment}
"""

LOAD_PICKLE: bool = False
DIR_PICKLE: str = './pickle'
