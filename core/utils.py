# izhbet/core/utils.py
"""
Утилиты для обработки данных.
"""
import os
import json
import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import List, Dict, Tuple, Any
import pickle
import logging
from sklearn.preprocessing import (
    StandardScaler, MinMaxScaler, RobustScaler, PowerTransformer,
    QuantileTransformer
)
import datetime as dt

from config import Session_pool
from db.models import Standing, Feature
from core.constants import (
    STRATEGY_RECORD, NOT_IN_FEATURE, TARGET_FIELDS,
    SIZE_TOTAL, SIZE_ITOTAL, SPR_SPORTS, SPR_SPORTS_RU,
    ATTR_AVERAGE, PREFIX_AVERAGE, ATTR_NOT_AVERAGE,
    FIELDS_FORECAST_WIN_DRAW_LOSS, FIELDS_FORECAST_OZ,
    FIELDS_FORECAST_TOTAL, FIELDS_FORECAST_TOTAL_HOME,
    FIELDS_FORECAST_TOTAL_AWAY, FIELDS_RENAME_FILTER,
    FIELDS_FORECAST_TOTAL_AMOUNT, FIELDS_FORECAST_TOTAL_HOME_AMOUNT,
    FIELDS_FORECAST_TOTAL_AWAY_AMOUNT, OUTCOME_YES, OUTCOME_NO,
    FIELD_OUTCOME, SPR_SPORTS, FORECAST_TO_NUMERIC, FORECAST_TO_TYPE,
    DROP_FIELD_EMBEDDING, DROP_FIELD_BLOWOUTS,
    CATEGORICAL_VARIABLE_P1, CATEGORICAL_VARIABLE_X, CATEGORICAL_VARIABLE_P2,
    CATEGORICAL_VARIABLE_OZY, CATEGORICAL_VARIABLE_OZN,
    CATEGORICAL_VARIABLE_TB, CATEGORICAL_VARIABLE_TM,
    CATEGORICAL_VARIABLE_TBH, CATEGORICAL_VARIABLE_TMH,
    CATEGORICAL_VARIABLE_TBG, CATEGORICAL_VARIABLE_TMG,
    CATEGORICAL_VARIABLE_GYH, CATEGORICAL_VARIABLE_GNH,
    CATEGORICAL_VARIABLE_GYG, CATEGORICAL_VARIABLE_GNG,
    # One-Hot encoding константы
    WIN_DRAW_LOSS_ONEHOT, BOTH_SCORE_ONEHOT, TOTAL_ONEHOT,
    HOME_TOTAL_ONEHOT, AWAY_TOTAL_ONEHOT, HOME_GOAL_ONEHOT, AWAY_GOAL_ONEHOT
)
from db.queries.feature import get_match_in_feature_all
from db.queries.match import get_match_id_pool
from db.queries.statistic import get_statistic_team_id
from db.queries.prediction import (
    get_match_in_prediction_all, get_prediction_matchs
)
from core.constants import DIR_PICKLE, LOAD_PICKLE
from db.models import Match, Prediction, Feature
from core.target_utils import create_target_from_match_result
from db.queries.match import get_match_tournament_id
from db.queries.prediction import get_prediction_matchs
from db.queries.feature import get_match_in_feature_all
from db.queries.prediction import get_match_in_prediction_all
logger = logging.getLogger(__name__)


def save_pickle(file_save, obj):
    """
    Сохраняет объект в pickle-файл.
    """
    try:
        with open(file_save, 'wb') as f:
            pickle.dump(obj, f)
        return True
    except Exception as e:
        logger.error(
            msg=f'Ошибка сохранения pickle файла: '
            f'{file_save}. Ошибка: {e}',
            exc_info=True
        )
        return False

def load_pickle(file_load):
    """
    Загружает объект из pickle-файла.
    """
    if not LOAD_PICKLE:
        return None

    if not os.path.isfile(file_load):
        return None

    try:
        with open(file_load, 'rb') as f:
            data = pickle.load(f)
            return data
    except (pickle.PickleError, EOFError, TypeError) as e:
        logger.error(
            msg=f'Ошибка десериализации pickle: {file_load}. '
                f'Ошибка: {e}',
            exc_info=True
        )
        return None
    except Exception as e:
        logger.error(
            f'Неизвестная ошибка при загрузке файла: '
            f'{file_load}. Ошибка: {e}',
            exc_info=True
        )
        return None


def convert_standing(standings, team_id):
    standing_record = Standing()
    for strategy in STRATEGY_RECORD.keys():
        try:
            attr = standings[strategy][team_id]
            for key, value in attr.items():

                if key == 'team':

                    standing_record.sport_id = value.sport_id
                    standing_record.country_id = value.country_id
                    standing_record.team_id = value.id
                    try:
                        standing_record.tournament_id = (
                            value.matchaway.championships.id
                        )
                    except AttributeError:
                        standing_record.tournament_id = (
                            value.matchhome.championships.id
                        )
                        logger.error(
                            f'Не удалось определить championships '
                            f'для team_id={team_id}'
                        )

                elif key in ['gameData', 'match_id', 'team_id']:
                    setattr(standing_record, key, value)
                else:
                    name_attr = f'{STRATEGY_RECORD[strategy]}_{key}'
                    setattr(standing_record, name_attr, value)
        except KeyError:
            continue

    return standing_record

# Новая версия
def create_feature_vector_new(standings_team_home, standings_team_away, current_match_id=None):
    """
    Создает вектор признаков для матча на основе статистики домашней и гостевой команд.
    Вместо деления, использует конкатенацию и разность для сохранения максимальной информации.
    
    Args:
        standings_team_home: Статистика домашней команды
        standings_team_away: Статистика гостевой команды
        current_match_id: ID текущего обрабатываемого матча (если None, берется из standings)
    """
    not_in_vector = NOT_IN_FEATURE + TARGET_FIELDS  # Предполагается, что эти константы определены

    feature_vector_home = Feature()
    feature_vector_away = Feature()
    feature_vector_diff = Feature()
    feature_vector_ratio = Feature()

    # Получаем список всех атрибутов, которые будем использовать
    all_attr_names = [
        attr for attr in standings_team_home.__dict__.keys()
            if attr not in not_in_vector and not attr.startswith('_')
    ]

    for attr_name in all_attr_names:
        # Извлекаем значения для домашней и гостевой команды
        home_attr = get_attribute_with_default(standings_team_home, attr_name, default_value=0.0)
        away_attr = get_attribute_with_default(standings_team_away, attr_name, default_value=0.0)
        
        # Валидация значений
        if home_attr is None or away_attr is None:
            logger.warning(f"None значения в атрибуте {attr_name}: home={home_attr}, away={away_attr}")
            home_attr = 0.0 if home_attr is None else home_attr
            away_attr = 0.0 if away_attr is None else away_attr

        # 1. АБСОЛЮТНЫЕ ЗНАЧЕНИЯ (конкатенация)
        # Создаем новые имена атрибутов для вектора: {attr_name}_home, {attr_name}_away
        #setattr(feature_vector, f"{attr_name}_home", home_attr)
        #setattr(feature_vector, f"{attr_name}_away", away_attr)
        setattr(feature_vector_home, attr_name, home_attr)
        setattr(feature_vector_away, attr_name, away_attr)

        # 2. ОТНОСИТЕЛЬНЫЕ ЗНАЧЕНИЯ (разность и, опционально, нормализованное отношение)
        # Разность - устойчивая и симметричная операция
        diff_attr = home_attr - away_attr
        #setattr(feature_vector, f"{attr_name}_diff", diff_attr)
        setattr(feature_vector_diff, attr_name, diff_attr)

        # Опционально: Добавляем отношение, но с защитой от деления на ноль и ограничением
        # Это может быть полезно для некоторых признаков (например, average_scoring), но требует осторожности
        if isinstance(home_attr, (int, float)) and isinstance(away_attr, (int, float)):
            if away_attr > 0.1:  # Избегаем деления на очень маленькие числа
                ratio_attr = home_attr / away_attr
            elif home_attr > 0:
                ratio_attr = 10.0  # Большое значение для случая, когда away=0, home>0
            else:
                ratio_attr = 1.0   # Нейтральное значение когда оба = 0
            
            # Ограничиваем слишком большие значения для стабильности
            ratio_attr = max(0.01, min(100.0, ratio_attr))
        else:
            ratio_attr = 1.0  # Нейтральное значение для нечисловых типов
        
        setattr(feature_vector_ratio, attr_name, ratio_attr)

    # Добавляем новые поля на основе standings snapshots, если присутствуют
    # Например: позиция и очки как прямые признаки и их разница
    for extra_attr in ['general_position', 'general_points', 'general_goals_scored', 'general_goals_conceded']:
        h = get_attribute_with_default(standings_team_home, extra_attr, default_value=0.0)
        a = get_attribute_with_default(standings_team_away, extra_attr, default_value=0.0)
        setattr(feature_vector_home, extra_attr, h)
        setattr(feature_vector_away, extra_attr, a)
        setattr(feature_vector_diff, extra_attr, (h - a) if isinstance(h, (int, float)) and isinstance(a, (int, float)) else 0.0)
        # ratio для позиции инвертируем (меньше — лучше)
        if 'position' in extra_attr:
            denom = a if isinstance(a, (int, float)) and a > 0 else 1.0
            ratio_val = (a / denom) if denom else 1.0
        else:
            denom = a if isinstance(a, (int, float)) and a > 0.1 else 1.0
            ratio_val = h / denom if isinstance(h, (int, float)) else 1.0
        setattr(feature_vector_ratio, extra_attr, max(0.01, min(100.0, ratio_val)))

    # Возвращаем вектор, содержащий в 3 раза больше признаков, чем раньше,
    # но зато гораздо более информативный и устойчивый.
    feature_vector = {}

    # Используем переданный match_id или берем из standings (для обратной совместимости)
    match_id = current_match_id if current_match_id is not None else standings_team_home.match_id
    setattr(feature_vector_home, 'match_id', match_id)
    setattr(feature_vector_away, 'match_id', match_id)
    setattr(feature_vector_diff, 'match_id', match_id)
    setattr(feature_vector_ratio, 'match_id', match_id)

    feature_vector['home'] = feature_vector_home
    feature_vector['away'] = feature_vector_away
    feature_vector['diff'] = feature_vector_diff
    feature_vector['ratio'] = feature_vector_ratio

    return feature_vector

def create_feature_vector(standings_team_home, standings_team_away):
    not_in_vector = NOT_IN_FEATURE + TARGET_FIELDS
    feature_vector = Feature()
    for attr_name in standings_team_home.__dict__.keys():
        if attr_name in not_in_vector:
           continue
        splits_name_attr = attr_name.split('_')
        if len(splits_name_attr) == 2:
            prefix_name_attr = splits_name_attr[0]
            postfix_name_attr = splits_name_attr[1]
        elif len(splits_name_attr) == 3:
            if splits_name_attr[1] in ['strong', 'medium', 'weak']:
                prefix_name_attr = (
                    f'{splits_name_attr[0]}_{splits_name_attr[1]}'
                )
                postfix_name_attr = splits_name_attr[2]
            else:
                prefix_name_attr = splits_name_attr[0]
                postfix_name_attr = (
                    f'{splits_name_attr[1]}_{splits_name_attr[2]}'
                )
        else:
            prefix_name_attr = (
                f'{splits_name_attr[0]}_{splits_name_attr[1]}'
            )
            postfix_name_attr = (
                f'{splits_name_attr[2]}_{splits_name_attr[3]}'
            )

        if postfix_name_attr in ATTR_NOT_AVERAGE:
            continue

        if prefix_name_attr in PREFIX_AVERAGE:
            home_attr = get_attribute_with_default(
                standings_team_home,
                attr_name,
                default_value=0.
            )
            away_attr = get_attribute_with_default(
                standings_team_away,
                attr_name,
                default_value=0.
            )

            if postfix_name_attr in ATTR_AVERAGE:
                home_games_played = get_attribute_with_default(
                    standings_team_home,
                    attr_name=f'{prefix_name_attr}_games_played',
                    default_value=0.
                )
                away_games_played = get_attribute_with_default(
                    standings_team_away,
                    attr_name=f'{prefix_name_attr}_games_played',
                    default_value=0.
                )
                aver_home = (
                    home_attr / home_games_played
                        if home_games_played > 0 else 0.
                )
                aver_away = (
                    away_attr / away_games_played
                        if away_games_played > 0 else 0.
                )
                # Улучшенная обработка деления на ноль с логированием
                if aver_away > 0:
                    feature_attr = aver_home / aver_away
                elif aver_home > 0:
                    feature_attr = 10.0  # Большое значение для случая, когда away=0, home>0
                else:
                    feature_attr = 1.0   # Нейтральное значение когда оба = 0
                
                # Ограничиваем экстремальные значения для стабильности
                feature_attr = max(0.01, min(100.0, feature_attr))
                setattr(feature_vector, attr_name, feature_attr)
            else:
                # Улучшенная обработка деления на ноль
                if away_attr > 0:
                    feature_attr = home_attr / away_attr
                elif home_attr > 0:
                    feature_attr = 10.0  # Большое значение для случая, когда away=0, home>0
                else:
                    feature_attr = 1.0   # Нейтральное значение когда оба = 0
                
                # Ограничиваем экстремальные значения для стабильности
                feature_attr = max(0.01, min(100.0, feature_attr))
                setattr(feature_vector, attr_name, feature_attr)

    return feature_vector


# Категории НС для: П1/Х/П2 => 1 - Победа хозяева, 0 - Ничья, -1 - Победа гости
# Категории НС для: ОЗ, ТБ ИТ, 1З, 2З => 1 - Произошло событие, 0 - Не произошло
def create_feature_attr_onehot(
    feature_record,
    sport_id,
    goal_home,
    goal_away,
    type_outcome
):
    """
    Создает One-Hot encoded атрибуты фичи на основе результата матча.
    
    Args:
        feature_record: Объект фичи для заполнения
        sport_id: ID спорта
        goal_home: Голы домашней команды
        goal_away: Голы гостевой команды
        type_outcome: Тип исхода матча
    """
    sport = SPR_SPORTS[sport_id]
    
    # Улучшенная обработка пустых значений и типов данных
    def safe_int(value):
        """Безопасное преобразование в int с обработкой пустых значений"""
        if value is None or value == '' or value == 'None':
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
    
    # Преобразуем в числа с безопасной обработкой
    goal_home_int = safe_int(goal_home)
    goal_away_int = safe_int(goal_away)
    
    # Логируем проблемные данные
    if goal_home_int is None or goal_away_int is None:
        logger.warning(f"Проблемные данные голов: home={goal_home}, away={goal_away}, type_outcome={type_outcome}")
    
    # Проверяем корректность данных
    if goal_home_int is not None and goal_away_int is not None:
        if goal_home_int < 0 or goal_away_int < 0:
            logger.warning(f"Отрицательные голы: home={goal_home_int}, away={goal_away_int}")
            return feature_record
    
    # Target поля теперь обрабатываются в отдельной таблице targets
    # Создаем целевую переменную через target_utils
    from core.target_utils import create_target_from_match_result
    result = create_target_from_match_result(feature_record.match_id, goal_home_int, goal_away_int)
    if not result:
        logger.warning(f"Не удалось создать target для матча {feature_record.match_id} (голы: {goal_home_int}:{goal_away_int})")

    return feature_record


def create_feature_attr(
    feature_record,
    sport_id,
    goal_home,
    goal_away,
    type_outcome
):
    sport = SPR_SPORTS[sport_id]
    
    # Улучшенная обработка пустых значений и типов данных
    def safe_int(value):
        """Безопасное преобразование в int с обработкой пустых значений"""
        if value is None or value == '' or value == 'None':
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
    
    # Преобразуем в числа с безопасной обработкой
    goal_home_int = safe_int(goal_home)
    goal_away_int = safe_int(goal_away)
    
    # Логируем проблемные данные
    if goal_home_int is None or goal_away_int is None:
        logger.warning(f"Проблемные данные голов: home={goal_home}, away={goal_away}, type_outcome={type_outcome}")
    
    # Проверяем корректность данных
    if goal_home_int is not None and goal_away_int is not None:
        if goal_home_int < 0 or goal_away_int < 0:
            logger.warning(f"Отрицательные голы: home={goal_home_int}, away={goal_away_int}")
            return feature_record
    
    # Обработка результата матча (win/draw/loss)
    if goal_home_int is not None and goal_away_int is not None:
        if goal_home_int > goal_away_int:
            wdl = CATEGORICAL_VARIABLE_P1
        elif goal_home_int < goal_away_int:
            wdl = CATEGORICAL_VARIABLE_P2
        else:  # goal_home_int == goal_away_int
            wdl = CATEGORICAL_VARIABLE_X
    else:
        wdl = None

    
    # Target поля теперь обрабатываются в отдельной таблице targets
    # Создаем целевую переменную через target_utils
    result = create_target_from_match_result(feature_record.match_id, goal_home_int, goal_away_int)
    if not result:
        logger.warning(f"Не удалось создать target для матча {feature_record.match_id} (голы: {goal_home_int}:{goal_away_int})")

    return feature_record


def get_attribute_with_default(obj, attr_name, default_value):
    value = getattr(obj, attr_name, None)  # Получаем значение атрибута
    return value if value is not None else default_value


def normalize_features(feature_vector):
    """
    Нормализация признаков для улучшения качества моделей.
    
    Args:
        feature_vector: Словарь с признаками (home, away, diff, ratio)
    
    Returns:
        Нормализованный словарь признаков
    """
    def safe_normalize(value, min_val=0.01, max_val=100.0):
        """Безопасная нормализация значения"""
        if value is None:
            return 0.0
        try:
            # Ограничиваем экстремальные значения
            normalized = max(min_val, min(max_val, float(value)))
            # Логарифмическая нормализация для уменьшения влияния выбросов
            if normalized > 1:
                return np.log1p(normalized - 1) + 1
            return normalized
        except (ValueError, TypeError):
            return 0.0
    
    normalized_vector = {}
    
    for key, features in feature_vector.items():
        if hasattr(features, '__dict__'):
            normalized_features = type(features)()
            for attr_name in features.__dict__:
                if not attr_name.startswith('_'):
                    value = getattr(features, attr_name)
                    normalized_value = safe_normalize(value)
                    setattr(normalized_features, attr_name, normalized_value)
            normalized_vector[key] = normalized_features
        else:
            normalized_vector[key] = features
    
    return normalized_vector


def validate_features(feature_vector):
    """
    Валидация признаков для выявления проблемных данных.
    
    Args:
        feature_vector: Словарь с признаками
    
    Returns:
        Словарь с результатами валидации
    """
    validation_results = {
        'is_valid': True,
        'warnings': [],
        'errors': []
    }
    
    for key, features in feature_vector.items():
        if hasattr(features, '__dict__'):
            for attr_name in features.__dict__:
                if not attr_name.startswith('_'):
                    value = getattr(features, attr_name)
                    
                    # Проверка на NaN
                    if pd.isna(value) if hasattr(pd, 'isna') else (value != value):
                        validation_results['errors'].append(f"{key}.{attr_name}: NaN value")
                        validation_results['is_valid'] = False
                    
                    # Проверка на бесконечность
                    elif np.isinf(value) if hasattr(np, 'isinf') else False:
                        validation_results['errors'].append(f"{key}.{attr_name}: Infinite value")
                        validation_results['is_valid'] = False
                    
                    # Проверка на экстремальные значения
                    elif isinstance(value, (int, float)) and abs(value) > 1000:
                        validation_results['warnings'].append(f"{key}.{attr_name}: Extreme value {value}")
    
    return validation_results


def analyze_feature_quality(features_dict):
    """
    Анализ качества созданных фичей для выявления проблем.
    
    Args:
        features_dict: Словарь с фичами по матчам
    
    Returns:
        Словарь с анализом качества
    """
    analysis = {
        'total_matches': len(features_dict),
        'matches_with_issues': 0,
        'common_issues': {},
        'feature_distribution': {}
    }
    
    for match_id, feature_vector in features_dict.items():
        has_issues = False
        
        for vector_type, features in feature_vector.items():
            if hasattr(features, '__dict__'):
                for attr_name in features.__dict__:
                    if not attr_name.startswith('_'):
                        value = getattr(features, attr_name)
                        
                        # Подсчет проблем
                        if pd.isna(value) if hasattr(pd, 'isna') else (value != value):
                            issue_key = f"{vector_type}.{attr_name}_nan"
                            analysis['common_issues'][issue_key] = analysis['common_issues'].get(issue_key, 0) + 1
                            has_issues = True
                        
                        elif np.isinf(value) if hasattr(np, 'isinf') else False:
                            issue_key = f"{vector_type}.{attr_name}_inf"
                            analysis['common_issues'][issue_key] = analysis['common_issues'].get(issue_key, 0) + 1
                            has_issues = True
                        
                        # Анализ распределения значений
                        elif isinstance(value, (int, float)):
                            dist_key = f"{vector_type}.{attr_name}"
                            if dist_key not in analysis['feature_distribution']:
                                analysis['feature_distribution'][dist_key] = []
                            analysis['feature_distribution'][dist_key].append(value)
        
        if has_issues:
            analysis['matches_with_issues'] += 1
    
    # Статистика по распределению
    for feature_name, values in analysis['feature_distribution'].items():
        if values:
            analysis['feature_distribution'][feature_name] = {
                'count': len(values),
                'mean': np.mean(values),
                'std': np.std(values),
                'min': np.min(values),
                'max': np.max(values),
                'zeros': sum(1 for v in values if v == 0),
                'negative': sum(1 for v in values if v < 0)
            }
    
    return analysis


# def convert_digit_to_dataframe(digit_series):
#
#     def convert_digit_to_list(digit):
#         mapping = {
#             1: [0, 1], #, 0],
#             0: [0, 0], #, 0],
#             2: [1, 0] #, 1]
#         }
#         return mapping.get(digit, [None, None]) # , None])
#
#     # Применяем функцию к серии и создаем DataFrame
#     result_lists = digit_series.apply(convert_digit_to_list)
#     result_df = pd.DataFrame(result_lists.tolist(), columns=['Col1', 'Col2']) #, 'Col3'])
#
#     return result_df


@dataclass
class FeatureConfig:
    features: List[str]
    target: str
    task_type: str
    normalization_method: str


def create_feature_config(features: List[str]) -> Dict[str, FeatureConfig]:
    """
    Создает конфигурацию фичей для нейронных сетей с One-Hot encoding.
    
    :param features: Список названий фичей
    :return: Словарь конфигураций для каждой целевой переменной
    """
    return {
        # One-Hot encoded категориальные переменные
        'win_draw_loss_home_win': FeatureConfig(
            features=features,
            target='target_win_draw_loss_home_win',
            task_type='classification',
            normalization_method='robust'
        ),
        'win_draw_loss_draw': FeatureConfig(
            features=features,
            target='target_win_draw_loss_draw',
            task_type='classification',
            normalization_method='robust'
        ),
        'win_draw_loss_away_win': FeatureConfig(
            features=features,
            target='target_win_draw_loss_away_win',
            task_type='classification',
            normalization_method='robust'
        ),
        'oz_both_score': FeatureConfig(
            features=features,
            target='target_oz_both_score',
            task_type='classification',
            normalization_method='robust'
        ),
        'oz_not_both_score': FeatureConfig(
            features=features,
            target='target_oz_not_both_score',
            task_type='classification',
            normalization_method='robust'
        ),
        'goal_home_scores': FeatureConfig(
            features=features,
            target='target_goal_home_yes',
            task_type='classification',
            normalization_method='robust'
        ),
        'goal_home_no_score': FeatureConfig(
            features=features,
            target='target_goal_home_no',
            task_type='classification',
            normalization_method='robust'
        ),
        'goal_away_scores': FeatureConfig(
            features=features,
            target='target_goal_away_yes',
            task_type='classification',
            normalization_method='robust'
        ),
        'goal_away_no_score': FeatureConfig(
            features=features,
            target='target_goal_away_no',
            task_type='classification',
            normalization_method='robust'
        ),
        'total_over': FeatureConfig(
            features=features,
            target='target_total_over',
            task_type='classification',
            normalization_method='robust'
        ),
        'total_under': FeatureConfig(
            features=features,
            target='target_total_under',
            task_type='classification',
            normalization_method='robust'
        ),
        'total_home_over': FeatureConfig(
            features=features,
            target='target_total_home_over',
            task_type='classification',
            normalization_method='robust'
        ),
        'total_home_under': FeatureConfig(
            features=features,
            target='target_total_home_under',
            task_type='classification',
            normalization_method='robust'
        ),
        'total_away_over': FeatureConfig(
            features=features,
            target='target_total_away_over',
            task_type='classification',
            normalization_method='robust'
        ),
        'total_away_under': FeatureConfig(
            features=features,
            target='target_total_away_under',
            task_type='classification',
            normalization_method='robust'
        ),
        # Регрессионные переменные (количество голов)
        'total_amount': FeatureConfig(
            features=features,
            target='target_total_amount',
            task_type='regression',
            normalization_method='robust'
        ),
        'total_home_amount': FeatureConfig(
            features=features,
            target='target_total_home_amount',
            task_type='regression',
            normalization_method='robust'
        ),
        'total_away_amount': FeatureConfig(
            features=features,
            target='target_total_away_amount',
            task_type='regression',
            normalization_method='robust'
        )
    }


def get_scalers(method: str):
    """
    Получение скалера по названию метода.

    Args:
        method: Название метода нормализации

    Returns:
        Объект скалера
    """
    scalers = {
        'standard': StandardScaler(),
        'minmax': MinMaxScaler(),
        'robust': RobustScaler(),
        'power': PowerTransformer(
            method='yeo-johnson'
        ),
        'quantile': QuantileTransformer(
            n_quantiles=100,
            output_distribution='normal'
        )
}
    return scalers.get(method, StandardScaler())


def get_feature_correct(row, forecast_col, feature_col):
    """Получает корректность прогноза."""
    match_id = row['match_id']
    match = get_match_id_pool(match_id=match_id)
    total = SIZE_TOTAL[SPR_SPORTS[match.sport_id]]
    itotal = SIZE_ITOTAL[SPR_SPORTS[match.sport_id]]

    if feature_col == 'target_total_amount':
        forecast_value = 0
        correct = 0
        if row['forecast_total_amount'] > total:
            forecast_value = 1
        if row['feature_total_amount'] > total:
            correct = 1
    elif feature_col == 'target_total_home_amount':
        forecast_value = 0
        correct = 0
        if row['forecast_total_home_amount'] > itotal:
            forecast_value = 1
        if row['feature_total_home_amount'] > itotal:
            correct = 1
    elif feature_col == 'target_total_away_amount':
        forecast_value = 0
        correct = 0
        if row['forecast_total_away_amount'] > itotal:
            forecast_value = 1
        if row['feature_total_away_amount'] > itotal:
            correct = 1
    else:
        correct = int(int(row[forecast_col]) == int(row[feature_col]))
        forecast_value = row[forecast_col]

    return correct, forecast_value


def get_outcome_original(row, outcome_forecast_col, feature_col):
    """Получает исходный прогноз."""
    match_id = row['match_id']
    match = get_match_id_pool(match_id=match_id)
    total = SIZE_TOTAL[SPR_SPORTS[match.sport_id]]
    itotal = SIZE_ITOTAL[SPR_SPORTS[match.sport_id]]

    outcome_value = row[outcome_forecast_col]

    if feature_col == 'target_total_amount':
        outcome_value = 'ТМ'
        if row['forecast_total_amount'] >= total:
            outcome_value = 'ТБ'

    elif feature_col == 'target_total_home_amount':
        outcome_value = 'ИТ1М'
        if row['forecast_total_home_amount'] >= itotal:
            outcome_value = 'ИТ1Б'

    elif feature_col == 'target_total_away_amount':
        outcome_value = 'ИТ2М'
        if row['forecast_total_away_amount'] >= itotal:
            outcome_value = 'ИТ2Б'

    return outcome_value


def create_forecast_report(template, row):
    match = get_match_id_pool(match_id=row['match_id'])
    template = template.format(
        data_game=match.gameData,
        tour=match.tour,
        sport=SPR_SPORTS_RU[match.sports.sportName],
        country=match.countrys.countryName,
        championship=match.championships.championshipName,
        team1=match.teamhomes.teamName,
        team2=match.teamaways.teamName,
        outcome=row['outcome'],
        probability=row['probability'],
        confidence=row['confidence'],
        accuracy_home=row['accuracy_home'],
        accuracy_away=row['accuracy_away']
    )
    return template


def create_outcome_report(template, row):
    match = get_match_id_pool(match_id=row['match_id'])
    total = SIZE_TOTAL[SPR_SPORTS[match.sport_id]]
    itotal = SIZE_ITOTAL[SPR_SPORTS[match.sport_id]]

    feature_report = ''

    if row.feature is not None:
        try:
            if row['feature'] == CATEGORICAL_VARIABLE_P1 and (row['outcome'] in OUTCOME_YES):
                feature_report = '✅'
            elif row['feature'] == CATEGORICAL_VARIABLE_X and (row['outcome'] in OUTCOME_NO):
                feature_report = '✅'
            elif row['feature'] == CATEGORICAL_VARIABLE_P2 and (row['outcome'] in ['п2']):
                feature_report = '✅'
            else:
                if row['feature'] >= total and row['outcome'] == 'ТБ':
                    feature_report = '✅'
                if row['feature'] >= itotal and (
                    row['outcome'] == 'ИТ1Б' or row['outcome'] == 'ИТ2Б'
                ):
                    feature_report = '✅'
                if row['feature'] < total and row['outcome'] == 'ТМ':
                    feature_report = '✅'
                if row['feature'] < itotal and (
                    row['outcome'] == 'ИТ1М' or row['outcome'] == 'ИТ2М'
                ):
                    feature_report = '✅'

            if feature_report == '' and row['feature'] is not None:
                feature_report = '❌'

        except (TypeError, ValueError) as err:
            logger.error(
                f'ERROR в модуле -> create_outcome_report: {err}'
            )

    if match.numOfHeadsHome is None:
        numOfHeadsHome = ''
        numOfHeadsAway = ''
        gameComment = ''

    else:
        numOfHeadsHome = match.numOfHeadsHome
        numOfHeadsAway = match.numOfHeadsAway
        gameComment = match.gameComment

    template = template.format(
        data_game=match.gameData,
        tour=match.tour,
        sport=SPR_SPORTS[match.sport_id],
        country=match.countrys.countryName,
        championship=match.championships.championshipName,
        team1=match.teamhomes.teamName,
        team2=match.teamaways.teamName,
        goals_team1=numOfHeadsHome,
        goals_team2=numOfHeadsAway,
        ext=match.typeOutcome if match.typeOutcome is not None else '',
        feature=feature_report,
        outcome=row['outcome'],
        comment=gameComment,
        probability = row['probability'],
        confidence = row['confidence'],
        accuracy_home = row['accuracy_home'],
        accuracy_away=row['accuracy_away'],
    )

    feature = 0
    if feature_report == '✅':
        feature = 1

    return template, feature


def filter_forecats(db_session, df, filters):
    if not df.empty:
        # Применение фильтров
        for outcome in FIELD_OUTCOME:
            name_outcome = outcome.replace('outcome_forecast', '')
            if name_outcome != '_total_away_amount':
                accuracy_home = f'accuracy_home{name_outcome}'
                df[accuracy_home] = 0.
            if name_outcome != '_total_home_amount':
                accuracy_away = f'accuracy_away{name_outcome}'
                df[accuracy_away] = 0.

        for index, row in df.iterrows():
            for outcome in FIELD_OUTCOME:

                if row[outcome] == '':
                    continue

                name_outcome = outcome.replace('outcome_forecast', '')

                vid = 'nn' #'original'
                if vid == 'original':
                    forecast = row[outcome]
                else:
                    forecast = FORECAST_TO_NUMERIC[row[outcome]]

                forecast_type = FORECAST_TO_TYPE[row[outcome]]

                statistic_team_home = get_statistic_team_id(
                    db_session=db_session,
                    team_id=row['teamHome_id'],
                    forecast_type=forecast_type,
                    forecast_vid=vid,
                    forecast=forecast
                )

                statistic_team_away = get_statistic_team_id(
                    db_session=db_session,
                    team_id=row['teamAway_id'],
                    forecast_type=forecast_type,
                    forecast_vid=vid,
                    forecast=forecast
                )

                if statistic_team_home is not None:
                    if name_outcome != '_total_away_amount':
                        accuracy_home = f'accuracy_home{name_outcome}'

                        df.loc[index, accuracy_home] = (
                            float(statistic_team_home.accuracy)
                        )

                if statistic_team_away is not None:
                    if name_outcome != '_total_home_amount':
                        accuracy_away = f'accuracy_away{name_outcome}'

                        df.loc[index, accuracy_away] = (
                            float(statistic_team_away.accuracy)
                        )

        outcome_home_df = df.copy()
        for f in filters['outcome_home']:
            outcome_home_df = f.apply(
                outcome_home_df[FIELDS_FORECAST_WIN_DRAW_LOSS]
            )
        outcome_home_df.rename(columns=FIELDS_RENAME_FILTER, inplace=True)

        outcome_draw_df = df.copy()
        for f in filters['outcome_draw']:
            outcome_draw_df = f.apply(
                outcome_draw_df[FIELDS_FORECAST_WIN_DRAW_LOSS]
            )
        outcome_draw_df.rename(columns=FIELDS_RENAME_FILTER, inplace=True)

        outcome_away_df = df.copy()
        for f in filters['outcome_away']:
            outcome_away_df = f.apply(
                outcome_away_df[FIELDS_FORECAST_WIN_DRAW_LOSS]
            )
        outcome_away_df.rename(columns=FIELDS_RENAME_FILTER, inplace=True)

        oz_yes_df = df.copy()
        for f in filters['oz_yes']:
            oz_yes_df = f.apply(
                oz_yes_df[FIELDS_FORECAST_OZ]
            )
        oz_yes_df.rename(columns=FIELDS_RENAME_FILTER, inplace=True)

        oz_no_df = df.copy()
        for f in filters['oz_no']:
            oz_no_df = f.apply(
                oz_no_df[FIELDS_FORECAST_OZ]
            )
        oz_no_df.rename(columns=FIELDS_RENAME_FILTER, inplace=True)

        total_b_df = df.copy()
        for f in filters['total_b']:
            total_b_df = f.apply(
                total_b_df[FIELDS_FORECAST_TOTAL]
            )
        total_b_df.rename(columns=FIELDS_RENAME_FILTER, inplace=True)

        total_m_df = df.copy()
        for f in filters['total_m']:
            total_m_df = f.apply(
                total_m_df[FIELDS_FORECAST_TOTAL]
            )
        total_m_df.rename(columns=FIELDS_RENAME_FILTER, inplace=True)

        total_home_b_df = df.copy()
        for f in filters['total_home_b']:
            total_home_b_df = f.apply(
                total_home_b_df[FIELDS_FORECAST_TOTAL_HOME]
            )
        total_home_b_df.rename(columns=FIELDS_RENAME_FILTER, inplace=True)

        total_home_m_df = df.copy()
        for f in filters['total_home_m']:
            total_home_m_df = f.apply(
                total_home_m_df[FIELDS_FORECAST_TOTAL_HOME]
            )
        total_home_m_df.rename(columns=FIELDS_RENAME_FILTER, inplace=True)

        total_away_b_df = df.copy()
        for f in filters['total_away_b']:
            total_away_b_df = f.apply(
                total_away_b_df[FIELDS_FORECAST_TOTAL_AWAY]
            )
        total_away_b_df.rename(columns=FIELDS_RENAME_FILTER, inplace=True)

        total_away_m_df = df.copy()
        for f in filters['total_away_m']:
            total_away_m_df = f.apply(
                total_away_m_df[FIELDS_FORECAST_TOTAL_AWAY]
            )
        total_away_m_df.rename(columns=FIELDS_RENAME_FILTER, inplace=True)

        total_amount_b_df = df.copy()
        for f in filters['total_amount_b']:
            total_amount_b_df = f.apply(
                total_amount_b_df[FIELDS_FORECAST_TOTAL_AMOUNT]
            )
        total_amount_b_df.rename(columns=FIELDS_RENAME_FILTER, inplace=True)

        total_home_amount_b_df = df.copy()
        for f in filters['total_home_amount_b']:
            total_home_amount_b_df = f.apply(
                total_home_amount_b_df[FIELDS_FORECAST_TOTAL_HOME_AMOUNT]
            )
        total_home_amount_b_df.rename(columns=FIELDS_RENAME_FILTER, inplace=True)

        total_away_amount_b_df = df.copy()
        for f in filters['total_away_amount_b']:
            total_away_amount_b_df = f.apply(
                total_away_amount_b_df[FIELDS_FORECAST_TOTAL_AWAY_AMOUNT]
            )
        total_away_amount_b_df.rename(columns=FIELDS_RENAME_FILTER, inplace=True)

        total_amount_m_df = df.copy()
        for f in filters['total_amount_m']:
            total_amount_m_df = f.apply(
                total_amount_m_df[FIELDS_FORECAST_TOTAL_AMOUNT]
            )
        total_amount_m_df.rename(columns=FIELDS_RENAME_FILTER, inplace=True)

        total_home_amount_m_df = df.copy()
        for f in filters['total_home_amount_m']:
            total_home_amount_m_df = f.apply(
                total_home_amount_m_df[FIELDS_FORECAST_TOTAL_HOME_AMOUNT]
            )
        total_home_amount_m_df.rename(columns=FIELDS_RENAME_FILTER, inplace=True)

        total_away_amount_m_df = df.copy()
        for f in filters['total_away_amount_m']:
            total_away_amount_m_df = f.apply(
                total_away_amount_m_df[FIELDS_FORECAST_TOTAL_AWAY_AMOUNT]
            )
        total_away_amount_m_df.rename(columns=FIELDS_RENAME_FILTER, inplace=True)

        # Формируем список только непустых DataFrame
        valid_dfs = [
            df for df in
            [
                outcome_home_df,
                outcome_draw_df,
                outcome_away_df,
                oz_yes_df,
                oz_no_df,
                total_b_df,
                total_m_df,
                total_home_b_df,
                total_home_m_df,
                total_away_b_df,
                total_away_m_df,
                total_amount_b_df,
                total_amount_m_df,
                total_home_amount_b_df,
                total_home_amount_m_df,
                total_away_amount_b_df,
                total_away_amount_m_df
] if not df.empty
        ]
        if valid_dfs:
            combined_df = pd.concat(valid_dfs).drop_duplicates()
            combined_df.rename(columns=FIELDS_RENAME_FILTER, inplace=True)
        else:
            combined_df = pd.DataFrame()
    else:
        combined_df = pd.DataFrame()

    return combined_df


def get_tournament_ids():
    """
    Получает список всех tournament_id из базы данных.
    """
    from db.queries.match import get_match_modeling

    with Session_pool() as db_session:
        # Получаем все матчи для извлечения уникальных tournament_id
        matches_all = get_match_modeling(create_model=False)

        if not matches_all:
            logger.warning('Нет данных матчей для получения tournament_id')
            return []

        # Создаем DataFrame и извлекаем уникальные tournament_id
        df = pd.DataFrame([match.as_dict() for match in matches_all])
        tournament_ids = df['tournament_id'].drop_duplicates().tolist()

        logger.info(f'Найдено {len(tournament_ids)} чемпионатов')
        return tournament_ids


def create_df_matches_by_tournament(tournament_id: int = None):
    """
    Создает DataFrame с данными матчей для конкретного чемпионата.

    Args:
        tournament_id: ID чемпионата. Если None, загружает все данные.
    """
    with Session_pool() as db_session:
        # Получаем match_ids для конкретного чемпионата
        if tournament_id:
            logger.info(
                f'Загрузка данных для чемпионата {tournament_id}.'
            )
            if LOAD_PICKLE:
                matches_played = load_pickle(
                    f'{DIR_PICKLE}/forecast/matches_played_'
                    f'{tournament_id}.pickl'
                )
                if matches_played is None:
                    matches_played = get_match_tournament_id(
                        tournament_id,
                        played_only=True
                    )
                    save_pickle(
                        f'{DIR_PICKLE}/forecast/matches_played_'
                        f'{tournament_id}.pickl',
                        matches_played
                    )

                matches_all = load_pickle(
                    f'{DIR_PICKLE}/forecast/matches_all_'
                    f'{tournament_id}.pickl'
                )
                if matches_all is None:
                    matches_all = get_match_tournament_id(
                        tournament_id,
                        played_only=False
                    )
                    save_pickle(
                        f'{DIR_PICKLE}/forecast/matches_all_'
                        f'{tournament_id}.pickl',
                        matches_all
                    )
            else:
                matches_played = get_match_tournament_id(
                    tournament_id,
                    played_only=True
                )
                matches_all = get_match_tournament_id(
                    tournament_id,
                    played_only=False
                )
                pass

            match_ids_played = [m.id for m in matches_played]
            match_ids_all = [m.id for m in matches_all]
            pass
        else:
            if LOAD_PICKLE:
                # Получаем данные для прошедших матчей
                match_ids_played = load_pickle(
                    f'{DIR_PICKLE}/forecast/matches_ids_played_'
                    f'{tournament_id}.pickl'
                )
                if match_ids_played is None:
                    match_ids_played = get_prediction_matchs(all=False)
                    save_pickle(
                        f'{DIR_PICKLE}/matches_ids_played.pickl',
                        match_ids_played
                    )

                # Получаем данные для всех матчей
                match_ids_all = load_pickle(
                    f'{DIR_PICKLE}/forecast/matches_ids_all_'
                    f'{tournament_id}.pickl'
                )
                if match_ids_all is None:
                    match_ids_all = get_prediction_matchs(all=True)
                    save_pickle(
                        f'{DIR_PICKLE}/forecast/matches_ids_all_'
                        f'{tournament_id}.pickl',
                        match_ids_all
                    )
            else:
                match_ids_played = get_prediction_matchs(all=False)
                match_ids_all = get_prediction_matchs(all=True)
                pass

        if LOAD_PICKLE:
            # Получаем данные prediction и feature
            predict_played = load_pickle(
                f'{DIR_PICKLE}/forecast/predict_played.pickl'
            )
            if predict_played is None:
                predict_played = get_match_in_prediction_all(
                    db_session=db_session,
                    match_ids=match_ids_played
                )
                save_pickle(
                    f'{DIR_PICKLE}/forecast/predict_played.pickl',
                    predict_played
                )

            feature_played = load_pickle(
                f'{DIR_PICKLE}/forecast/feature_played.pickl'
            )
            if feature_played is None:
                feature_played = get_match_in_feature_all(
                    db_session=db_session,
                    match_ids=match_ids_played
                )
                save_pickle(
                    f'{DIR_PICKLE}/forecast/feature_played.pickl',
                    feature_played
                )

            predict_all = load_pickle(
                f'{DIR_PICKLE}/forecast/predict_all.pickl'
            )
            if predict_all is None:
                predict_all = get_match_in_prediction_all(
                    db_session=db_session,
                    match_ids=match_ids_all
                )
                save_pickle(
                    f'{DIR_PICKLE}/forecast/predict_all.pickl',
                    predict_all
                )

            feature_all = load_pickle(
                f'{DIR_PICKLE}/forecast/feature_all.pickl'
            )
            if feature_all is None:
                feature_all = get_match_in_feature_all(
                    db_session=db_session,
                    match_ids=match_ids_all
                )
                save_pickle(
                    f'{DIR_PICKLE}/forecast/feature_all.pickl',
                    feature_all
                )
        else:
            # Получаем данные prediction и feature
            predict_played = get_match_in_prediction_all(
                db_session=db_session,
                match_ids=match_ids_played
            )

            feature_played = get_match_in_feature_all(
                db_session=db_session,
                match_ids=match_ids_played
            )

            predict_all = get_match_in_prediction_all(
                db_session=db_session,
                match_ids=match_ids_all
            )

            feature_all = get_match_in_feature_all(
                db_session=db_session,
                match_ids=match_ids_all
            )

        logger.info(
            f'Загружено: {len(predict_played)} прошедших '
            f'матчей, {len(predict_all)} всех матчей'
        )

        # Создание DataFrame
        df_matches_played = pd.DataFrame([
            item.as_dict() for item in predict_played
        ])
        df_matches_all = pd.DataFrame([
            item.as_dict() for item in predict_all
        ])

        df_features_played = pd.DataFrame([
            item.as_dict() for item in feature_played
        ])
        df_features_all = pd.DataFrame([
            item.as_dict() for item in feature_all
        ])

        # Объединение данных
        df_combined_played = pd.DataFrame()
        df_combined_all = pd.DataFrame()

        if not df_matches_played.empty and not df_features_played.empty:
            df_combined_played = pd.merge(
                df_matches_played,
                df_features_played,
                on='match_id',
                how='right'
            )

        if not df_matches_all.empty and not df_features_all.empty:
            df_combined_all = pd.merge(
                df_matches_all,
                df_features_all,
                on='match_id',
                how='right'
            )

        logger.info('Данные матчей успешно загружены и объединены')

        return df_combined_played, df_combined_all


# Вызывается из модуля FORECAST->funnel
def create_df_matches():
    """
    Оптимизированная версия создания DataFrame с данными матчей.
    Объединяет запросы для улучшения производительности.
    """

    def fetch_combined_data(db_session, all_flag):
        """
        Объединенный запрос для получения данных прогнозов и фич
        """
        logger.info(f'Старт -> get_prediction_matchs(all={all_flag})')
        match_ids = get_prediction_matchs(all=all_flag)
        logger.info(f'Финиш -> get_prediction_matchs(all={all_flag}) match_ids={len(match_ids)}')

        logger.info(f'Старт -> get_match_in_prediction_all')
        prediction_data = get_match_in_prediction_all(
                db_session=db_session,
                match_ids=match_ids
            )
        logger.info(f'Финиш -> get_match_in_prediction_all prediction_data={len(prediction_data)}')

        logger.info(f'Старт -> get_match_in_feature_all')
        feature_data = get_match_in_feature_all(
                db_session=db_session,
                match_ids=match_ids
            )
        logger.info(f'Финиш -> get_match_in_feature_all feature_data={len(feature_data)}')

        return prediction_data, feature_data

    with Session_pool() as db_session:
        logger.info('Начало загрузки данных матчей...')

        # Получаем данные для прошедших матчей
        predict_played, feature_played = fetch_combined_data(
            db_session=db_session,
            all_flag=False
        )

        # Получаем данные для всех матчей
        predict_all, feature_all = fetch_combined_data(
            db_session=db_session,
            all_flag=True
        )

        # Создание DataFrame
        df_matches_played = pd.DataFrame([
            item.as_dict() for item in predict_played
        ])
        df_matches_all = pd.DataFrame([
            item.as_dict() for item in predict_all
        ])

        df_features_played = pd.DataFrame([
            item.as_dict() for item in feature_played
        ])
        df_features_all = pd.DataFrame([
            item.as_dict() for item in feature_all
        ])

        # Объединение данных
        df_combined_played = pd.merge(
            df_matches_played,
            df_features_played,
            on='match_id',
            how='right'
        ) if not df_matches_played.empty and not df_features_played.empty else pd.DataFrame()

        df_combined_all = pd.merge(
            df_matches_all,
            df_features_all,
            on='match_id',
            how='right'
        ) if not df_matches_all.empty and not df_features_all.empty else pd.DataFrame()

        logger.info('Данные матчей успешно загружены и объединены')

        return df_combined_played, df_combined_all


# Вызывается из publisher.py
def process_matches(
        select_func,
        delta_param_name,
        delta_value,
        get_prediction_func,
        get_feature_func,
        TARGET_FIELDS
):
    # Получаем матчи за указанный период, используя правильное имя параметра
    matches, teams = select_func(
        **{delta_param_name: delta_value}
    )

    # Получаем прогнозы и фичи для матчей
    prediction_matches = get_prediction_func(
        matchs_id=matches
    )
    feature_matches = get_feature_func(
        matchs_id=matches
    )

    # Создаем DataFrame для прогнозов и фич
    df_matches = pd.DataFrame(
        predict.as_dict() for predict in prediction_matches
    )
    df_features = pd.DataFrame(
        feature.as_dict() for feature in feature_matches
    )

    # Добавляем match_id в список полей фич
    feature_fields_ext = TARGET_FIELDS + ['match_id']
    df_features = df_features[feature_fields_ext]

    # Объединяем прогнозы и фичи по match_id
    if not df_matches.empty:
        df_matches = pd.merge(
            df_matches,
            df_features,
            on='match_id',
            how='right'
        )
        df_matches.dropna(subset=['model_name'], inplace=True)
    else:
        df_matches = pd.DataFrame()

    return df_matches


def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Подготовка признаков и целевых переменных.

    Args:
        df: Исходный DataFrame

    Returns:
        df: признаки
    """
    # Целевые переменные Выставить вызов получение таргет из таблицы
    # df_target = df[TARGET_FIELDS].copy()

    # Признаки - исключаем служебные поля и целевые переменные
    # Исключаем match_id из удаляемых колонок, так как он нужен для идентификации
    columns_to_drop = [col for col in DROP_FIELD_EMBEDDING if col != 'match_id'] #+ TARGET_FIELDS
    # Проверяем, какие колонки действительно существуют в DataFrame
    existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
    
    df_feature = df.drop(columns=existing_columns_to_drop, axis=1)
    
    # Преобразуем только числовые колонки в float
    # Сначала попробуем преобразовать все колонки в числовые, заменив ошибки на NaN
    for col in df_feature.columns:
        df_feature[col] = pd.to_numeric(df_feature[col], errors='coerce')
    
    # Удаляем колонки, которые полностью состоят из NaN
    df_feature = df_feature.dropna(axis=1, how='all')
    
    # Заполняем оставшиеся NaN значения нулями
    df_feature = df_feature.fillna(0)
    
    # Преобразуем в float
    df_feature = df_feature.astype(float)

    return df_feature


def validate_dataframe(df: pd.DataFrame, required_columns: List[str]) -> bool:
    """
    Проверка DataFrame на наличие required столбцов.

    Args:
        df: DataFrame для проверки
        required_columns: Список обязательных столбцов

    Returns:
        True если все столбцы присутствуют, иначе False
    """
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.warning(f'Отсутствуют столбцы: {missing_columns}')
        return False
    return True


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Безопасное деление с обработкой деления на ноль.

    Args:
        numerator: Числитель
        denominator: Знаменатель
        default: Значение по умолчанию при делении на ноль

    Returns:
        Результат деления или значение по умолчанию
    """
    if denominator == 0:
        return default
    return numerator / denominator


def convert_numpy_to_python(obj):
    """
    Рекурсивно конвертирует numpy типы в стандартные Python типы.

    Args:
        obj: Объект для конвертации

    Returns:
        Объект с конвертированными типами
    """
    if isinstance(obj, (np.integer, np.int32, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float32, np.float64)):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {key: convert_numpy_to_python(value) for key, value in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_numpy_to_python(item) for item in obj]
    elif pd.isna(obj):
        return None
    return obj


def safe_json_dumps(data, indent=None):
    """
    Безопасная сериализация в JSON с обработкой numpy типов.

    Args:
        data: Данные для сериализации
        indent: Отступ для форматирования

    Returns:
        JSON строка
    """
    converted_data = convert_numpy_to_python(data)
    return json.dumps(converted_data, indent=indent, ensure_ascii=False)


"""
Утилиты для обработки данных.
"""


def handle_missing_features(df: pd.DataFrame, fill_value: float = 0.0) -> pd.DataFrame:
    """
    Обработка пропущенных значений в фичах.

    Args:
        df: DataFrame с фичами
        fill_value: Значение для заполнения пропусков

    Returns:
        Очищенный DataFrame
    """
    # Безопасная проверка на пропущенные значения
    has_nulls = False
    try:
        has_nulls = df.isnull().values.any()
    except (ValueError, TypeError):
        has_nulls = False
    
    if has_nulls:
        missing_count = df.isnull().sum().sum()
        logger.warning(f"Обнаружено {missing_count} пропущенных значений в фичах. Заполняем {fill_value}")

        # Заполняем пропуски
        df_filled = df.fillna(fill_value)

        # Проверяем, что все пропуски заполнены
        has_remaining_nulls = False
        try:
            has_remaining_nulls = df_filled.isnull().values.any()
        except (ValueError, TypeError):
            has_remaining_nulls = False
            
        if has_remaining_nulls:
            remaining_missing = df_filled.isnull().sum().sum()
            logger.error(f"После заполнения осталось {remaining_missing} пропущенных значений")

        return df_filled

    return df


def validate_targets(target_series: pd.Series, model_name: str) -> pd.Series:
    """
    Валидация целевых переменных.

    Args:
        target_series: Series с целевыми значениями
        model_name: Название модели для логирования

    Returns:
        Валидированный Series

    Raises:
        ValueError: Если обнаружены пропущенные значения
    """
    if target_series.isnull().values.any():
        missing_count = target_series.isnull().sum()
        null_mask = target_series.isnull()
        missing_indices = target_series[null_mask].index.tolist()

        logger.critical(
            f"КРИТИЧЕСКАЯ ОШИБКА: Обнаружено {missing_count} пропущенных значений "
            f"в таргете для модели {model_name}. Индексы: {missing_indices[:10]}"  # Логируем первые 10
        )

        # Можно также сохранить дополнительную информацию для анализа
        raise ValueError(f"Пропущенные значения в таргете модели {model_name}")

    # Проверяем на бесконечные значения
    if np.any(np.isinf(target_series.values)):
        inf_count = np.isinf(target_series.values).sum()
        logger.critical(
            f"КРИТИЧЕСКАЯ ОШИБКА: Обнаружено {inf_count} бесконечных значений "
            f"в таргете для модели {model_name}"
        )
        raise ValueError(f"Бесконечные значения в таргете модели {model_name}")

    return target_series


def clean_feature_data(df_features: pd.DataFrame) -> pd.DataFrame:
    """
    Полная очистка данных фич.

    Args:
        df_features: DataFrame с фичами

    Returns:
        Очищенный DataFrame
    """
    # Заполняем пропуски
    df_cleaned = handle_missing_features(df_features)

    # Заменяем бесконечные значения
    if np.any(np.isinf(df_cleaned.values)):
        inf_count = np.isinf(df_cleaned.values).sum()
        logger.warning(
            f"Обнаружено {inf_count} бесконечных значений в фичах. Заменяем на максимальные/минимальные значения")

        # Заменяем +inf на максимальное значение, -inf на минимальное
        for col in df_cleaned.columns:
            col_data = df_cleaned[col]
            if np.any(np.isinf(col_data)):
                max_val = col_data[np.isfinite(col_data)].max()
                min_val = col_data[np.isfinite(col_data)].min()

                # Заменяем +inf на max, -inf на min
                col_data = col_data.replace([np.inf], max_val)
                col_data = col_data.replace([-np.inf], min_val)
                df_cleaned[col] = col_data

    return df_cleaned


def prepare_features_and_targets(
        df_feature: pd.DataFrame,
        df_target: pd.DataFrame,
        feature_config: Dict[str, Any]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Подготовка признаков и целевых переменных с валидацией.

    Args:
        df_feature: Признаки
        df_target: Целевые переменные
        feature_config: Конфигурация признаков

    Returns:
        Кортеж (очищенные признаки, валидированные целевые переменные)
    """
    # Очищаем фичи
    df_feature_cleaned = clean_feature_data(df_feature)

    # Проверяем, что target данные не пустые
    if df_target.empty:
        logger.warning("Target данные пустые, возвращаем пустой DataFrame")
        return df_feature_cleaned, pd.DataFrame()

    # Проверяем соответствие количества записей
    if len(df_feature_cleaned) != len(df_target):
        logger.warning(f"Несоответствие количества записей после очистки: features={len(df_feature_cleaned)}, targets={len(df_target)}")
        
        # Получаем match_id из features и targets
        feature_match_ids = set(df_feature_cleaned['match_id'].tolist()) if 'match_id' in df_feature_cleaned.columns else set()
        target_match_ids = set(df_target['match_id'].tolist()) if 'match_id' in df_target.columns else set()
        
        # Находим общие match_id
        common_match_ids = feature_match_ids.intersection(target_match_ids)
        logger.info(f"Общие match_id: {len(common_match_ids)} из {len(feature_match_ids)} features и {len(target_match_ids)} targets")
        
        if common_match_ids:
            # Фильтруем данные по общим match_id
            df_feature_cleaned = df_feature_cleaned[df_feature_cleaned['match_id'].isin(common_match_ids)]
            df_target = df_target[df_target['match_id'].isin(common_match_ids)]
            logger.info(f"Отфильтровано до {len(df_feature_cleaned)} записей по общим match_id")
        else:
            logger.error("Нет общих match_id между features и targets!")
            return df_feature_cleaned, pd.DataFrame()

    # Валидируем таргеты для каждой модели
    df_target_validated = df_target.copy()

    for model_name, config in feature_config.items():
        target_col = config.target
        if target_col in df_target_validated.columns:
            try:
                df_target_validated[target_col] = validate_targets(
                    df_target_validated[target_col], model_name
                )
            except ValueError as e:
                logger.critical(f"Прерывание обработки из-за ошибки в таргете: {e}")
                raise
        else:
            logger.warning(f"Колонка {target_col} не найдена в target данных")

    return df_feature_cleaned, df_target_validated


def create_advanced_feature_config(df_columns=None) -> Dict[str, Any]:
    """
    Создание расширенной конфигурации признаков с настройками регуляризации.

    Args:
        df_columns: Список колонок DataFrame (для обратной совместимости)

    Returns:
        Конфигурация признаков
    """
    # Базовый конфиг
    config = {
        'win_draw_loss': {
            'features': [
                'home_goals_form', 'away_goals_form', 'home_possession_std',
                'away_possession_std', 'total_goals', 'goal_difference',
                'day_of_week', 'month', 'home_shots_trend', 'away_shots_trend',
                # Добавляем базовые фичи для совместимости
                'home_goals', 'away_goals', 'home_possession', 'away_possession'
            ],
            'target': 'result',  # Используем существующее название целевой переменной
            'task_type': 'classification',
            'normalization_method': 'robust',
            'l1_reg': 0.001,
            'l2_reg': 0.002,
            'dropout_rate': 0.3,
            'learning_rate': 0.001,
            'patience': 20,
            'min_delta': 0.0005
        },
        'total_goals': {
            'features': [
                'home_goals_last5', 'away_goals_last5', 'home_shots_form',
                'away_shots_form', 'total_goals', 'goals_ratio',
                'home_corners_std', 'away_corners_std',
                # Базовые фичи
                'home_goals', 'away_goals', 'home_shots', 'away_shots'
            ],
            'target': 'total_goals',
            'task_type': 'regression',
            'normalization_method': 'standard',
            'l1_reg': 0.0005,
            'l2_reg': 0.001,
            'dropout_rate': 0.2,
            'learning_rate': 0.001,
            'patience': 15,
            'min_delta': 0.001
        }
    }

    # Фильтруем фичи, которые существуют в данных
    if df_columns is not None:
        for model_name, model_config in config.items():
            existing_features = [f for f in model_config['features'] if f in df_columns]
            if existing_features:
                config[model_name]['features'] = existing_features
            else:
                # Если нет расширенных фич, используем базовые
                basic_features = [f for f in ['home_goals', 'away_goals', 'home_possession', 'away_possession']
                                  if f in df_columns]
                config[model_name]['features'] = basic_features

    return config

def create_advanced_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Создание расширенных признаков для улучшения качества модели.
    """
    df = df.copy()

    # Временные особенности
    date_columns = ['match_date', 'date', 'event_date']
    for date_col in date_columns:
        if date_col in df.columns:
            try:
                df[date_col] = pd.to_datetime(df[date_col])
                df['day_of_week'] = df[date_col].dt.dayofweek
                df['month'] = df[date_col].dt.month
                df['season_part'] = (df[date_col].dt.month % 12 + 3) // 3
                break
            except:
                continue

    # Статистические особенности команд
    for prefix in ['home_', 'away_', 'team1_', 'team2_']:
        # Форма команды (последние 5 матчей)
        for stat in ['goals', 'shots', 'corners', 'points']:
            col = f'{prefix}{stat}_last5'
            if col in df.columns:
                df[f'{prefix}{stat}_form'] = df[col].rolling(5, min_periods=1).mean().fillna(0)

        # Волатильность производительности
        for stat in ['goals', 'possession', 'rating']:
            col = f'{prefix}{stat}'
            if col in df.columns:
                df[f'{prefix}{stat}_std'] = df[col].rolling(10, min_periods=3).std().fillna(0)

    # Взаимодействия между командами
    goals_columns = [('home_goals', 'away_goals'), ('team1_goals', 'team2_goals')]
    for home_col, away_col in goals_columns:
        if all(col in df.columns for col in [home_col, away_col]):
            df['total_goals'] = df[home_col] + df[away_col]
            df['goal_difference'] = df[home_col] - df[away_col]
            df['goals_ratio'] = (df[home_col] + 1) / (df[away_col] + 1)
            break

    # Трендовые особенности
    for stat in ['goals', 'shots', 'corners', 'fouls']:
        for prefix in ['home_', 'away_', 'team1_', 'team2_']:
            col = f'{prefix}{stat}'
            if col in df.columns:
                df[f'{prefix}{stat}_trend'] = df[col].diff().rolling(3, min_periods=1).mean().fillna(0)

    # Заполнение пропущенных значений
    df = df.fillna(method='ffill').fillna(0)

    return df
