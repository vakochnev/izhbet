# izhbet/processing/prediction_keras.py
"""
Модуль для работы с предсказаниями на Keras с интеграцией мониторинга.
"""

import logging
import json
from typing import Any, Dict, Optional, List
from datetime import datetime

import pandas as pd
import numpy as np

from core.types import FeatureConfig
from core.evaluation import NumpyEncoder, ModelEvaluator, DataQualityMonitor
from core.utils import prepare_features_and_targets
from .keras_manager import KerasModelManager
from .keras_builder import KerasModelBuilder
from .keras_preprocessor import DataPreprocessor
from .keras_config import Config
from db.queries.match import get_match_id_pool
from db.storage.metric import save_metrics

logger = logging.getLogger(__name__)


def train_and_save_keras(
        models_dir: str,
        df_feature: pd.DataFrame,
        df_target: pd.DataFrame,
        feature_config: Dict[str, FeatureConfig],
        championship_info: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Обучение и сохранение моделей Keras с интеграцией мониторинга.
    """
    try:
        logger.info("Начинаем train_and_save_keras")
        
        # Проверка и очистка данных
        logger.info("Вызываем prepare_features_and_targets")
        df_feature_cleaned, df_target_validated = prepare_features_and_targets(
            df_feature, df_target, feature_config
        )
        logger.info(f"prepare_features_and_targets завершен: features={len(df_feature_cleaned)}, targets={len(df_target_validated)}")

        # Проверка качества данных
        logger.info("Создаем DataQualityMonitor")
        data_quality_monitor = DataQualityMonitor()
        logger.info("Объединяем DataFrame")
        df_combined = pd.concat([df_feature_cleaned, df_target_validated], axis=1)
        logger.info("Проверяем качество данных")
        quality_report = data_quality_monitor.check_data_quality(df_combined, 'training')
        logger.info("Качество данных проверено")

        logger.debug(
            f"Качество тренировочных данных: "
            f"{json.dumps(quality_report, indent=2, cls=NumpyEncoder)}"
        )

        # Проверяем, что есть данные для обучения
        logger.info(f"Проверяем количество данных: {len(df_feature_cleaned)} < {Config.MIN_SAMPLES_FOR_TRAINING}")
        if len(df_feature_cleaned) < Config.MIN_SAMPLES_FOR_TRAINING:
            logger.warning(
                f"Слишком мало данных для обучения: "
                f"{len(df_feature_cleaned)} samples"
            )
            return {}

        # Предобработка данных
        logger.info("Создаем DataPreprocessor")
        preprocessor = DataPreprocessor(
            df_feature_cleaned,
            df_target_validated,
            feature_config
        )
        logger.info("Вызываем preprocess_data")
        processed_data = preprocessor.preprocess_data()
        logger.info(f"preprocess_data завершен: {len(processed_data)} моделей")

        if not processed_data:
            logger.warning(
                "Нет данных для обучения после предобработки"
            )
            return {}

        trained_models = {}
        logger.info("Создаем KerasModelManager")
        model_manager = KerasModelManager(models_dir, feature_config)

        for model_name, model_data in processed_data.items():
            try:
                logger.info(f"Обрабатываем модель: {model_name}")
                # Создание и обучение модели
                model = _create_and_train_model(
                    model_name,
                    model_data,
                    models_dir,
                    feature_config
                )
                logger.info(f"Модель {model_name} обучена")

                # Оценка качества на тестовых данных
                logger.info(f"Делаем предсказания для {model_name}")
                y_pred = model.predict(model_data.X_test, verbose=0)
                logger.info(f"Предсказания для {model_name} готовы")

                if model_data.task_type == 'classification':
                    y_pred_classes = np.argmax(y_pred, axis=1)
                    evaluator = ModelEvaluator()
                    metrics = evaluator.evaluate_classification(
                        model_data.y_test, y_pred_classes, model_name
                    )
                else:  # regression
                    y_pred_flat = y_pred.flatten()
                    evaluator = ModelEvaluator()
                    metrics = evaluator.evaluate_regression(
                        model_data.y_test, y_pred_flat, model_name
                    )

                trained_models[model_name] = {
                    'model': model,
                    'processed_data': model_data,
                    'metrics': metrics,
                    'sample_size': len(model_data.X_train) + len(model_data.X_test),
                    'feature_count': len(feature_config[model_name].features),
                    'training_date': datetime.now().isoformat(),
                    'model_type': model_data.task_type
                }

                # Сохранение метрик обучения для мониторинга
                if Config.MONITORING_ENABLED and championship_info:
                    _save_training_metrics_for_monitoring(
                        model_name,
                        metrics,
                        trained_models[model_name],
                        championship_info
                    )

            except Exception as e:
                logger.error(f'Ошибка обучения модели {model_name}: {e}')
                continue

        # Сохранение моделей
        logger.info(f"Сохраняем {len(trained_models)} моделей")
        if trained_models:
            model_manager.save_models(models_dir, trained_models)

        logger.info("train_and_save_keras завершен успешно")
        return trained_models

    except Exception as e:
        logger.error(f'Критическая ошибка в train_and_save_keras: {e}')
        return {}


def _create_and_train_model(
        model_name: str,
        model_data: Any,
        models_dir: str,
        feature_config: Dict[str, Any]
) -> Any:
    """Создание и обучение одной модели."""
    try:
        # Получаем конфигурацию для типа модели из feature_config
        model_type = feature_config[model_name].task_type
        model_config = Config.get_model_config(model_type)

        # Определяем num_classes для классификации
        num_classes = None
        if model_type == 'classification':
            # Для классификации определяем количество уникальных классов
            unique_classes = len(np.unique(model_data.y_train))
            num_classes = unique_classes if unique_classes > 1 else 2  # Минимум 2 класса

        model = KerasModelBuilder.create_advanced_model(
            input_shape=model_data.X_train.shape[1],
            task_type=model_type,
            num_classes=num_classes,
            l1_reg=model_config['l1_reg'],
            l2_reg=model_config['l2_reg'],
            dropout_rate=model_config['dropout_rate'],
            initial_learning_rate=model_config['learning_rate']
        )

        callbacks_list = KerasModelBuilder.create_callbacks(
            models_dir, model_name,
            patience=model_config['patience'],
            min_delta=model_config['min_delta']
        )

        # Проверяем, что данные не пустые
        if (model_data.X_train.size == 0 or model_data.y_train.size == 0 or
                model_data.X_test.size == 0 or model_data.y_test.size == 0):
            raise ValueError(f"Пустые данные для модели {model_name}")

        # Подготовка данных для обучения
        X_train = model_data.X_train
        y_train = model_data.y_train
        X_val = model_data.X_test
        y_val = model_data.y_test

        # Для классификации проверяем, что y_train имеет правильную форму
        if model_type == 'classification' and len(y_train.shape) == 1:
            # y_train уже должен быть encoded (целые числа)
            pass

        history = model.fit(
            X_train,
            y_train,
            validation_data=(X_val, y_val),
            epochs=Config.EPOCHS,
            batch_size=Config.BATCH_SIZE,
            callbacks=callbacks_list,
            verbose=0
        )

        return model

    except Exception as e:
        logger.error(f'Ошибка создания и обучения модели {model_name}: {e}')
        raise

def _save_training_metrics_for_monitoring(
        model_name: str,
        metrics: Dict[str, float],
        model_info: Dict[str, Any],
        championship_info: Dict[str, Any],
) -> None:
    """Сохранение метрик обучения для системы мониторинга (в БД)."""
    if championship_info.get('championship_id') and championship_info.get('championship_name'):
        save_metrics(
            championship_info['championship_id'],
            championship_info['championship_name'],
            {
                'model_name': model_name,
                'metrics': metrics,
                'model_info': {
                    'sample_size': model_info['sample_size'],
                    'feature_count': model_info['feature_count'],
                    'model_type': model_info['model_type'],
                    'training_date': model_info['training_date']
                },
                'championship_info': championship_info,
                'evaluation_date': datetime.now().isoformat()
            }
        )

    logger.info(f"Метрики обучения сохранены для модели {model_name}")


def make_prediction_keras(
        models_dir: str,
        df: pd.DataFrame,
        feature_config: Dict[str, FeatureConfig]
) -> Dict[str, Dict[str, Any]]:
    """
    Выполнение предсказаний на основе обученных моделей.

    Args:
        models_dir: Директория с моделями
        df: DataFrame с данными для прогноза
        feature_config: Конфигурация признаков

    Returns:
        Словарь с результатами прогнозов
    """
    # Проверяем соответствие колонок в df и feature_config
    logger.info(f"Колонки в df: {list(df.columns)[:10]}...")
    logger.info(f"Количество колонок в df: {len(df.columns)}")
    
    # Получаем все фичи из feature_config
    all_features = set()
    for config in feature_config.values():
        all_features.update(config.features)
    
    logger.info(f"Фичи в feature_config: {len(all_features)}")
    logger.debug(f"Первые 10 фичей из feature_config: {list(all_features)[:10]}")
    
    # Проверяем, какие фичи отсутствуют в df
    missing_features = all_features - set(df.columns)
    if missing_features:
        logger.warning(f"Отсутствующие фичи в df: {list(missing_features)[:10]}...")
    
    # Проверяем, какие колонки в df не используются в feature_config
    unused_columns = set(df.columns) - all_features
    if unused_columns:
        logger.debug(f"Неиспользуемые колонки в df: {list(unused_columns)[:10]}...")
    
    prediction_service = KerasModelManager(models_dir, feature_config)
    predictions = {}

    for i in range(len(df)):
        try:
            prediction = _predict_single_match(df.iloc[i], prediction_service, feature_config)
            predictions.update(prediction)
        except Exception as e:
            # Безопасное получение match_id
            match_id = df.iloc[i].get('match_id', df.iloc[i].get('id', 'unknown'))
            logger.error(f'Ошибка предсказания для матча {match_id}: {e}')
            continue

    return predictions


def _predict_single_match(
        row: pd.Series,
        prediction_service: KerasModelManager,
        feature_config: Dict[str, FeatureConfig]
) -> Dict[int, Dict[str, Any]]:
    """Предсказание для одного матча."""
    from core.constants import TARGET_FIELDS

    prediction = prediction_service.batch_predict(
        row.to_frame().T,
        feature_config
    )

    # Безопасное получение match_id
    match_id = row.get('match_id', row.get('id', None))
    if match_id is None:
        raise ValueError("Не найден match_id в данных строки")
    
    match = get_match_id_pool(match_id)

    # Преобразуем структуру данных для совместимости с save_prediction
    formatted_prediction = _format_prediction_for_save(prediction)

    result = {
        match_id: {
            **formatted_prediction,
            'teamHome_id': match.teamHome_id,
            'teamAway_id': match.teamAway_id,
            'feature': {field: float(row[field]) if hasattr(row[field], '__len__') and len(row[field]) == 1 else row[field] for field in row.index if field not in ['match_id', 'id']},
            #'championship_id': match.championship_id,
            'prediction_timestamp': datetime.now().isoformat()
        }
    }

    return result


def _format_prediction_for_save(prediction: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Преобразует структуру предсказаний для совместимости с save_prediction."""
    import logging
    logger = logging.getLogger(__name__)
    
    # Маппинг One-Hot encoded полей на тип прогноза
    # Ключи в batch_predict - это model_name из feature_config (без префикса target_)
    field_mapping = {
        'win_draw_loss_home_win': 'win_draw_loss_home_win',
        'win_draw_loss_draw': 'win_draw_loss_draw', 
        'win_draw_loss_away_win': 'win_draw_loss_away_win',
        'oz_both_score': 'oz_yes',
        'oz_not_both_score': 'oz_no',
        'goal_home_scores': 'goal_home',
        'goal_home_no_score': 'goal_home',
        'goal_away_scores': 'goal_away',
        'goal_away_no_score': 'goal_away',
        'total_over': 'total',
        'total_under': 'total',
        'total_home_over': 'total_home',
        'total_home_under': 'total_home',
        'total_away_over': 'total_away',
        'total_away_under': 'total_away',
        'total_amount': 'total_amount',
        'total_home_amount': 'total_home_amount',
        'total_away_amount': 'total_away_amount'
    }
    
    # Группируем предсказания по типам
    grouped_predictions = {}
    
    logger.debug(f"Форматируем предсказания: {list(prediction.keys())}")
    
    # Обрабатываем модели в детерминированном порядке, чтобы собрать вероятности предсказуемо
    for model_name, pred_data in sorted(prediction.items(), key=lambda x: x[0]):
        if 'error' in pred_data:
            logger.warning(f"Ошибка в предсказании {model_name}: {pred_data['error']}")
            continue
            
        # Определяем тип поля
        field_type = None
        for onehot_field, old_field in field_mapping.items():
            if model_name == onehot_field:
                field_type = old_field
                break
        
        if field_type is None:
            # Если это не One-Hot поле, используем как есть
            field_type = model_name.replace('target_', '')
        
        logger.debug(f"Модель {model_name} -> тип поля {field_type}")
        
        if field_type not in grouped_predictions:
            grouped_predictions[field_type] = {
                'prediction': None,
                'probabilities': []
            }
        
        # Для One-Hot полей объединяем вероятности
        if field_type in ['win_draw_loss_home_win', 'win_draw_loss_draw', 'win_draw_loss_away_win', 'oz_yes', 'oz_no', 'goal_home', 'goal_away', 'total', 'total_home', 'total_away']:
            # Безопасная проверка probabilities
            has_probabilities = False
            probabilities_list = []
            if 'probabilities' in pred_data and pred_data['probabilities'] is not None:
                try:
                    # Конвертируем в список для безопасной работы
                    if hasattr(pred_data['probabilities'], '__iter__') and not isinstance(pred_data['probabilities'], str):
                        probabilities_list = list(pred_data['probabilities'])
                        has_probabilities = len(probabilities_list) > 0
                    else:
                        probabilities_list = [float(pred_data['probabilities'])]
                        has_probabilities = True
                except (ValueError, TypeError):
                    has_probabilities = False
                    probabilities_list = []
            
            if has_probabilities:
                # Для WIN/DRAW/LOSS подмоделей сохраняем напрямую первую вероятность из каждой подмодели
                if field_type in ['win_draw_loss_home_win', 'win_draw_loss_draw', 'win_draw_loss_away_win']:
                    grouped_predictions[field_type]['probabilities'] = [float(probabilities_list[0])]
                # Для OZ_YES и OZ_NO сохраняем напрямую первую вероятность из каждой подмодели
                elif field_type in ['oz_yes', 'oz_no']:
                    grouped_predictions[field_type]['probabilities'] = [float(probabilities_list[0])]
                # Для GOAL_HOME гарантируем порядок вероятностей: [no, yes]
                elif field_type == 'goal_home':
                    # ожидаем две подмодели: goal_home_no_score и goal_home_scores
                    if 'goal_home_no_score' in model_name:
                        # индекс 0 = NO
                        probs = grouped_predictions[field_type]['probabilities']
                        if isinstance(probs, list) and len(probs) == 0:
                            grouped_predictions[field_type]['probabilities'] = [0.0, 0.0]
                        grouped_predictions[field_type]['probabilities'][0] = float(probabilities_list[0])
                    elif 'goal_home_scores' in model_name:
                        # индекс 1 = YES
                        probs = grouped_predictions[field_type]['probabilities']
                        if isinstance(probs, list) and len(probs) == 0:
                            grouped_predictions[field_type]['probabilities'] = [0.0, 0.0]
                        grouped_predictions[field_type]['probabilities'][1] = float(probabilities_list[0])
                    else:
                        # fallback: как есть
                        probs = grouped_predictions[field_type]['probabilities']
                        if isinstance(probs, list) and len(probs) == 0:
                            grouped_predictions[field_type]['probabilities'] = list(probabilities_list)
                        else:
                            grouped_predictions[field_type]['probabilities'].extend(list(probabilities_list))
                # Для GOAL_AWAY гарантируем порядок вероятностей: [no, yes]
                elif field_type == 'goal_away':
                    # ожидаем две подмодели: goal_away_no_score и goal_away_scores
                    if 'goal_away_no_score' in model_name:
                        # индекс 0 = NO
                        probs = grouped_predictions[field_type]['probabilities']
                        if isinstance(probs, list) and len(probs) == 0:
                            grouped_predictions[field_type]['probabilities'] = [0.0, 0.0]
                        grouped_predictions[field_type]['probabilities'][0] = float(probabilities_list[0])
                    elif 'goal_away_scores' in model_name:
                        # индекс 1 = YES
                        probs = grouped_predictions[field_type]['probabilities']
                        if isinstance(probs, list) and len(probs) == 0:
                            grouped_predictions[field_type]['probabilities'] = [0.0, 0.0]
                        grouped_predictions[field_type]['probabilities'][1] = float(probabilities_list[0])
                    else:
                        # fallback: как есть
                        probs = grouped_predictions[field_type]['probabilities']
                        if isinstance(probs, list) and len(probs) == 0:
                            grouped_predictions[field_type]['probabilities'] = list(probabilities_list)
                        else:
                            grouped_predictions[field_type]['probabilities'].extend(list(probabilities_list))
                # Для TOTAL гарантируем порядок вероятностей: [under, over]
                elif field_type == 'total':
                    # ожидаем две подмодели: total_under и total_over
                    if 'total_under' in model_name:
                        # индекс 0 = UNDER
                        probs = grouped_predictions[field_type]['probabilities']
                        if isinstance(probs, list) and len(probs) == 0:
                            grouped_predictions[field_type]['probabilities'] = [0.0, 0.0]
                        grouped_predictions[field_type]['probabilities'][0] = float(probabilities_list[0])
                    elif 'total_over' in model_name:
                        # индекс 1 = OVER
                        probs = grouped_predictions[field_type]['probabilities']
                        if isinstance(probs, list) and len(probs) == 0:
                            grouped_predictions[field_type]['probabilities'] = [0.0, 0.0]
                        grouped_predictions[field_type]['probabilities'][1] = float(probabilities_list[0])
                    else:
                        # fallback: как есть
                        probs = grouped_predictions[field_type]['probabilities']
                        if isinstance(probs, list) and len(probs) == 0:
                            grouped_predictions[field_type]['probabilities'] = list(probabilities_list)
                        else:
                            grouped_predictions[field_type]['probabilities'].extend(list(probabilities_list))
                # Для TOTAL_HOME гарантируем порядок вероятностей: [under, over]
                elif field_type == 'total_home':
                    # ожидаем две подмодели: total_home_under и total_home_over
                    if 'total_home_under' in model_name:
                        # индекс 0 = UNDER
                        probs = grouped_predictions[field_type]['probabilities']
                        if isinstance(probs, list) and len(probs) == 0:
                            grouped_predictions[field_type]['probabilities'] = [0.0, 0.0]
                        grouped_predictions[field_type]['probabilities'][0] = float(probabilities_list[0])
                    elif 'total_home_over' in model_name:
                        # индекс 1 = OVER
                        probs = grouped_predictions[field_type]['probabilities']
                        if isinstance(probs, list) and len(probs) == 0:
                            grouped_predictions[field_type]['probabilities'] = [0.0, 0.0]
                        grouped_predictions[field_type]['probabilities'][1] = float(probabilities_list[0])
                    else:
                        # fallback: как есть
                        probs = grouped_predictions[field_type]['probabilities']
                        if isinstance(probs, list) and len(probs) == 0:
                            grouped_predictions[field_type]['probabilities'] = list(probabilities_list)
                        else:
                            grouped_predictions[field_type]['probabilities'].extend(list(probabilities_list))
                # Для TOTAL_AWAY гарантируем порядок вероятностей: [under, over]
                elif field_type == 'total_away':
                    # ожидаем две подмодели: total_away_under и total_away_over
                    if 'total_away_under' in model_name:
                        # индекс 0 = UNDER
                        probs = grouped_predictions[field_type]['probabilities']
                        if isinstance(probs, list) and len(probs) == 0:
                            grouped_predictions[field_type]['probabilities'] = [0.0, 0.0]
                        grouped_predictions[field_type]['probabilities'][0] = float(probabilities_list[0])
                    elif 'total_away_over' in model_name:
                        # индекс 1 = OVER
                        probs = grouped_predictions[field_type]['probabilities']
                        if isinstance(probs, list) and len(probs) == 0:
                            grouped_predictions[field_type]['probabilities'] = [0.0, 0.0]
                        grouped_predictions[field_type]['probabilities'][1] = float(probabilities_list[0])
                    else:
                        # fallback: как есть
                        probs = grouped_predictions[field_type]['probabilities']
                        if isinstance(probs, list) and len(probs) == 0:
                            grouped_predictions[field_type]['probabilities'] = list(probabilities_list)
                        else:
                            grouped_predictions[field_type]['probabilities'].extend(list(probabilities_list))
                else:
                    # Добавляем только если еще нет вероятностей для этого типа
                    probs = grouped_predictions[field_type]['probabilities']
                    if isinstance(probs, list) and len(probs) == 0:
                        grouped_predictions[field_type]['probabilities'] = list(probabilities_list)
                    else:
                        # Объединяем вероятности для One-Hot групп
                        grouped_predictions[field_type]['probabilities'].extend(list(probabilities_list))
            if 'prediction' in pred_data and grouped_predictions[field_type]['prediction'] is None:
                grouped_predictions[field_type]['prediction'] = pred_data['prediction']
        
        # Для бинарных One-Hot полей определяем итоговый прогноз на основе максимальной вероятности
        if field_type in ['oz_yes', 'oz_no', 'goal_home', 'goal_away', 'total', 'total_home', 'total_away']:
            # Безопасная проверка grouped_predictions probabilities
            has_grouped_probabilities = False
            grouped_probs = []
            if 'probabilities' in grouped_predictions[field_type] and grouped_predictions[field_type]['probabilities'] is not None:
                try:
                    grouped_probs = list(grouped_predictions[field_type]['probabilities'])
                    has_grouped_probabilities = len(grouped_probs) > 0
                except (ValueError, TypeError):
                    has_grouped_probabilities = False
                    grouped_probs = []
            
            if has_grouped_probabilities:
                probs = grouped_probs
                if len(probs) >= 1:  # Для oz_yes/oz_no достаточно одной вероятности
                    if field_type in ['oz_yes', 'oz_no']:
                        # Для oz_yes/oz_no используем вероятность как есть (0 = no, 1 = yes)
                        grouped_predictions[field_type]['prediction'] = 1 if probs[0] > 0.5 else 0
                    elif field_type in ['goal_home', 'goal_away'] and len(probs) >= 2:
                        # Для goal_home/goal_away: 0 = no, 1 = yes
                        max_idx = probs.index(max(probs))
                        grouped_predictions[field_type]['prediction'] = max_idx
                    elif field_type in ['total', 'total_home', 'total_away'] and len(probs) >= 2:
                        # Для тоталов: 0 = under, 1 = over
                        max_idx = probs.index(max(probs))
                        grouped_predictions[field_type]['prediction'] = max_idx
        else:
            # Для регрессионных полей и одиночных подмоделей используем как есть
            grouped_predictions[field_type] = pred_data
    
    logger.debug(f"Результат форматирования: {list(grouped_predictions.keys())}")
    return grouped_predictions


def _convert_numpy_types(data):
    """Конвертирует numpy типы в стандартные Python типы."""
    if isinstance(data, np.ndarray):
        if data.dtype.kind in 'iub':  # integer, unsigned integer, boolean
            return data.astype(int).tolist()
        elif data.dtype.kind in 'f':  # float
            return data.astype(float).tolist()
        else:
            return data.tolist()
    elif isinstance(data, (np.integer, np.int32, np.int64)):
        return int(data)
    elif isinstance(data, (np.floating, np.float32, np.float64)):
        return float(data)
    elif isinstance(data, np.bool_):
        return bool(data)
    return data