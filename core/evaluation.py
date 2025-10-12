# izhbet/core/evaluation.py
"""
Модуль для оценки качества моделей и мониторинга.
"""

import logging
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    mean_absolute_error, mean_squared_error, r2_score,
    confusion_matrix, classification_report
)
import matplotlib.pyplot as plt
import seaborn as sns
import os

logger = logging.getLogger(__name__)


class NumpyEncoder(json.JSONEncoder):
    """Кастомный JSON encoder для обработки numpy типов."""

    def default(self, obj):
        if isinstance(obj, (np.integer, np.int32, np.int64)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float32, np.float64)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.bool_):
            return bool(obj)
        elif pd.isna(obj):  # Обработка NaN значений
            return None
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, timedelta):
            return str(obj)
        return super().default(obj)


class ModelEvaluator:
    """Класс для оценки качества моделей."""

    def __init__(self, results_dir: str = 'results'):
        self.results_dir = results_dir
        os.makedirs(results_dir, exist_ok=True)

    def evaluate_classification(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray,
        model_name: str,
        timestamp: datetime = None
    ) -> Dict[str, float]:
        """
        Оценка качества классификационной модели.

        Args:
            y_true: Истинные значения
            y_pred: Предсказанные значения
            model_name: Название модели
            timestamp: Время оценки

        Returns:
            Словарь с метриками качества
        """
        # Конвертируем numpy типы в Python типы
        y_true_converted = self._convert_numpy_types(y_true)
        y_pred_converted = self._convert_numpy_types(y_pred)

        metrics = {
            'accuracy': float(accuracy_score(y_true_converted, y_pred_converted)),
            'precision': float(precision_score(y_true_converted, y_pred_converted,
                                             average='weighted', zero_division=0)),
            'recall': float(recall_score(y_true_converted, y_pred_converted,
                                       average='weighted', zero_division=0)),
            'f1_score': float(f1_score(y_true_converted, y_pred_converted,
                                     average='weighted', zero_division=0)),
        }

        # Дополнительные метрики для бинарной классификации
        unique_classes = len(np.unique(y_true_converted))
        if unique_classes == 2:
            metrics.update({
                'precision_binary': float(precision_score(y_true_converted, y_pred_converted,
                                                        average='binary', zero_division=0)),
                'recall_binary': float(recall_score(y_true_converted, y_pred_converted,
                                                  average='binary', zero_division=0)),
                'f1_binary': float(f1_score(y_true_converted, y_pred_converted,
                                          average='binary', zero_division=0)),
            })

        self._save_evaluation_results(metrics, model_name, 'classification', timestamp)
        return metrics

    def evaluate_regression(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray,
        model_name: str,
        timestamp: datetime = None
    ) -> Dict[str, float]:
        """
        Оценка качества регрессионной модели.

        Args:
            y_true: Истинные значения
            y_pred: Предсказанные значения
            model_name: Название модели
            timestamp: Время оценки

        Returns:
            Словарь с метриками качества
        """
        # Конвертируем numpy типы в Python типы
        y_true_converted = self._convert_numpy_types(y_true)
        y_pred_converted = self._convert_numpy_types(y_pred)

        # Вычисляем абсолютные ошибки для min/max
        absolute_errors = np.abs(np.array(y_true_converted) - np.array(y_pred_converted))
        
        metrics = {
            'mae': float(mean_absolute_error(y_true_converted, y_pred_converted)),
            'mse': float(mean_squared_error(y_true_converted, y_pred_converted)),
            'rmse': float(np.sqrt(mean_squared_error(y_true_converted, y_pred_converted))),
            'r2': float(r2_score(y_true_converted, y_pred_converted)),
            'min_error': float(np.min(absolute_errors)),
            'max_error': float(np.max(absolute_errors)),
        }

        self._save_evaluation_results(metrics, model_name, 'regression', timestamp)
        return metrics

    def _convert_numpy_types(self, data: Any) -> Any:
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
        elif pd.isna(data):
            return None
        return data

    def _save_evaluation_results(
        self,
        metrics: Dict[str, float],
        model_name: str,
        model_type: str,
        timestamp: datetime
    ) -> None:
        """Отключено: хранилище метрик перенесено в БД (таблица metrics)."""
        logger.debug(
            f"Skip writing metrics to files for model={model_name}, type={model_type}. "
            f"Metrics are stored in DB."
        )
        return

    def create_confusion_matrix(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray,
        model_name: str,
        class_names: List[str] = None
    ) -> None:
        """Создание и сохранение матрицы ошибок."""
        # Конвертируем numpy типы
        y_true_converted = self._convert_numpy_types(y_true)
        y_pred_converted = self._convert_numpy_types(y_pred)

        cm = confusion_matrix(y_true_converted, y_pred_converted)

        try:
            plt.figure(figsize=(10, 8))
            sns.heatmap(cm, annot=True, fmt='d', cmap='Blues')
            plt.title(f'Матрица ошибок - {model_name}')
            plt.ylabel('Истинные значения')
            plt.xlabel('Предсказанные значения')

            if class_names:
                plt.xticks(np.arange(len(class_names)) + 0.5, class_names)
                plt.yticks(np.arange(len(class_names)) + 0.5, class_names)

            filename = f"confusion_matrix_{model_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            filepath = os.path.join(self.results_dir, filename)
            plt.savefig(filepath)
            plt.close()

            logger.info(f"Матрица ошибок сохранена: {filepath}")
        except Exception as e:
            logger.error(f"Ошибка создания матрицы ошибок: {e}")


class ModelMonitor:
    """Класс для мониторинга моделей в реальном времени."""

    def __init__(self, monitoring_dir: str = 'monitoring'):
        self.monitoring_dir = monitoring_dir
        os.makedirs(monitoring_dir, exist_ok=True)
        self.metrics_history = {}

    def track_metrics(
        self,
        metrics: Dict[str, float],
        model_name: str,
        timestamp: datetime = None
    ) -> None:
        """
        Отслеживание метрик модели.

        Args:
            metrics: Метрики качества
            model_name: Название модели
            timestamp: Время измерения
        """
        if timestamp is None:
            timestamp = datetime.now()

        if model_name not in self.metrics_history:
            self.metrics_history[model_name] = []

        # Конвертируем numpy типы в метриках
        converted_metrics = self._convert_metrics_types(metrics)

        record = {
            'timestamp': timestamp,
            'metrics': converted_metrics
        }

        self.metrics_history[model_name].append(record)
        self._save_monitoring_data(model_name)

        # Проверка на ухудшение качества
        self._check_quality_degradation(model_name, converted_metrics)

    def _convert_metrics_types(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Конвертирует типы в метриках для JSON сериализации."""
        converted = {}
        for key, value in metrics.items():
            if isinstance(value, (np.integer, np.int32, np.int64)):
                converted[key] = int(value)
            elif isinstance(value, (np.floating, np.float32, np.float64)):
                converted[key] = float(value)
            elif isinstance(value, np.bool_):
                converted[key] = bool(value)
            elif pd.isna(value):
                converted[key] = None
            else:
                converted[key] = value
        return converted

    def _save_monitoring_data(self, model_name: str) -> None:
        """Сохранение данных мониторинга."""
        filename = f"{model_name}_monitoring.json"
        filepath = os.path.join(self.monitoring_dir, filename)

        try:
            # Конвертируем datetime в строки для JSON
            save_data = []
            for record in self.metrics_history[model_name]:
                save_record = record.copy()
                save_record['timestamp'] = record['timestamp'].isoformat()
                save_data.append(save_record)

            with open(filepath, 'w') as f:
                json.dump(save_data, f, indent=2, cls=NumpyEncoder)
        except Exception as e:
            logger.error(f"Ошибка сохранения данных мониторинга: {e}")

    def _check_quality_degradation(self, model_name: str, current_metrics: Dict[str, float]) -> None:
        """Проверка на ухудшение качества модели."""
        if len(self.metrics_history[model_name]) < 2:
            return

        # Берем предыдущие 5 записей для сравнения
        recent_records = self.metrics_history[model_name][-6:-1]
        if not recent_records:
            return

        for metric_name, current_value in current_metrics.items():
            if current_value is None:
                continue

            previous_values = [r['metrics'].get(metric_name) for r in recent_records]
            previous_values = [v for v in previous_values if v is not None]

            if not previous_values:
                continue

            avg_previous = np.mean(previous_values)

            if metric_name in ['mse', 'mae', 'rmse']:  # Метрики, где меньше = лучше
                if current_value > avg_previous * 1.2:  # Ухудшение на 20%
                    logger.warning(
                        f"⚠️ Ухудшение метрики {metric_name} для модели {model_name}: "
                        f"было {avg_previous:.3f}, стало {current_value:.3f}"
                    )

            elif metric_name in ['accuracy', 'precision', 'recall', 'f1_score', 'r2']:  # Метрики, где больше = лучше
                if current_value < avg_previous * 0.8:  # Ухудшение на 20%
                    logger.warning(
                        f"⚠️ Ухудшение метрики {metric_name} для модели {model_name}: "
                        f"было {avg_previous:.3f}, стало {current_value:.3f}"
                    )

    def generate_report(self, model_name: str, days: int = 30) -> Dict[str, Any]:
        """Генерация отчета за указанный период."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        model_data = self.metrics_history.get(model_name, [])
        filtered_data = [
            record for record in model_data
            if start_date <= record['timestamp'] <= end_date
        ]

        if not filtered_data:
            return {}

        report = {
            'model_name': model_name,
            'period_start': start_date.isoformat(),
            'period_end': end_date.isoformat(),
            'total_records': len(filtered_data),
            'metrics_trend': {},
            'summary': {}
        }

        # Анализ трендов по метрикам
        for metric_name in filtered_data[0]['metrics'].keys():
            values = [record['metrics'][metric_name] for record in filtered_data
                     if metric_name in record['metrics'] and record['metrics'][metric_name] is not None]

            if values:
                report['metrics_trend'][metric_name] = {
                    'min': min(values),
                    'max': max(values),
                    'mean': float(np.mean(values)),
                    'std': float(np.std(values)) if len(values) > 1 else 0.0,
                    'trend': self._calculate_trend(values)
                }

        return report

    def _calculate_trend(self, values: List[float]) -> str:
        """Расчет тренда значений."""
        if len(values) < 2:
            return 'stable'

        # Простая линейная регрессия для определения тренда
        x = np.arange(len(values))
        slope, _ = np.polyfit(x, values, 1)

        if slope > 0.01:
            return 'improving'
        elif slope < -0.01:
            return 'deteriorating'
        else:
            return 'stable'


class DataQualityMonitor:
    """Мониторинг качества данных."""

    def __init__(self):
        self.quality_metrics = {}

    def check_data_quality(self, df: pd.DataFrame, data_type: str) -> Dict[str, Any]:
        """
        Проверка качества данных.

        Args:
            df: DataFrame для проверки
            data_type: Тип данных ('training', 'validation', 'production')

        Returns:
            Словарь с метриками качества данных
        """
        logger.info(f"Начинаем проверку качества данных: {df.shape}")
        
        try:
            logger.info("Создаем базовый quality_report")
            quality_report = {
                'timestamp': datetime.now().isoformat(),
                'data_type': data_type,
                'row_count': int(len(df)),
                'column_count': int(len(df.columns)),
                'missing_values': {},
                'missing_percentage': {},
                'duplicate_rows': 0,
                'data_types': {},
                'numeric_stats': {},
                'categorical_stats': {}
            }
            
            logger.info("Вычисляем missing_values")
            quality_report['missing_values'] = self._convert_series_to_dict(df.isnull().sum())
            
            logger.info("Вычисляем missing_percentage")
            quality_report['missing_percentage'] = self._convert_series_to_dict((df.isnull().sum() / len(df) * 100))
            
            logger.info("Вычисляем duplicate_rows")
            quality_report['duplicate_rows'] = int(df.duplicated().sum())
            
            logger.info("Вычисляем data_types")
            quality_report['data_types'] = self._convert_dtypes_to_string(df.dtypes)
            
        except Exception as e:
            logger.error(f"Ошибка в базовом quality_report: {e}")
            raise

        # Статистика для числовых колонок
        try:
            logger.info("Обрабатываем числовые колонки")
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            logger.info(f"Найдено {len(numeric_cols)} числовых колонок")
            
            for i, col in enumerate(numeric_cols):
                if i % 100 == 0:  # Логируем каждые 100 колонок
                    logger.info(f"Обрабатываем числовую колонку {i+1}/{len(numeric_cols)}: {col}")
                
                try:
                    col_series = df[col]
                    has_data = len(col_series) > 0
                    
                    # Безопасное вычисление статистик с обработкой исключений
                    try:
                        mean_val = col_series.mean()
                        mean_is_na = pd.isna(mean_val)
                        mean_ok = has_data and not mean_is_na
                    except Exception as e:
                        logger.warning(f"Ошибка при вычислении mean для {col}: {e}")
                        mean_val = 0.0
                        mean_ok = False
                    
                    try:
                        std_val = col_series.std()
                        std_is_na = pd.isna(std_val)
                        std_ok = has_data and not std_is_na
                    except Exception as e:
                        logger.warning(f"Ошибка при вычислении std для {col}: {e}")
                        std_val = 0.0
                        std_ok = False
                    
                    try:
                        min_val = col_series.min()
                        min_is_na = pd.isna(min_val)
                        min_ok = has_data and not min_is_na
                    except Exception as e:
                        logger.warning(f"Ошибка при вычислении min для {col}: {e}")
                        min_val = 0.0
                        min_ok = False
                    
                    try:
                        max_val = col_series.max()
                        max_is_na = pd.isna(max_val)
                        max_ok = has_data and not max_is_na
                    except Exception as e:
                        logger.warning(f"Ошибка при вычислении max для {col}: {e}")
                        max_val = 0.0
                        max_ok = False
                    
                    try:
                        median_val = col_series.median()
                        median_is_na = pd.isna(median_val)
                        median_ok = has_data and not median_is_na
                    except Exception as e:
                        logger.warning(f"Ошибка при вычислении median для {col}: {e}")
                        median_val = 0.0
                        median_ok = False
                    
                    quality_report['numeric_stats'][str(col)] = {
                        'mean': float(mean_val) if mean_ok else 0.0,
                        'std': float(std_val) if std_ok else 0.0,
                        'min': float(min_val) if min_ok else 0.0,
                        'max': float(max_val) if max_ok else 0.0,
                        'median': float(median_val) if median_ok else 0.0
                    }
                except Exception as col_error:
                    logger.error(f"Ошибка в числовой колонке {col}: {col_error}")
                    raise
                    
        except Exception as e:
            logger.error(f"Ошибка в обработке числовых колонок: {e}")
            raise

        # Статистика для категориальных колонок
        try:
            logger.info("Обрабатываем категориальные колонки")
            categorical_cols = df.select_dtypes(include=['object']).columns
            logger.info(f"Найдено {len(categorical_cols)} категориальных колонок")
            
            for i, col in enumerate(categorical_cols):
                logger.info(f"Обрабатываем категориальную колонку {i+1}/{len(categorical_cols)}: {col}")
                
                try:
                    col_series = df[col]
                    col_mode = col_series.mode()
                    col_value_counts = col_series.value_counts()
                    quality_report['categorical_stats'][str(col)] = {
                        'unique_count': int(col_series.nunique()),
                        'most_common': str(col_mode.iloc[0]) if len(col_mode) > 0 else None,
                        'most_common_count': int(col_value_counts.iloc[0]) if len(col_value_counts) > 0 else 0
                    }
                except Exception as col_error:
                    logger.error(f"Ошибка в категориальной колонке {col}: {col_error}")
                    raise
                    
        except Exception as e:
            logger.error(f"Ошибка в обработке категориальных колонок: {e}")
            raise

        # Проверка аномалий
        self._check_anomalies(quality_report)

        return quality_report

    def _convert_series_to_dict(self, series: pd.Series) -> Dict:
        """Конвертирует Series в словарь с JSON-сериализуемыми значениями."""
        result = {}
        for key, value in series.items():
            # Безопасная проверка на NaN
            try:
                is_na = pd.isna(value)
                if isinstance(is_na, pd.Series):
                    is_na = is_na.iloc[0] if len(is_na) > 0 else False
            except Exception:
                is_na = False
            
            if is_na:
                result[str(key)] = None
            elif isinstance(value, (np.integer, np.int32, np.int64)):
                result[str(key)] = int(value)
            elif isinstance(value, (np.floating, np.float32, np.float64)):
                result[str(key)] = float(value)
            else:
                result[str(key)] = value
        return result

    def _convert_dtypes_to_string(self, dtypes: pd.Series) -> Dict:
        """Конвертирует dtypes в строки."""
        result = {}
        try:
            for col, dtype in dtypes.items():
                result[str(col)] = str(dtype)
        except Exception as e:
            logger.warning(f"Ошибка при конвертации dtypes: {e}")
            # Fallback: создаем пустой словарь
            result = {}
        return result

    def _check_anomalies(self, quality_report: Dict[str, Any]) -> None:
        """Проверка на аномалии в данных."""
        try:
            # Проверка на слишком много пропущенных значений
            high_missing = {}
            if 'missing_percentage' in quality_report:
                for col, perc in quality_report['missing_percentage'].items():
                    try:
                        if perc is not None and isinstance(perc, (int, float)) and perc > 50:
                            high_missing[col] = perc
                    except Exception as e:
                        logger.warning(f"Ошибка при проверке пропущенных значений для {col}: {e}")

            if high_missing:
                logger.debug(f"Высокий процент пропущенных значений: {high_missing}")

            # Проверка на постоянные значения (игнорируем некоторые ожидаемые колонки)
            ignored_columns = ['home_weak_overtime_losses', 'away_weak_overtime_losses']

            # Проверка на постоянные значения
            if 'numeric_stats' in quality_report:
                for col, stats in quality_report['numeric_stats'].items():
                    try:
                        if col in ignored_columns:
                            continue  # Пропускаем ожидаемые постоянные значения

                        if isinstance(stats, dict) and 'std' in stats and stats['std'] == 0:
                            logger.debug(f"Постоянное значение в колонке {col}")
                    except Exception as e:
                        logger.warning(f"Ошибка при проверке постоянных значений для {col}: {e}")
                        
        except Exception as e:
            logger.warning(f"Ошибка в _check_anomalies: {e}")


# Утилитарные функции
def convert_numpy_to_python(obj: Any) -> Any:
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
    elif isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def safe_json_dumps(data: Any, indent: Optional[int] = None) -> str:
    """
    Безопасная сериализация в JSON с обработкой numpy типов.

    Args:
        data: Данные для сериализации
        indent: Отступ для форматирования

    Returns:
        JSON строка
    """
    converted_data = convert_numpy_to_python(data)
    return json.dumps(converted_data, indent=indent, ensure_ascii=False, cls=NumpyEncoder)