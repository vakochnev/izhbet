# izhbet/processing/neural_conformal.py
"""
Конформное прогнозирование на основе существующих прогнозов нейронной сети.
Перенесено из forecast/conformal_neural.py в processing/.
"""
import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class NeuralConformalPredictor:
    """
    Конформный предиктор на основе существующих прогнозов нейронной сети.
    Использует прогнозы из таблицы predictions для создания интервалов неопределенности.
    """

    def __init__(self, confidence_level: float = 0.95):
        self.confidence_level = confidence_level
        self.quantiles = {}
        self.is_fitted = False

    def fit(self, predictions_df: pd.DataFrame, outcomes_df: pd.DataFrame) -> None:
        """Обучает конформный предиктор на основе прогнозов и исходов."""
        logger.info("Обучение конформного предиктора на основе прогнозов нейронной сети")

        merged_df = pd.merge(predictions_df, outcomes_df, on='match_id', how='inner')
        if merged_df.empty:
            logger.warning("Нет данных для обучения конформного предиктора")
            return

        logger.info(f"Объединено {len(merged_df)} записей для обучения")

        for forecast_type in self._get_forecast_types():
            try:
                logger.info(f"Обучение конформного предиктора для {forecast_type}")
                residuals = self._compute_residuals(merged_df, forecast_type)
                if len(residuals) > 0:
                    quantile = np.quantile(residuals, self.confidence_level)
                    self.quantiles[forecast_type] = quantile
                    logger.info(f"Квантиль для {forecast_type}: {quantile:.4f}")
                else:
                    logger.warning(f"Нет остатков для {forecast_type}")
                    self.quantiles[forecast_type] = 0.1
            except Exception as e:
                logger.error(f"Ошибка при обучении {forecast_type}: {e}")
                self.quantiles[forecast_type] = 0.1

        self.is_fitted = True
        logger.info("Конформный предиктор обучен на основе прогнозов нейронной сети")

    def _get_forecast_types(self) -> List[str]:
        return [
            'win_draw_loss', 'oz', 'goal_home', 'goal_away',
            'total', 'total_home', 'total_away',
            'total_amount', 'total_home_amount', 'total_away_amount'
        ]

    def _compute_residuals(self, df: pd.DataFrame, forecast_type: str) -> np.ndarray:
        residuals: List[float] = []
        for _, row in df.iterrows():
            try:
                if forecast_type in ['win_draw_loss', 'oz', 'goal_home', 'goal_away', 'total', 'total_home', 'total_away']:
                    residual = self._compute_classification_residual(row, forecast_type)
                else:
                    residual = self._compute_regression_residual(row, forecast_type)
                if residual is not None:
                    residuals.append(residual)
            except Exception as e:
                logger.debug(f"Ошибка при вычислении остатка для {forecast_type}: {e}")
                continue
        return np.array(residuals)

    def _compute_classification_residual(self, row: pd.Series, forecast_type: str) -> Optional[float]:
        try:
            prob_yes = self._get_probability_yes(row, forecast_type)
            prob_no = self._get_probability_no(row, forecast_type)
            if prob_yes is None or prob_no is None:
                return None

            if forecast_type == 'win_draw_loss':
                if row.get('target_win_draw_loss_home_win', 0) == 1:
                    correct_prob = prob_yes
                elif row.get('target_win_draw_loss_draw', 0) == 1:
                    correct_prob = row.get('win_draw_loss_x', 0)
                elif row.get('target_win_draw_loss_away_win', 0) == 1:
                    correct_prob = prob_no
                else:
                    return None
            elif forecast_type == 'oz':
                if row.get('target_oz_both_score', 0) == 1:
                    correct_prob = prob_yes
                elif row.get('target_oz_not_both_score', 0) == 1:
                    correct_prob = prob_no
                else:
                    return None
            elif forecast_type == 'goal_home':
                if row.get('target_goal_home_yes', 0) == 1:
                    correct_prob = prob_yes
                elif row.get('target_goal_home_no', 0) == 1:
                    correct_prob = prob_no
                else:
                    return None
            elif forecast_type == 'goal_away':
                if row.get('target_goal_away_yes', 0) == 1:
                    correct_prob = prob_yes
                elif row.get('target_goal_away_no', 0) == 1:
                    correct_prob = prob_no
                else:
                    return None
            elif forecast_type in ['total', 'total_home', 'total_away']:
                if forecast_type == 'total':
                    if row.get('target_total_over', 0) == 1:
                        correct_prob = prob_yes
                    elif row.get('target_total_under', 0) == 1:
                        correct_prob = prob_no
                    else:
                        return None
                elif forecast_type == 'total_home':
                    if row.get('target_total_home_over', 0) == 1:
                        correct_prob = prob_yes
                    elif row.get('target_total_home_under', 0) == 1:
                        correct_prob = prob_no
                    else:
                        return None
                elif forecast_type == 'total_away':
                    if row.get('target_total_away_over', 0) == 1:
                        correct_prob = prob_yes
                    elif row.get('target_total_away_under', 0) == 1:
                        correct_prob = prob_no
                    else:
                        return None
            else:
                return None

            residual = 1 - correct_prob
            return max(residual, 0.01)
        except Exception as e:
            logger.debug(f"Ошибка в _compute_classification_residual: {e}")
            return None

    def _compute_regression_residual(self, row: pd.Series, forecast_type: str) -> Optional[float]:
        try:
            forecast_col = f'forecast_{forecast_type}'
            forecast_value = row.get(forecast_col)
            real_outcome = row.get('outcome', '')
            if forecast_value is None or not real_outcome:
                return None
            try:
                real_value = float(real_outcome)
            except (ValueError, TypeError):
                return None
            residual = abs(float(forecast_value) - real_value)
            return max(residual, 0.01)
        except Exception as e:
            logger.debug(f"Ошибка в _compute_regression_residual: {e}")
            return None

    def _get_probability_yes(self, row: Dict[str, Any], forecast_type: str) -> Optional[float]:
        mapping = {
            'win_draw_loss': 'win_draw_loss_home_win',
            'oz': 'oz_yes',
            'goal_home': 'goal_home_yes',
            'goal_away': 'goal_away_yes',
            'total': 'total_yes',
            'total_home': 'total_home_yes',
            'total_away': 'total_away_yes'
        }
        col = mapping.get(forecast_type)
        if col and col in row:
            val = row[col]
            return float(val) if pd.notna(val) else None
        return None

    def _get_probability_no(self, row: Dict[str, Any], forecast_type: str) -> Optional[float]:
        mapping = {
            'win_draw_loss': 'win_draw_loss_away_win',
            'oz': 'oz_no',
            'goal_home': 'goal_home_no',
            'goal_away': 'goal_away_no',
            'total': 'total_no',
            'total_home': 'total_home_no',
            'total_away': 'total_away_no'
        }
        col = mapping.get(forecast_type)
        if col and col in row:
            val = row[col]
            return float(val) if pd.notna(val) else None
        return None

    def predict_interval(self, prediction_row: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        if not self.is_fitted:
            raise ValueError("Модель не обучена. Вызовите fit() сначала.")
        results: Dict[str, Dict[str, Any]] = {}
        for forecast_type in self._get_forecast_types():
            try:
                quantile = self.quantiles.get(forecast_type, 0.1)
                if forecast_type in ['win_draw_loss', 'oz', 'goal_home', 'goal_away', 'total', 'total_home', 'total_away']:
                    result = self._predict_classification_interval(prediction_row, forecast_type, quantile)
                else:
                    result = self._predict_regression_interval(prediction_row, forecast_type, quantile)
                results[forecast_type] = result
            except Exception as e:
                logger.error(f"Ошибка при создании интервала для {forecast_type}: {e}")
                results[forecast_type] = {
                    'forecast': '',
                    'probability': 0.0,
                    'confidence': 0.0,
                    'lower_bound': None,
                    'upper_bound': None,
                    'uncertainty': None
                }
        return results

    def _predict_classification_interval(self, row: Dict[str, Any], forecast_type: str, quantile: float) -> Dict[str, Any]:
        if forecast_type == 'win_draw_loss':
            # Специальная обработка для win_draw_loss с тремя исходами
            prob_home = row.get('win_draw_loss_home_win')
            prob_draw = row.get('win_draw_loss_draw')
            prob_away = row.get('win_draw_loss_away_win')
            
            if prob_home is None or prob_draw is None or prob_away is None:
                return {
                    'forecast': '',
                    'probability': 0.0,
                    'confidence': 0.0,
                    'lower_bound': None,
                    'upper_bound': None,
                    'uncertainty': None
                }
            
            # Находим максимальную вероятность
            probs = [prob_home, prob_draw, prob_away]
            outcomes = ['home_win', 'draw', 'away_win']
            max_idx = probs.index(max(probs))
            max_prob = probs[max_idx]
            max_outcome = outcomes[max_idx]
            
            # Вычисляем интервалы (приводим к float для совместимости типов)
            max_prob_float = float(max_prob)
            quantile_float = float(quantile)
            lower_bound = max(0, max_prob_float - quantile_float)
            upper_bound = min(1, max_prob_float + quantile_float)
            uncertainty = upper_bound - lower_bound
            dynamic_confidence = max(0.5, min(0.99, self.confidence_level - uncertainty * 0.5))
            
            return {
                'forecast': self._format_classification_forecast(forecast_type, max_outcome),
                'probability': float(max_prob),
                'confidence': float(dynamic_confidence),
                'lower_bound': float(lower_bound),
                'upper_bound': float(upper_bound),
                'uncertainty': float(uncertainty)
            }
        else:
            # Обычная обработка для бинарных исходов
            prob_yes = self._get_probability_yes(row, forecast_type)
            prob_no = self._get_probability_no(row, forecast_type)
            if prob_yes is None or prob_no is None:
                return {
                    'forecast': '',
                    'probability': 0.0,
                    'confidence': 0.0,
                    'lower_bound': None,
                    'upper_bound': None,
                    'uncertainty': None
                }
            # Приводим к float для совместимости типов
            base_prob_yes = float(prob_yes)
            base_prob_no = float(prob_no)
            quantile_float = float(quantile)
            lower_yes = max(0, base_prob_yes - quantile_float)
            upper_yes = min(1, base_prob_yes + quantile_float)
            lower_no = max(0, base_prob_no - quantile_float)
            upper_no = min(1, base_prob_no + quantile_float)
            if base_prob_yes > base_prob_no:
                forecast = self._format_classification_forecast(forecast_type, 'yes')
                probability = base_prob_yes
                lower_bound = lower_yes
                upper_bound = upper_yes
            else:
                forecast = self._format_classification_forecast(forecast_type, 'no')
                probability = base_prob_no
                lower_bound = lower_no
                upper_bound = upper_no
            uncertainty = upper_bound - lower_bound
            dynamic_confidence = max(0.5, min(0.99, self.confidence_level - uncertainty * 0.5))
        return {
            'forecast': forecast,
            'probability': float(probability),
            'confidence': float(dynamic_confidence),
            'lower_bound': float(lower_bound),
            'upper_bound': float(upper_bound),
            'uncertainty': float(uncertainty)
        }

    def _predict_regression_interval(self, row: Dict[str, Any], forecast_type: str, quantile: float) -> Dict[str, Any]:
        forecast_col = f'forecast_{forecast_type}'
        forecast_value = row.get(forecast_col)
        if forecast_value is None:
            return {
                'forecast': '',
                'probability': 0.0,
                'confidence': 0.0,
                'lower_bound': None,
                'upper_bound': None,
                'uncertainty': None
            }
        forecast_value = float(forecast_value)
        lower_bound = forecast_value - quantile
        upper_bound = forecast_value + quantile
        uncertainty = quantile * 2
        dynamic_confidence = max(0.5, min(0.99, self.confidence_level - uncertainty * 0.1))
        return {
            'forecast': str(forecast_value),
            'probability': 0.5,
            'confidence': float(dynamic_confidence),
            'lower_bound': float(lower_bound),
            'upper_bound': float(upper_bound),
            'uncertainty': float(uncertainty)
        }

    def _format_classification_forecast(self, forecast_type: str, outcome: str) -> str:
        if forecast_type == 'win_draw_loss':
            if outcome == 'home_win':
                return 'п1'
            elif outcome == 'draw':
                return 'х'
            elif outcome == 'away_win':
                return 'п2'
            else:
                return 'п1' if outcome == 'yes' else 'п2'
        elif forecast_type == 'oz':
            return 'обе забьют - да' if outcome == 'yes' else 'обе забьют - нет'
        elif forecast_type == 'goal_home':
            return '1 забьет - да' if outcome == 'yes' else '1 забьет - нет'
        elif forecast_type == 'goal_away':
            return '2 забьет - да' if outcome == 'yes' else '2 забьет - нет'
        elif forecast_type == 'total':
            return 'тб' if outcome == 'yes' else 'тм'
        elif forecast_type == 'total_home':
            return 'ит1б' if outcome == 'yes' else 'ит1м'
        elif forecast_type == 'total_away':
            return 'ит2б' if outcome == 'yes' else 'ит2м'
        else:
            return outcome


class NeuralConformalAnalyzer:
    """Анализатор конформных прогнозов на основе нейронной сети."""

    def __init__(self, db_session, conformal_predictor: NeuralConformalPredictor):
        self.db_session = db_session
        self.conformal_predictor = conformal_predictor

    def analyze_prediction(self, prediction_row: Dict[str, Any]) -> Dict[str, Any]:
        try:
            conformal_results = self.conformal_predictor.predict_interval(prediction_row)
            conformal_results['match_id'] = prediction_row.get('match_id', 0)
            return conformal_results
        except Exception as e:
            logger.error(f"Ошибка при анализе прогноза: {e}")
            return {
                'match_id': prediction_row.get('match_id', 0),
                'error': str(e)
            }


