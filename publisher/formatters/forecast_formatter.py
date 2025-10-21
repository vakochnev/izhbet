"""
Форматтер для прогнозов (regular и quality).
"""

import logging
from datetime import date
from typing import List, Dict

from publisher.forecast_helpers import get_feature_description
from db.queries.statistics_cache import get_complete_statistics_cached as get_complete_statistics

logger = logging.getLogger(__name__)


def get_feature_sort_order(feature: int) -> int:
    """Возвращает порядок сортировки для feature."""
    order_map = {
        1: 1, 2: 2, 5: 3, 8: 4, 6: 5, 9: 6, 7: 7, 10: 8, 3: 9, 4: 10
    }
    return order_map.get(feature, 99)


def get_forecast_type_sort_order(forecast_type: str) -> int:
    """Возвращает порядок сортировки для forecast_type."""
    order_map = {
        'win_draw_loss': 1, 'oz': 2, 'total': 3, 'total_amount': 4,
        'total_home': 5, 'total_home_amount': 6, 'total_away': 7,
        'total_away_amount': 8, 'goal_home': 9, 'goal_away': 10
    }
    return order_map.get(forecast_type.lower() if forecast_type else '', 99)


class ForecastFormatter:
    """Класс для форматирования прогнозов в текстовый отчет."""
    
    def format_daily_forecasts_regular(self, forecasts_data: List[Dict], target_date: date) -> str:
        """
        Форматирует regular прогнозы в текстовый отчет.
        
        Args:
            forecasts_data: Список с прогнозами и информацией о матчах
            target_date: Дата для заголовка
            
        Returns:
            str: Отформатированный отчет
        """
        report = f"📊 ОБЫЧНЫЕ ПРОГНОЗЫ - {target_date.strftime('%d.%m.%Y')}\n\n"
        
        for item in forecasts_data:
            match = item['match']
            forecasts = item['forecasts']
            
            # Сортируем прогнозы по заданному порядку
            sorted_forecasts = sorted(forecasts, key=lambda x: get_feature_sort_order(x.get('feature', 0)))
            
            report += f"🆔 Match ID: {match['id']}\n"
            report += f"🏆 {match.get('sportName', 'Unknown')} - {match.get('championshipName', 'Unknown')}\n"
            report += f"⚽ {match.get('team_home_name', 'Unknown')} vs {match.get('team_away_name', 'Unknown')}\n"
            report += f"🕐 {match.get('gameData', '').strftime('%H:%M') if match.get('gameData') else 'TBD'}\n\n"
            report += f"📊 ДЕТАЛЬНАЯ СТАТИСТИКА ПРОГНОЗА:\n\n"
            
            for forecast in sorted_forecasts:
                feature = forecast.get('feature', 0)
                outcome = forecast.get('outcome', '')
                probability = forecast.get('probability', 0) * 100 if forecast.get('probability') else 0
                confidence = forecast.get('confidence', 0) * 100 if forecast.get('confidence') else 0
                uncertainty = forecast.get('uncertainty', 0) * 100 if forecast.get('uncertainty') else 0
                lower_bound = forecast.get('lower_bound', 0)
                upper_bound = forecast.get('upper_bound', 0)
                
                # Получаем расширенную статистику
                hist_stats = self._get_extended_statistics_for_feature(feature, outcome)
                feature_desc = get_feature_description(feature, outcome)
                
                report += f"• {feature_desc}: {outcome}\n"
                report += f"  🎯 Вероятность: {probability:.1f}% | 🔒 Уверенность: {confidence:.1f}% | 📊 Неопределенность: {uncertainty:.1f}%\n"
                report += f"  📈 Границы: [{lower_bound:.2f} - {upper_bound:.2f}]"
                
                if hist_stats:
                    report += f" | ⚖️ Калибровка: {hist_stats.get('calibration', 0):.1f}% | 🛡️ Стабильность: {hist_stats.get('stability', 0):.1f}%\n"
                    
                    acc_mark = "📊" if hist_stats.get('historical_accuracy', 0) >= 0.7 else "📉"
                    report += f"  {acc_mark} Историческая точность: {hist_stats.get('historical_correct', 0)}/{hist_stats.get('historical_total', 0)} ({hist_stats.get('historical_accuracy', 0)*100:.1f}%)"
                    
                    recent_mark = "🔥" if hist_stats.get('recent_accuracy', 0) >= 0.7 else "❄️"
                    report += f" | {recent_mark} Последние 10: {hist_stats.get('recent_correct', 0)}/10 ({hist_stats.get('recent_accuracy', 0)*100:.1f}%)\n"
                else:
                    report += "\n"
                
            report += "\n"
        
        return report
    
    def format_daily_forecasts_quality(self, forecasts_data: List[Dict], target_date: date) -> str:
        """
        Форматирует quality прогнозы в текстовый отчет.
        
        Args:
            forecasts_data: Список с прогнозами и информацией о матчах
            target_date: Дата для заголовка
            
        Returns:
            str: Отформатированный отчет
        """
        report = f"🌟 КАЧЕСТВЕННЫЕ ПРОГНОЗЫ - {target_date.strftime('%d.%m.%Y')}\n\n"
        
        for item in forecasts_data:
            match = item['match']
            forecasts = item['forecasts']
            
            # Сортируем прогнозы по заданному порядку (по forecast_type для quality)
            sorted_forecasts = sorted(forecasts, key=lambda x: get_forecast_type_sort_order(x.get('forecast_type', '')))
            
            report += f"🏆 {match.get('sportName', 'Unknown')} - {match.get('championshipName', 'Unknown')}\n"
            report += f"🆔 Match ID: {match['id']}\n"
            report += f"⚽ {match.get('team_home_name', 'Unknown')} vs {match.get('team_away_name', 'Unknown')}\n"
            report += f"🕐 {match.get('gameData', '').strftime('%H:%M') if match.get('gameData') else 'TBD'}\n\n"
            report += f"📊 ДЕТАЛЬНАЯ СТАТИСТИКА ПРОГНОЗА:\n\n"
            
            for stat in sorted_forecasts:
                forecast_type = stat.get('forecast_type', '')
                forecast_subtype = stat.get('forecast_subtype', '')
                accuracy = stat.get('prediction_accuracy', 0) * 100 if stat.get('prediction_accuracy') else 0
                
                # Получаем расширенную статистику
                from publisher.forecast_helpers import get_forecast_type_subtype
                hist_stats = self._get_historical_statistics(forecast_type, forecast_subtype)
                
                report += f"• {forecast_type}: {forecast_subtype}\n"
                report += f"  🎯 Точность модели: {accuracy:.1f}%\n"
                
                if hist_stats:
                    confidence = hist_stats.get('confidence', 0) * 100
                    uncertainty = hist_stats.get('uncertainty', 0) * 100
                    calibration = hist_stats.get('calibration', 0) * 100
                    stability = hist_stats.get('stability', 0) * 100
                    
                    report += f"  🔒 Уверенность: {confidence:.1f}% | 📊 Неопределенность: {uncertainty:.1f}%\n"
                    report += f"  ⚖️ Калибровка: {calibration:.1f}% | 🛡️ Стабильность: {stability:.1f}%\n"
                    
                    acc_mark = "📊" if hist_stats.get('historical_accuracy', 0) >= 0.7 else "📉"
                    report += f"  {acc_mark} Историческая точность: {hist_stats.get('historical_correct', 0)}/{hist_stats.get('historical_total', 0)} ({hist_stats.get('historical_accuracy', 0)*100:.1f}%)"
                    
                    recent_mark = "🔥" if hist_stats.get('recent_accuracy', 0) >= 0.7 else "❄️"
                    report += f" | {recent_mark} Последние 10: {hist_stats.get('recent_correct', 0)}/10 ({hist_stats.get('recent_accuracy', 0)*100:.1f}%)\n"
                
            report += "\n"
        
        return report
    
    def _get_extended_statistics_for_feature(self, feature: int, outcome: str = '') -> Dict:
        """
        Получает расширенную статистику для feature с учетом outcome.
        
        Args:
            feature: Код feature (1-10)
            outcome: Значение прогноза (например, 'п1', 'тб', 'обе забьют - да')
            
        Returns:
            Dict: Статистика
        """
        from publisher.forecast_helpers import FEATURE_TYPE_MAPPING, get_empty_statistics
        
        try:
            forecast_type = FEATURE_TYPE_MAPPING.get(feature, 'Unknown')
            if forecast_type == 'Unknown':
                return get_empty_statistics()
            
            # Нормализуем outcome для использования в БД (lowercase)
            # Преобразуем в строку, т.к. outcome может быть числом (float) для регрессионных моделей
            forecast_subtype = str(outcome).lower().strip() if outcome else ''
            
            stats = get_complete_statistics(forecast_type, forecast_subtype=forecast_subtype)
            
            return {
                'calibration': stats.get('calibration', 0.75) * 100,
                'stability': stats.get('stability', 0.80) * 100,
                'confidence': stats.get('confidence', 0.75) * 100,
                'uncertainty': stats.get('uncertainty', 0.25) * 100,
                'lower_bound': stats.get('lower_bound', 0.5),
                'upper_bound': stats.get('upper_bound', 0.9),
                'historical_correct': stats.get('historical_correct', 0),
                'historical_total': stats.get('historical_total', 0),
                'historical_accuracy': stats.get('historical_accuracy', 0.0),
                'recent_correct': stats.get('recent_correct', 0),
                'recent_accuracy': stats.get('recent_accuracy', 0.0)
            }
        except Exception as e:
            logger.error(f'Ошибка при получении статистики для feature {feature}, outcome {outcome}: {e}')
            from publisher.forecast_helpers import get_empty_statistics
            return get_empty_statistics()
    
    def _get_historical_statistics(self, forecast_type: str, forecast_subtype: str) -> Dict:
        """Получает историческую статистику для типа прогноза."""
        try:
            return get_complete_statistics(forecast_type, forecast_subtype)
        except Exception as e:
            logger.error(f'Ошибка при получении статистики для {forecast_type}/{forecast_subtype}: {e}')
            return {}

