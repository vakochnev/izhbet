"""
Форматтер для итогов матчей (regular и quality).
"""

import logging
from datetime import date
from typing import List, Dict

from publisher.forecast_helpers import get_feature_description
from db.queries.target import get_target_by_match_id
from core.prediction_validator import get_prediction_status_from_target

logger = logging.getLogger(__name__)


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


class OutcomeFormatter:
    """Класс для форматирования итогов матчей в текстовый отчет."""
    
    def format_daily_outcomes_regular(self, outcomes_data: List[Dict], target_date: date) -> str:
        """
        Форматирует regular итоги в текстовый отчет.
        
        Args:
            outcomes_data: Список с итогами и информацией о матчах
            target_date: Дата для заголовка
            
        Returns:
            str: Отформатированный отчет
        """
        report = f"🏁 ИТОГИ МАТЧЕЙ - {target_date.strftime('%d.%m.%Y')}\n\n"
        
        for item in outcomes_data:
            match = item['match']
            outcomes = item['outcomes']
            
            # Форматируем тип окончания матча
            result_type = _format_match_result_type(match.get('typeOutcome'))
            
            report += f"🆔 Match ID: {match['id']}\n"
            report += f"🏆 {match.get('sportName', 'Unknown')} - {match.get('championshipName', 'Unknown')}\n"
            report += f"⚽ {match.get('team_home_name', 'Unknown')} vs {match.get('team_away_name', 'Unknown')}\n"
            report += f"📊 Счет: {match.get('numOfHeadsHome', '-')} : {match.get('numOfHeadsAway', '-')}{result_type}\n\n"
            report += f"📋 ИТОГИ ПРОГНОЗОВ:\n\n"
            
            for outcome in outcomes:
                feature = outcome.get('feature', 0)
                outcome_value = outcome.get('outcome', '')
                
                # Определяем статус прогноза
                status = self._determine_prediction_status(feature, outcome_value, match['id'])
                feature_desc = get_feature_description(feature, outcome_value)
                
                report += f"{status} • {feature_desc}: {outcome_value}\n"
            
            report += "\n"
        
        return report
    
    def format_daily_outcomes_quality(self, outcomes_data: List[Dict], target_date: date) -> str:
        """
        Форматирует quality итоги в текстовый отчет.
        
        Args:
            outcomes_data: Список с итогами и информацией о матчах
            target_date: Дата для заголовка
            
        Returns:
            str: Отформатированный отчет
        """
        report = f"🏁 КАЧЕСТВЕННЫЕ ИТОГИ МАТЧЕЙ - {target_date.strftime('%d.%m.%Y')}\n\n"
        
        for item in outcomes_data:
            match = item['match']
            outcomes = item['outcomes']
            
            # Форматируем тип окончания матча
            result_type = _format_match_result_type(match.get('typeOutcome'))
            
            report += f"🏆 {match.get('sportName', 'Unknown')} - {match.get('championshipName', 'Unknown')}\n"
            report += f"🆔 Match ID: {match['id']}\n"
            report += f"⚽ {match.get('team_home_name', 'Unknown')} vs {match.get('team_away_name', 'Unknown')}\n"
            report += f"📊 Счет: {match.get('numOfHeadsHome', '-')} : {match.get('numOfHeadsAway', '-')}{result_type}\n\n"
            report += f"📋 ИТОГИ КАЧЕСТВЕННЫХ ПРОГНОЗОВ:\n\n"
            
            for stat in outcomes:
                forecast_type = stat.get('forecast_type', '')
                forecast_subtype = stat.get('forecast_subtype', '')
                prediction_correct = stat.get('prediction_correct', None)
                
                # Определяем статус
                if prediction_correct is None:
                    status = '⏳'
                elif prediction_correct:
                    status = '✅'
                else:
                    status = '❌'
                
                report += f"{status} • {forecast_type}: {forecast_subtype}\n"
            
            report += "\n"
        
        return report
    
    def _determine_prediction_status(self, feature: int, outcome: str, match_id: int) -> str:
        """
        Определяет статус прогноза на основе target из БД.
        
        Args:
            feature: Код feature (1-10)
            outcome: Прогноз из таблицы outcomes
            match_id: ID матча
            
        Returns:
            str: '✅' если прогноз правильный, '❌' если неправильный, '⏳' если матч не состоялся
        """
        try:
            target = get_target_by_match_id(match_id)
            return get_prediction_status_from_target(feature, outcome, target)
            
        except Exception as e:
            logger.error(f'Ошибка при определении статуса прогноза для feature {feature}, match {match_id}: {e}')
            return '❌'

