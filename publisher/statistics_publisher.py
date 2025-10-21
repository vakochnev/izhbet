# izhbet/publisher/statistics_publisher.py
"""
Публикатор прогнозов из таблицы statistics.
Упрощенный сервис для работы с качественными прогнозами.
"""

import logging
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Any
import pandas as pd

from db.models.outcome import Outcome
from db.models.prediction import Prediction
from db.queries.statistics import (
    get_statistics_for_today, get_statistics_for_date, get_statistics_for_period, get_all_statistics,
    get_predictions_for_today, get_all_predictions,
)
from db.queries.outcome import get_outcomes_for_date as get_outcomes_for_date_outcome, get_all_outcomes
from db.queries.match import get_matches_for_date, get_statistics_for_match
from db.queries.statistics_metrics import (
    get_historical_accuracy_regular,
    get_recent_accuracy,
    get_calibration,
    get_stability,
    get_confidence_bounds
)
from db.queries.statistics_cache import (
    get_complete_statistics_cached as get_complete_statistics,
    clear_statistics_cache,
    get_cache_info
)
from db.queries.target import get_target_by_match_id
from db.storage.publisher import save_conformal_report
from publisher.sending import Publisher
from publisher.conformal_sending import ConformalPublisher, ConformalDailyPublisher
from publisher.formatters import ForecastFormatter, OutcomeFormatter, ReportBuilder
from core.prediction_validator import get_prediction_status_from_target
from config import Session_pool


logger = logging.getLogger(__name__)


def get_feature_sort_order(feature: int) -> int:
    """
    Возвращает порядок сортировки для feature.
    
    Порядок:
    1. WIN_DRAW_LOSS (feature 1)
    2. OZ (feature 2)
    3. TOTAL (feature 5)
    4. TOTAL_AMOUNT (feature 8)
    5. TOTAL_HOME (feature 6)
    6. TOTAL_HOME_AMOUNT (feature 9)
    7. TOTAL_AWAY (feature 7)
    8. TOTAL_AWAY_AMOUNT (feature 10)
    9. GOAL_HOME (feature 3)
    10. GOAL_AWAY (feature 4)
    """
    order_map = {
        1: 1,   # WIN_DRAW_LOSS
        2: 2,   # OZ
        5: 3,   # TOTAL
        8: 4,   # TOTAL_AMOUNT
        6: 5,   # TOTAL_HOME
        9: 6,   # TOTAL_HOME_AMOUNT
        7: 7,   # TOTAL_AWAY
        10: 8,  # TOTAL_AWAY_AMOUNT
        3: 9,   # GOAL_HOME
        4: 10   # GOAL_AWAY
    }
    return order_map.get(feature, 99)


def get_forecast_type_sort_order(forecast_type: str) -> int:
    """
    Возвращает порядок сортировки для forecast_type (для quality отчетов).
    
    Порядок аналогичен get_feature_sort_order.
    """
    order_map = {
        'win_draw_loss': 1,
        'oz': 2,
        'total': 3,
        'total_amount': 4,
        'total_home': 5,
        'total_home_amount': 6,
        'total_away': 7,
        'total_away_amount': 8,
        'goal_home': 9,
        'goal_away': 10
    }
    return order_map.get(forecast_type.lower() if forecast_type else '', 99)


class StatisticsPublisher:
    """
    Публикатор прогнозов из таблицы statistics.
    
    Отвечает за:
    - Загрузку качественных прогнозов из таблицы statistics
    - Форматирование отчетов
    - Публикацию через различные платформы
    """
    
    def __init__(self):
        self.publishers: List[Publisher] = []
        self.conformal_publishers: List[ConformalPublisher] = []
        self.forecast_formatter = ForecastFormatter()
        self.outcome_formatter = OutcomeFormatter()
        self.report_builder = ReportBuilder()
        self._setup_publishers()
    
    def _setup_publishers(self) -> None:
        """Настраивает публикаторы."""
        logger.info('Настройка публикаторов для статистики')
        
        # Добавляем файловый публикатор
        self.conformal_publishers.append(
            ConformalDailyPublisher(file='results')
        )
        
        # Добавляем другие публикаторы при необходимости
        # self.publishers.append(TelegramPublisher(...))
        # self.publishers.append(VkPublisher(...))
        
        logger.info('Публикаторы настроены')
    
    def publish_today_forecasts_and_outcomes(self) -> bool:
        """
        Публикует прогнозы на сегодня и итоги вчера с разделением на regular и quality.
        Архитектура:
        - Выбираем матчи за вчера и сегодня из таблицы matches
        - Для каждого матча формируем прогнозы (outcomes + statistics) -> папка forecast
        - Для завершенных матчей формируем итоги (outcomes + statistics) -> папка outcome
        
        Returns:
            bool: True если публикация успешна, False иначе
        """
        logger.info('Публикация прогнозов на сегодня и итогов вчера (regular + quality)')
        
        try:
            today = datetime.now().date()
            yesterday = today - timedelta(days=1)
            
            # Загружаем матчи за вчера и сегодня
            matches_today = get_matches_for_date(today)
            matches_yesterday = get_matches_for_date(yesterday)
            
            logger.info(f'Найдено матчей на сегодня ({today}): {len(matches_today)}')
            logger.info(f'Найдено матчей за вчера ({yesterday}): {len(matches_yesterday)}')
            
            # 1. Формируем прогнозы на сегодня
            if len(matches_today) > 0:
                self._publish_forecasts_for_matches(matches_today, today)
            else:
                logger.warning(f'Нет матчей на {today} для формирования прогнозов')
            
            # 2. Формируем итоги за вчера
            if len(matches_yesterday) > 0:
                self._publish_outcomes_for_matches(matches_yesterday, yesterday)
            else:
                logger.warning(f'Нет матчей за {yesterday} для формирования итогов')
            
            # Логируем статистику кеша
            cache_info = get_cache_info()
            logger.info(f'Кеш статистики: {cache_info["hits"]} попаданий, {cache_info["misses"]} промахов, эффективность {cache_info["hit_rate"]*100:.1f}%')
            
            logger.info('Публикация прогнозов и итогов завершена')
            return True
            
        except Exception as e:
            logger.error(f'Ошибка при публикации прогнозов и итогов: {e}', exc_info=True)
            return False
    
    def _publish_forecasts_for_matches(self, matches: List[Dict], target_date: date) -> None:
        """
        Формирует и публикует прогнозы для списка матчей.
        
        Args:
            matches: Список матчей
            target_date: Дата для группировки файлов
        """
        logger.info(f'Формирование прогнозов для {len(matches)} матчей на {target_date}')
        
        # Группируем прогнозы по типам
        regular_forecasts = []
        quality_forecasts = []
        
        for match in matches:
            match_id = match['id']
            
            # Получаем regular прогнозы из таблицы outcomes
            regular_data = self._get_outcomes_for_match(match_id)
            if regular_data:
                regular_forecasts.append({'match': match, 'forecasts': regular_data})
            else:
                logger.warning(f'Нет regular прогнозов (outcomes) для матча ID {match_id} ({match.get("team_home_name")} vs {match.get("team_away_name")})')
            
            # Получаем quality прогнозы из таблицы statistics
            quality_data = get_statistics_for_match(match_id)
            if quality_data:
                quality_forecasts.append({'match': match, 'forecasts': quality_data})
            else:
                logger.warning(f'Нет quality прогнозов (statistics) для матча ID {match_id} ({match.get("team_home_name")} vs {match.get("team_away_name")})')
        
        # Публикуем regular прогнозы
        if regular_forecasts:
            self._publish_daily_forecasts_regular(regular_forecasts, target_date)
            logger.info(f'Опубликовано {len(regular_forecasts)} regular прогнозов на {target_date}')
        
        # Публикуем quality прогнозы
        if quality_forecasts:
            self._publish_daily_forecasts_quality(quality_forecasts, target_date)
            logger.info(f'Опубликовано {len(quality_forecasts)} quality прогнозов на {target_date}')
    
    def _publish_outcomes_for_matches(self, matches: List[Dict], target_date: date) -> None:
        """
        Формирует и публикует итоги для списка завершенных матчей.
        
        Args:
            matches: Список матчей
            target_date: Дата для группировки файлов
        """
        logger.info(f'Формирование итогов для {len(matches)} матчей за {target_date}')
        
        # Фильтруем только завершенные матчи
        completed_matches = [m for m in matches if m.get('typeOutcome') is not None]
        logger.info(f'Из них завершенных: {len(completed_matches)}')
        
        # Группируем итоги по типам
        regular_outcomes = []
        quality_outcomes = []
        
        for match in completed_matches:
            match_id = match['id']
            
            # Получаем regular итоги из таблицы outcomes
            regular_data = self._get_outcomes_for_match(match_id)
            if regular_data:
                regular_outcomes.append({'match': match, 'outcomes': regular_data})
            else:
                logger.warning(f'Нет regular итогов (outcomes) для завершенного матча ID {match_id} ({match.get("team_home_name")} vs {match.get("team_away_name")})')
            
            # Получаем quality итоги из таблицы statistics
            quality_data = get_statistics_for_match(match_id)
            if quality_data:
                quality_outcomes.append({'match': match, 'outcomes': quality_data})
            else:
                logger.warning(f'Нет quality итогов (statistics) для завершенного матча ID {match_id} ({match.get("team_home_name")} vs {match.get("team_away_name")})')
        
        # Публикуем regular итоги
        if regular_outcomes:
            self._publish_daily_outcomes_regular(regular_outcomes, target_date)
            logger.info(f'Опубликовано {len(regular_outcomes)} regular итогов за {target_date}')
        
        # Публикуем quality итоги
        if quality_outcomes:
            self._publish_daily_outcomes_quality(quality_outcomes, target_date)
            logger.info(f'Опубликовано {len(quality_outcomes)} quality итогов за {target_date}')
    
    def _get_outcomes_for_match(self, match_id: int) -> List[Dict]:
        """
        Получает outcomes для конкретного матча.
        
        Args:
            match_id: ID матча
            
        Returns:
            List[Dict]: Список outcomes
        """
        with Session_pool() as session:
            query = session.query(Outcome).filter(Outcome.match_id == match_id)
            result = query.all()
            return [row.to_dict() if hasattr(row, 'to_dict') else row.__dict__ for row in result]
    

    def _publish_daily_forecasts_regular(self, forecasts_data: List[Dict], target_date: date) -> None:
        """
        Публикует regular прогнозы (из outcomes) в файл.
        
        Args:
            forecasts_data: Список прогнозов с информацией о матчах
            target_date: Дата для файла
        """
        # Форматируем отчет через форматтер
        report = self.forecast_formatter.format_daily_forecasts_regular(forecasts_data, target_date)
        
        # Сохраняем отчет
        save_conformal_report(report, 'regular', target_date)
        logger.info(f'Regular прогнозы на {target_date} сохранены в файл')
    
    def _publish_daily_forecasts_quality(self, forecasts_data: List[Dict], target_date: date) -> None:
        """
        Публикует quality прогнозы (из statistics) в файл.
        
        Args:
            forecasts_data: Список прогнозов с информацией о матчах
            target_date: Дата для файла
        """
        # Форматируем отчет через форматтер
        report = self.forecast_formatter.format_daily_forecasts_quality(forecasts_data, target_date)
        
        # Сохраняем отчет
        save_conformal_report(report, 'quality', target_date)
        logger.info(f'Quality прогнозы на {target_date} сохранены в файл')
    
    def _publish_daily_outcomes_regular(self, outcomes_data: List[Dict], target_date: date) -> None:
        """
        Публикует regular итоги (из outcomes) в файл.
        
        Args:
            outcomes_data: Список итогов с информацией о матчах
            target_date: Дата для файла
        """
        report = f"🏁 ИТОГИ МАТЧЕЙ - {target_date.strftime('%d.%m.%Y')}\n\n"
        
        for item in outcomes_data:
            match = item['match']
            outcomes = item['outcomes']
            
            # Сортируем outcomes по заданному порядку
            sorted_outcomes = sorted(outcomes, key=lambda x: get_feature_sort_order(x.get('feature', 0)))
            
            home_goals = match.get('numOfHeadsHome', 'N/A')
            away_goals = match.get('numOfHeadsAway', 'N/A')
            
            # Форматируем тип окончания матча
            result_type = self._format_match_result_type(match.get('typeOutcome'))
            
            report += f"🆔 Match ID: {match['id']}\n"
            report += f"🏆 {match.get('sportName', 'Unknown')} - {match.get('championshipName', 'Unknown')}\n"
            report += f"⚽ {match.get('team_home_name', 'Unknown')} vs {match.get('team_away_name', 'Unknown')}\n"
            report += f"📊 Результат: {home_goals}:{away_goals}{result_type}\n"
            report += f"🕐 {match.get('gameData', '').strftime('%H:%M') if match.get('gameData') else 'TBD'}\n"
            
            for outcome in sorted_outcomes:
                feature = outcome.get('feature', 0)
                outcome_value = outcome.get('outcome', '')
                forecast_value = outcome.get('forecast', '')
                probability = outcome.get('probability', 0) * 100 if outcome.get('probability') else 0
                confidence = outcome.get('confidence', 0) * 100 if outcome.get('confidence') else 0
                uncertainty = outcome.get('uncertainty', 0) * 100 if outcome.get('uncertainty') else 0
                lower_bound = outcome.get('lower_bound', 0)
                upper_bound = outcome.get('upper_bound', 0)
                
                # Получаем расширенную статистику с учетом реального outcome (а не forecast)
                # outcome_value - это фактический результат прогноза из таблицы outcomes
                hist_stats = self._get_extended_statistics_for_feature(feature, outcome_value)
                
                # Определяем правильность прогноза
                # Используем outcome_value (категория), а не forecast_value (вероятность)
                status = self._determine_prediction_status(feature, outcome_value, match['id'])
                
                feature_desc = self._get_feature_description_from_outcome(feature, outcome_value)
                
                # Убрали дублирование outcome_value - оно уже включено в feature_desc
                report += f"{status} • {feature_desc}\n"
                report += f"  Прогноз: {forecast_value} | 🎯 Вероятность: {probability:.1f}% | 🔒 Уверенность: {confidence:.1f}% | 📊 Неопределенность: {uncertainty:.1f}%\n"
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
        
        # Сохраняем отчет
        save_conformal_report(report, 'regular_outcome', target_date)
        logger.info(f'Regular итоги за {target_date} сохранены в файл')
    
    def _publish_daily_outcomes_quality(self, outcomes_data: List[Dict], target_date: date) -> None:
        """
        Публикует quality итоги (из statistics) в файл.
        
        Args:
            outcomes_data: Список итогов с информацией о матчах
            target_date: Дата для файла
        """
        report = f"🏁 КАЧЕСТВЕННЫЕ ИТОГИ МАТЧЕЙ - {target_date.strftime('%d.%m.%Y')}\n\n"
        
        for item in outcomes_data:
            match = item['match']
            outcomes = item['outcomes']
            
            # Сортируем outcomes по заданному порядку (по forecast_type для quality)
            sorted_outcomes = sorted(outcomes, key=lambda x: get_forecast_type_sort_order(x.get('forecast_type', '')))
            
            home_goals = match.get('numOfHeadsHome', 'N/A')
            away_goals = match.get('numOfHeadsAway', 'N/A')
            
            # Форматируем тип окончания матча
            result_type = self._format_match_result_type(match.get('typeOutcome'))
            
            report += f"🆔 Match ID: {match['id']}\n"
            report += f"🏆 {match.get('sportName', 'Unknown')} - {match.get('championshipName', 'Unknown')}\n"
            report += f"⚽ {match.get('team_home_name', 'Unknown')} vs {match.get('team_away_name', 'Unknown')}\n"
            report += f"📊 Результат: {home_goals}:{away_goals}{result_type}\n"
            report += f"🕐 {match.get('gameData', '').strftime('%H:%M') if match.get('gameData') else 'TBD'}\n"
            
            for stat in sorted_outcomes:
                forecast_type = stat.get('forecast_type', '')
                forecast_subtype = stat.get('forecast_subtype', '')
                actual_result = stat.get('actual_result', '')
                prediction_correct = stat.get('prediction_correct', False)
                accuracy = stat.get('prediction_accuracy', 0) * 100 if stat.get('prediction_accuracy') else 0
                
                # Получаем расширенную статистику
                hist_stats = self._get_historical_statistics(forecast_type, forecast_subtype)
                
                # Определяем статус (иконка показывает правильность прогноза)
                status = "✅" if prediction_correct else "❌"
                
                # Убрали дублирование: result_icon больше не нужен
                # Статус показывается один раз в начале строки
                report += f"{status} • {forecast_type}: {forecast_subtype}\n"
                report += f"  🎯 Вероятность: {accuracy:.1f}%"
                
                if hist_stats:
                    report += f" | 🔒 Уверенность: {hist_stats.get('confidence', 0):.1f}% | 📊 Неопределенность: {hist_stats.get('uncertainty', 0):.1f}%\n"
                    report += f"  📈 Границы: [{hist_stats.get('lower_bound', 0):.2f} - {hist_stats.get('upper_bound', 0):.2f}]"
                    report += f" | ⚖️ Калибровка: {hist_stats.get('calibration', 0):.1f}% | 🛡️ Стабильность: {hist_stats.get('stability', 0):.1f}%\n"
                    
                    acc_mark = "📊" if hist_stats.get('historical_accuracy', 0) >= 0.7 else "📉"
                    report += f"  {acc_mark} Историческая точность: {hist_stats.get('historical_correct', 0)}/{hist_stats.get('historical_total', 0)} ({hist_stats.get('historical_accuracy', 0)*100:.1f}%)"
                    
                    recent_mark = "🔥" if hist_stats.get('recent_accuracy', 0) >= 0.7 else "❄️"
                    report += f" | {recent_mark} Последние 10: {hist_stats.get('recent_correct', 0)}/10 ({hist_stats.get('recent_accuracy', 0)*100:.1f}%)\n"
                else:
                    report += "\n"
            
            report += "\n"
        
        # Сохраняем отчет
        save_conformal_report(report, 'quality_outcome', target_date)
        logger.info(f'Quality итоги за {target_date} сохранены в файл')
    
    def _get_extended_statistics_for_feature(self, feature: int, outcome: str = '') -> Dict[str, Any]:
        """
        Получает расширенную статистику из БД для feature с учетом outcome.
        
        Args:
            feature: Код feature (1-10)
            outcome: Значение прогноза (например, 'п1', 'тб', 'обе забьют - да')
            
        Returns:
            Dict: Словарь с расширенной статистикой из реальных данных БД
        """
        try:
            # Маппинг feature -> forecast_type
            feature_types = {
                1: 'WIN_DRAW_LOSS',
                2: 'OZ',
                3: 'GOAL_HOME',
                4: 'GOAL_AWAY',
                5: 'TOTAL',
                6: 'TOTAL_HOME',
                7: 'TOTAL_AWAY',
                8: 'TOTAL_AMOUNT',
                9: 'TOTAL_HOME_AMOUNT',
                10: 'TOTAL_AWAY_AMOUNT'
            }
            
            forecast_type = feature_types.get(feature, 'Unknown')
            
            if forecast_type == 'Unknown':
                logger.warning(f'Неизвестный feature: {feature}')
                return self._get_empty_statistics()
            
            # Нормализуем outcome для использования в БД
            # forecast_type должен быть в lowercase
            # forecast_subtype нормализуется через _normalize_forecast_subtype в get_complete_statistics
            # Преобразуем в строку, т.к. outcome может быть числом (float) для регрессионных моделей
            forecast_subtype = str(outcome).strip() if outcome else ''
            
            # Получаем статистику для конкретного типа и подтипа прогноза
            # forecast_type передаем как есть (функция сама нормализует)
            stats = get_complete_statistics(forecast_type, forecast_subtype)
            
            # Преобразуем в формат, совместимый со старым кодом
            return {
                'calibration': stats.get('calibration', 0.75) * 100,  # В процентах
                'stability': stats.get('stability', 0.80) * 100,       # В процентах
                'confidence': stats.get('confidence', 0.75) * 100,     # В процентах
                'uncertainty': stats.get('uncertainty', 0.25) * 100,   # В процентах
                'lower_bound': stats.get('lower_bound', 0.5),
                'upper_bound': stats.get('upper_bound', 0.9),
                'historical_correct': stats.get('historical_correct', 0),
                'historical_total': stats.get('historical_total', 0),
                'historical_accuracy': stats.get('historical_accuracy', 0.0),
                'recent_correct': stats.get('recent_correct', 0),
                'recent_accuracy': stats.get('recent_accuracy', 0.0)
            }
            
        except Exception as e:
            logger.error(f'Ошибка при получении расширенной статистики для feature {feature}: {e}')
            return self._get_empty_statistics()
    
    def _format_match_result_type(self, type_outcome: Optional[str]) -> str:
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
    
    def _get_empty_statistics(self) -> Dict[str, Any]:
        """Возвращает пустую статистику при отсутствии данных."""
        return {
            'calibration': 75.0,
            'stability': 80.0,
            'confidence': 75.0,
            'uncertainty': 25.0,
            'lower_bound': 0.5,
            'upper_bound': 0.9,
            'historical_correct': 0,
            'historical_total': 0,
            'historical_accuracy': 0.0,
            'recent_correct': 0,
            'recent_accuracy': 0.0
        }

    def publish_today_forecasts(self) -> Dict[str, str]:
        """
        Публикует прогнозы на сегодня (качественные + обычные).
        
        Returns:
            Dict[str, str]: Результаты публикации
        """
        logger.info('Публикация прогнозов на сегодня (качественные + обычные)')
        
        try:
            results = {}
            
            # Загружаем качественные прогнозы на сегодня
            df_quality_today = get_statistics_for_today()
            
            if not df_quality_today.empty:
                # Форматируем отчет с качественными прогнозами
                quality_report = self._format_daily_quality_report(df_quality_today, datetime.now().date())
                
                # Публикуем качественные прогнозы
                self._publish_report('today_quality', quality_report)
                results['quality'] = quality_report
                logger.info('Качественные прогнозы на сегодня опубликованы')
            else:
                logger.warning('Нет качественных прогнозов на сегодня')
                results['quality'] = '❌ Нет качественных прогнозов на сегодня'
            
            # Загружаем обычные прогнозы на сегодня
            df_regular_today = get_predictions_for_today()
            
            if not df_regular_today.empty:
                # Форматируем отчет с обычными прогнозами
                regular_report = self._format_daily_regular_report(df_regular_today, datetime.now().date())
                
                # Публикуем обычные прогнозы
                self._publish_report('today_regular', regular_report)
                results['regular'] = regular_report
                logger.info('Обычные прогнозы на сегодня опубликованы')
            else:
                logger.warning('Нет обычных прогнозов на сегодня')
                results['regular'] = '❌ Нет обычных прогнозов на сегодня'
            
            logger.info('Прогнозы на сегодня опубликованы')
            return results
            
        except Exception as e:
            logger.error(f'Ошибка при публикации прогнозов на сегодня: {e}')
            return {'error': f'❌ Ошибка: {e}'}
    
    def publish_yesterday_outcomes(self) -> Dict[str, str]:
        """
        Публикует итоги вчерашних матчей (качественные + обычные).
        
        Returns:
            Dict[str, str]: Результаты публикации
        """
        logger.info('Публикация итогов вчерашних матчей (качественные + обычные)')
        
        try:
            # Загружаем итоги вчера
            yesterday = datetime.now() - timedelta(days=1)
            df_quality_yesterday = get_statistics_for_date(yesterday.date())
            df_regular_yesterday = get_outcomes_for_date_outcome(yesterday.date())
            
            if df_quality_yesterday.empty and df_regular_yesterday.empty:
                logger.warning('Нет итогов вчерашних матчей (ни качественных, ни обычных)')
                return {'yesterday': '❌ Нет итогов вчерашних матчей'}
            
            # Форматируем отчет
            yesterday_report = self._format_combined_outcome_report(df_quality_yesterday, df_regular_yesterday, 'вчера')
            
            # Публикуем через доступные платформы
            self._publish_report('yesterday', yesterday_report)
            
            logger.info('Итоги вчерашних матчей опубликованы')
            return {'yesterday': yesterday_report}
            
        except Exception as e:
            logger.error(f'Ошибка при публикации итогов вчерашних матчей: {e}')
            return {'yesterday': f'❌ Ошибка: {e}'}
    
    def publish_all_time_statistics(self, year: Optional[str] = None) -> bool:
        """
        Публикует прогнозы и итоги за весь период с разделением по дням.
        
        Args:
            year: Год для фильтрации (опционально)
            
        Returns:
            bool: True если публикация успешна, False иначе
        """
        logger.info(f'Публикация прогнозов и итогов за весь период {year or "все время"} с разделением по дням')
        
        try:
            # Загружаем качественные прогнозы за период
            if year:
                # Фильтруем по году
                start_date = datetime.strptime(f'{year}-01-01', '%Y-%m-%d').date()
                end_date = datetime.strptime(f'{year}-12-31', '%Y-%m-%d').date()
                df_quality_statistics = get_statistics_for_period(start_date, end_date)
            else:
                # За весь период - загружаем все данные
                df_quality_statistics = get_all_statistics()
            
            # Загружаем обычные прогнозы за весь период (из таблицы outcomes для прошедших матчей)
            df_regular_outcomes = get_all_outcomes()
            
            # Загружаем обычные прогнозы для будущих матчей (из таблицы predictions)
            df_regular_predictions = get_all_predictions()
            
            # Объединяем outcomes и predictions для полного набора regular прогнозов
            if not df_regular_predictions.empty:
                # Преобразуем predictions в формат outcomes
                df_regular_predictions_formatted = self._convert_predictions_to_outcomes_format(df_regular_predictions)
                
                # Исключаем матчи, которые уже есть в outcomes (чтобы избежать дублей)
                if not df_regular_outcomes.empty:
                    existing_match_ids = df_regular_outcomes['match_id'].unique()
                    df_regular_predictions_formatted = df_regular_predictions_formatted[
                        ~df_regular_predictions_formatted['match_id'].isin(existing_match_ids)
                    ]
                    logger.info(f'Исключено {len(df_regular_predictions) - len(df_regular_predictions_formatted)} predictions, которые уже есть в outcomes')
                
                # Объединяем с outcomes
                df_regular_outcomes = pd.concat([df_regular_outcomes, df_regular_predictions_formatted], ignore_index=True)
            
            if df_quality_statistics.empty and df_regular_outcomes.empty:
                logger.warning('Нет прогнозов за указанный период (ни качественных, ни обычных)')
                return True  # Не ошибка, просто нет данных
            
            # Публикуем прогнозы по дням
            self._publish_daily_reports(df_quality_statistics, df_regular_outcomes, year)
            
            # Публикуем итоги по дням (только для прошедших матчей)
            # Передаем regular outcomes и quality statistics в правильном порядке
            self._publish_daily_outcomes(df_regular_outcomes, df_quality_statistics, year)
            
            logger.info('Прогнозы и итоги за весь период опубликованы с разделением по дням')
            return True
            
        except Exception as e:
            logger.error(f'Ошибка при публикации прогнозов и итогов за весь период: {e}')
            return False
    
    def _format_forecast_report(self, df: pd.DataFrame, period: str) -> str:
        """
        Форматирует отчет по прогнозам.
        
        Args:
            df: DataFrame с прогнозами
            period: Период (сегодня, вчера, etc.)
            
        Returns:
            str: Отформатированный отчет
        """
        try:
            if df.empty:
                return f'❌ Нет прогнозов на {period}'
            
            report = f'📊 Прогнозы на {period}\n\n'
            
            # Группируем по матчам
            for match_id, group in df.groupby('match_id'):
                match_info = group.iloc[0]
                report += f'⚽ {match_info.get("team_home_name", "Домашняя")} vs {match_info.get("team_away_name", "Гостевая")}\n'
                report += f'📅 {match_info.get("match_date", "Дата")}\n'
                
                # Добавляем прогнозы
                for _, row in group.iterrows():
                    forecast_type = row.get('forecast_type', 'Неизвестно')
                    forecast_subtype = row.get('forecast_subtype', '')
                    probability = row.get('prediction_accuracy', 0)
                    
                    report += f'  • {forecast_type} {forecast_subtype}: {probability:.1%}\n'
                
                report += '\n'
            
            return report
            
        except Exception as e:
            logger.error(f'Ошибка при форматировании отчета по прогнозам: {e}')
            return f'❌ Ошибка форматирования: {e}'
    
    def _format_outcome_report(self, df: pd.DataFrame, period: str) -> str:
        """
        Форматирует отчет по итогам матчей.
        
        Args:
            df: DataFrame с итогами
            period: Период (сегодня, вчера, etc.)
            
        Returns:
            str: Отформатированный отчет
        """
        try:
            if df.empty:
                return f'❌ Нет итогов за {period}'
            
            report = f'📊 Итоги матчей за {period}\n\n'
            
            # Группируем по матчам
            for match_id, group in df.groupby('match_id'):
                match_info = group.iloc[0]
                report += f'⚽ {match_info.get("team_home_name", "Домашняя")} vs {match_info.get("team_away_name", "Гостевая")}\n'
                report += f'📅 {match_info.get("match_date", "Дата")}\n'
                report += f'🏆 Счет: {match_info.get("actual_value", "Неизвестно")}\n'
                
                # Добавляем результаты прогнозов
                correct_predictions = group[group['prediction_correct'] == True]
                total_predictions = len(group)
                correct_count = len(correct_predictions)
                
                report += f'✅ Правильных прогнозов: {correct_count}/{total_predictions}\n'
                
                if correct_count > 0:
                    report += '  Правильные прогнозы:\n'
                    for _, row in correct_predictions.iterrows():
                        forecast_type = row.get('forecast_type', 'Неизвестно')
                        forecast_subtype = row.get('forecast_subtype', '')
                        report += f'    • {forecast_type} {forecast_subtype}\n'
                
                report += '\n'
            
            return report
            
        except Exception as e:
            logger.error(f'Ошибка при форматировании отчета по итогам: {e}')
            return f'❌ Ошибка форматирования: {e}'
    
    def _publish_report(self, report_type: str, content: str) -> None:
        """
        Публикует отчет через доступные платформы.
        
        Args:
            report_type: Тип отчета (today, yesterday)
            content: Содержимое отчета
        """
        try:
            logger.info(f'Публикация отчета {report_type} через доступные платформы')
            
            # Определяем тип папки в зависимости от типа отчета
            if report_type in ['today', 'today_quality', 'today_regular']:
                folder_type = 'forecasts'
            else:
                folder_type = 'outcomes'
            
            # Публикуем через конформные публикаторы
            for publisher in self.conformal_publishers:
                try:
                    publisher.publish({report_type: content, 'folder_type': folder_type})
                    logger.info(f'Отчет {report_type} опубликован через {type(publisher).__name__}')
                except Exception as pub_error:
                    logger.error(f'Ошибка публикации {report_type} через {type(publisher).__name__}: {pub_error}')
            
            # Публикуем через обычные публикаторы
            for publisher in self.publishers:
                try:
                    publisher.publish(content)
                    logger.info(f'Отчет {report_type} опубликован через {type(publisher).__name__}')
                except Exception as pub_error:
                    logger.error(f'Ошибка публикации {report_type} через {type(publisher).__name__}: {pub_error}')
            
            logger.info(f'Публикация отчета {report_type} завершена')
            
        except Exception as e:
            logger.error(f'Ошибка при публикации отчета {report_type}: {e}')
    
    def _format_all_time_report(self, df: pd.DataFrame, year: Optional[str] = None) -> str:
        """
        Форматирует отчет по статистике за весь период.
        
        Args:
            df: DataFrame со статистикой
            year: Год для фильтрации (опционально)
            
        Returns:
            str: Отформатированный отчет
        """
        try:
            if df.empty:
                return f'❌ Нет данных в статистике за {year or "весь период"}'
            
            period_title = f'за {year}' if year else 'за весь период'
            report = f'📊 Статистика прогнозов {period_title}\n\n'
            
            # Общая статистика
            total_predictions = len(df)
            correct_predictions = len(df[df['prediction_correct'] == True])
            accuracy = (correct_predictions / total_predictions * 100) if total_predictions > 0 else 0
            
            report += f'📈 Общая статистика:\n'
            report += f'  • Всего прогнозов: {total_predictions}\n'
            report += f'  • Правильных прогнозов: {correct_predictions}\n'
            report += f'  • Точность: {accuracy:.1f}%\n\n'
            
            # Статистика по типам прогнозов
            forecast_types = df['forecast_type'].value_counts()
            report += f'📊 Статистика по типам прогнозов:\n'
            for forecast_type, count in forecast_types.items():
                type_df = df[df['forecast_type'] == forecast_type]
                type_correct = len(type_df[type_df['prediction_correct'] == True])
                type_accuracy = (type_correct / count * 100) if count > 0 else 0
                report += f'  • {forecast_type}: {type_correct}/{count} ({type_accuracy:.1f}%)\n'
            
            report += '\n'
            
            # Топ-10 лучших матчей по точности
            if total_predictions > 0:
                # Группируем по матчам и считаем точность
                match_stats = df.groupby('match_id').agg({
                    'prediction_correct': ['count', 'sum'],
                    'team_home_name': 'first',
                    'team_away_name': 'first',
                    'match_date': 'first'
                }).round(2)
                
                match_stats.columns = ['total', 'correct', 'home_team', 'away_team', 'date']
                match_stats['accuracy'] = (match_stats['correct'] / match_stats['total'] * 100).round(1)
                
                # Берем топ-10 по точности
                top_matches = match_stats.sort_values('accuracy', ascending=False).head(10)
                
                report += f'🏆 Топ-10 матчей по точности прогнозов:\n'
                for match_id, row in top_matches.iterrows():
                    report += f'  • {row["home_team"]} vs {row["away_team"]} ({row["date"]})\n'
                    report += f'    Точность: {row["accuracy"]:.1f}% ({int(row["correct"])}/{int(row["total"])})\n'
            
            return report
            
        except Exception as e:
            logger.error(f'Ошибка при форматировании отчета по статистике: {e}')
            return f'❌ Ошибка форматирования: {e}'
    
    def _publish_all_time_report(self, content: str, year: Optional[str] = None) -> None:
        """
        Публикует отчет по статистике за весь период через доступные платформы.
        
        Args:
            content: Содержимое отчета
            year: Год для фильтрации (опционально)
        """
        try:
            logger.info(f'Публикация отчета по статистике за {year or "весь период"} через доступные платформы')
            
            # Разделяем отчет на прогнозы и итоги
            forecasts_content, outcomes_content = self._split_report_content(content)
            
            # Публикуем прогнозы
            if forecasts_content:
                message_forecasts = {
                    'forecasts': forecasts_content,
                    'date': datetime.now().strftime('%Y-%m-%d')
                }
                
                for publisher in self.conformal_publishers:
                    try:
                        publisher.publish(message_forecasts)
                        logger.info(f'Отчет по прогнозам опубликован через {type(publisher).__name__}')
                    except Exception as pub_error:
                        logger.error(f'Ошибка публикации прогнозов через {type(publisher).__name__}: {pub_error}')
            
            # Публикуем итоги
            if outcomes_content:
                message_outcomes = {
                    'outcomes': outcomes_content,
                    'date': datetime.now().strftime('%Y-%m-%d')
                }
                
                for publisher in self.conformal_publishers:
                    try:
                        publisher.publish(message_outcomes)
                        logger.info(f'Отчет по итогам опубликован через {type(publisher).__name__}')
                    except Exception as pub_error:
                        logger.error(f'Ошибка публикации итогов через {type(publisher).__name__}: {pub_error}')
            
            logger.info(f'Публикация отчета по статистике завершена')
            
        except Exception as e:
            logger.error(f'Ошибка при публикации отчета по статистике: {e}')
    
    def _split_report_content(self, content: str) -> tuple[str, str]:
        """
        Разделяет отчет на прогнозы и итоги.
        
        Args:
            content: Полный отчет
            
        Returns:
            tuple[str, str]: (прогнозы, итоги)
        """
        try:
            lines = content.split('\n')
            forecasts_lines = []
            outcomes_lines = []
            
            current_section = None
            
            for line in lines:
                if '📊 Статистика прогнозов' in line:
                    # Заголовок - добавляем в оба раздела
                    forecasts_lines.append(line)
                    outcomes_lines.append(line)
                elif '📈 Общая статистика:' in line:
                    # Общая статистика - добавляем в оба раздела
                    forecasts_lines.append(line)
                    outcomes_lines.append(line)
                elif '📊 Статистика по типам качественных прогнозов:' in line:
                    # Статистика по типам - только в прогнозы
                    current_section = 'forecasts'
                    forecasts_lines.append(line)
                elif '🏆 Статистика по турнирам' in line:
                    # Статистика по турнирам - только в итоги
                    current_section = 'outcomes'
                    outcomes_lines.append(line)
                elif line.strip() == '':
                    # Пустая строка - добавляем в текущий раздел
                    if current_section == 'forecasts':
                        forecasts_lines.append(line)
                    elif current_section == 'outcomes':
                        outcomes_lines.append(line)
                    else:
                        forecasts_lines.append(line)
                        outcomes_lines.append(line)
                else:
                    # Обычная строка - добавляем в текущий раздел
                    if current_section == 'forecasts':
                        forecasts_lines.append(line)
                    elif current_section == 'outcomes':
                        outcomes_lines.append(line)
                    else:
                        forecasts_lines.append(line)
                        outcomes_lines.append(line)
            
            forecasts_content = '\n'.join(forecasts_lines)
            outcomes_content = '\n'.join(outcomes_lines)
            
            return forecasts_content, outcomes_content
            
        except Exception as e:
            logger.error(f'Ошибка при разделении отчета: {e}')
            return content, content
    
    def _determine_file_date(self, df_quality: pd.DataFrame, df_regular: pd.DataFrame, year: Optional[str] = None) -> datetime:
        """
        Определяет дату для файла на основе данных.
        
        Args:
            df_quality: DataFrame с качественными прогнозами
            df_regular: DataFrame с обычными прогнозами
            year: Год для фильтрации (опционально)
            
        Returns:
            datetime: Дата для файла
        """
        try:
            if year:
                # Для конкретного года используем конец года
                return datetime.strptime(f'{year}-12-31', '%Y-%m-%d')
            
            # Для всего периода ищем последнюю дату в данных
            latest_date = None
            
            # Проверяем качественные прогнозы
            if not df_quality.empty and 'match_date' in df_quality.columns:
                quality_dates = pd.to_datetime(df_quality['match_date'], errors='coerce')
                if not quality_dates.isna().all():
                    latest_quality = quality_dates.max()
                    if pd.notna(latest_quality):
                        latest_date = latest_quality
            
            # Проверяем обычные прогнозы
            if not df_regular.empty and 'gameData' in df_regular.columns:
                regular_dates = pd.to_datetime(df_regular['gameData'], errors='coerce')
                if not regular_dates.isna().all():
                    latest_regular = regular_dates.max()
                    if pd.notna(latest_regular):
                        if latest_date is None or latest_regular > latest_date:
                            latest_date = latest_regular
            
            # Если нашли дату в данных, используем её
            if latest_date is not None:
                return latest_date.to_pydatetime()
            
            # Иначе используем текущую дату
            return datetime.now()
            
        except Exception as e:
            logger.error(f'Ошибка при определении даты файла: {e}')
            return datetime.now()
    
    def _convert_predictions_to_outcomes_format(self, df_predictions: pd.DataFrame) -> pd.DataFrame:
        """
        Преобразует данные из таблицы predictions в формат таблицы outcomes.
        
        Args:
            df_predictions: DataFrame из таблицы predictions
            
        Returns:
            pd.DataFrame: DataFrame в формате outcomes
        """
        try:
            outcomes_list = []
            
            for _, row in df_predictions.iterrows():
                match_id = row['match_id']
                
                # WIN_DRAW_LOSS (feature 1) - берем только самый вероятный исход
                win_prob = row.get('win_draw_loss_home_win', 0)
                draw_prob = row.get('win_draw_loss_draw', 0) 
                away_prob = row.get('win_draw_loss_away_win', 0)
                
                max_prob = max(win_prob, draw_prob, away_prob)
                if max_prob == win_prob:
                    outcome = 'п1'
                    forecast = win_prob
                elif max_prob == draw_prob:
                    outcome = 'х'
                    forecast = draw_prob
                else:
                    outcome = 'п2'
                    forecast = away_prob
                
                if pd.notna(forecast) and forecast > 0:
                    forecast_float = float(forecast)
                    outcomes_list.append({
                        'match_id': match_id,
                        'feature': 1,
                        'forecast': forecast_float,
                        'outcome': outcome,
                        'probability': forecast_float,
                        'confidence': 0.5,
                        'uncertainty': 1.0 - forecast_float,
                        'lower_bound': max(0, forecast_float - 0.1),
                        'upper_bound': min(1, forecast_float + 0.1),
                        'gameData': row['gameData'],
                        'team_home_name': row['team_home_name'],
                        'team_away_name': row['team_away_name'],
                        'championshipName': row['championshipName'],
                        'sportName': row['sportName'],
                        'numOfHeadsHome': row.get('numOfHeadsHome', 0),
                        'numOfHeadsAway': row.get('numOfHeadsAway', 0)
                    })
                
                # OZ (feature 2) - берем более вероятный исход
                oz_yes = row.get('oz_yes', 0)
                oz_no = row.get('oz_no', 0)
                
                if oz_yes > oz_no and pd.notna(oz_yes) and oz_yes > 0:
                    oz_yes_float = float(oz_yes)
                    outcomes_list.append({
                        'match_id': match_id,
                        'feature': 2,
                        'forecast': oz_yes_float,
                        'outcome': 'обе забьют - да',
                        'probability': oz_yes_float,
                        'confidence': 0.5,
                        'uncertainty': 1.0 - oz_yes_float,
                        'lower_bound': max(0, oz_yes_float - 0.1),
                        'upper_bound': min(1, oz_yes_float + 0.1),
                        'gameData': row['gameData'],
                        'team_home_name': row['team_home_name'],
                        'team_away_name': row['team_away_name'],
                        'championshipName': row['championshipName'],
                        'sportName': row['sportName'],
                        'numOfHeadsHome': row.get('numOfHeadsHome', 0),
                        'numOfHeadsAway': row.get('numOfHeadsAway', 0)
                    })
                elif oz_no > oz_yes and pd.notna(oz_no) and oz_no > 0:
                    oz_no_float = float(oz_no)
                    outcomes_list.append({
                        'match_id': match_id,
                        'feature': 2,
                        'forecast': oz_no_float,
                        'outcome': 'обе забьют - нет',
                        'probability': oz_no_float,
                        'confidence': 0.5,
                        'uncertainty': 1.0 - oz_no_float,
                        'lower_bound': max(0, oz_no_float - 0.1),
                        'upper_bound': min(1, oz_no_float + 0.1),
                        'gameData': row['gameData'],
                        'team_home_name': row['team_home_name'],
                        'team_away_name': row['team_away_name'],
                        'championshipName': row['championshipName'],
                        'sportName': row['sportName'],
                        'numOfHeadsHome': row.get('numOfHeadsHome', 0),
                        'numOfHeadsAway': row.get('numOfHeadsAway', 0)
                    })
                
                # TOTAL (feature 5)
                total_yes = row.get('total_yes', 0)
                total_no = row.get('total_no', 0)
                
                if total_yes > total_no and pd.notna(total_yes) and total_yes > 0:
                    total_yes_float = float(total_yes)
                    outcomes_list.append({
                        'match_id': match_id,
                        'feature': 5,
                        'forecast': total_yes_float,
                        'outcome': 'тб',
                        'probability': total_yes_float,
                        'confidence': 0.5,
                        'uncertainty': 1.0 - total_yes_float,
                        'lower_bound': max(0, total_yes_float - 0.1),
                        'upper_bound': min(1, total_yes_float + 0.1),
                        'gameData': row['gameData'],
                        'team_home_name': row['team_home_name'],
                        'team_away_name': row['team_away_name'],
                        'championshipName': row['championshipName'],
                        'sportName': row['sportName'],
                        'numOfHeadsHome': row.get('numOfHeadsHome', 0),
                        'numOfHeadsAway': row.get('numOfHeadsAway', 0)
                    })
                elif total_no > total_yes and pd.notna(total_no) and total_no > 0:
                    total_no_float = float(total_no)
                    outcomes_list.append({
                        'match_id': match_id,
                        'feature': 5,
                        'forecast': total_no_float,
                        'outcome': 'тм',
                        'probability': total_no_float,
                        'confidence': 0.5,
                        'uncertainty': 1.0 - total_no_float,
                        'lower_bound': max(0, total_no_float - 0.1),
                        'upper_bound': min(1, total_no_float + 0.1),
                        'gameData': row['gameData'],
                        'team_home_name': row['team_home_name'],
                        'team_away_name': row['team_away_name'],
                        'championshipName': row['championshipName'],
                        'sportName': row['sportName'],
                        'numOfHeadsHome': row.get('numOfHeadsHome', 0),
                        'numOfHeadsAway': row.get('numOfHeadsAway', 0)
                    })
                
            return pd.DataFrame(outcomes_list)
            
        except Exception as e:
            logger.error(f'Ошибка при преобразовании predictions в формат outcomes: {e}')
            return pd.DataFrame()
    
    def _publish_daily_reports(self, df_quality: pd.DataFrame, df_regular: pd.DataFrame, year: Optional[str] = None) -> None:
        """
        Публикует отчеты по дням с разделением на качественные и обычные прогнозы.
        
        Args:
            df_quality: DataFrame с качественными прогнозами (из таблицы statistics)
            df_regular: DataFrame с обычными прогнозами (из таблицы outcomes)
            year: Год для фильтрации (опционально)
        """
        try:
            logger.info('Публикация отчетов по дням с разделением на качественные и обычные')
            
            # Обрабатываем качественные прогнозы
            if not df_quality.empty:
                df_quality['match_date'] = pd.to_datetime(df_quality['match_date'], errors='coerce')
                quality_dates = df_quality['match_date'].dt.date.dropna().unique()
                
                for date in sorted(quality_dates):
                    day_quality = df_quality[df_quality['match_date'].dt.date == date]
                    self._publish_daily_quality_report(day_quality, date)
            
            # Обрабатываем обычные прогнозы (из таблицы outcomes)
            if not df_regular.empty:
                df_regular['gameData'] = pd.to_datetime(df_regular['gameData'], errors='coerce')
                regular_dates = df_regular['gameData'].dt.date.dropna().unique()
                
                for date in sorted(regular_dates):
                    day_regular = df_regular[df_regular['gameData'].dt.date == date]
                    self._publish_daily_regular_report(day_regular, date)
            
            logger.info('Отчеты по дням опубликованы')
            
        except Exception as e:
            logger.error(f'Ошибка при публикации отчетов по дням: {e}')
    
    def _publish_daily_outcomes(self, df_regular: pd.DataFrame, df_quality: pd.DataFrame, year: Optional[str] = None) -> None:
        """
        Публикует итоги по дням с разделением на качественные и обычные итоги.
        
        Args:
            df_regular: DataFrame с обычными прогнозами (из таблицы outcomes)
            df_quality: DataFrame с качественными прогнозами (из таблицы statistics)
            year: Год для фильтрации (опционально)
        """
        try:
            logger.info('Публикация итогов по дням с разделением на качественные и обычные')
            
            today = datetime.now().date()
            
            # Обрабатываем качественные итоги (только прошедшие матчи)
            if not df_quality.empty:
                df_quality['match_date'] = pd.to_datetime(df_quality['match_date'], errors='coerce')
                # Фильтруем только прошедшие матчи
                df_quality_past = df_quality[df_quality['match_date'].dt.date < today]
                quality_dates = df_quality_past['match_date'].dt.date.dropna().unique()
                
                for date in sorted(quality_dates):
                    day_quality = df_quality_past[df_quality_past['match_date'].dt.date == date]
                    self._publish_daily_quality_outcome_report(day_quality, date)
            
            # Обрабатываем обычные итоги (только прошедшие матчи)
            if not df_regular.empty:
                df_regular['gameData'] = pd.to_datetime(df_regular['gameData'], errors='coerce')
                # Фильтруем только прошедшие матчи
                df_regular_past = df_regular[df_regular['gameData'].dt.date < today]
                regular_dates = df_regular_past['gameData'].dt.date.dropna().unique()
                
                for date in sorted(regular_dates):
                    day_regular = df_regular_past[df_regular_past['gameData'].dt.date == date]
                    self._publish_daily_regular_outcome_report(day_regular, date)
            
            logger.info('Итоги по дням опубликованы')
            
        except Exception as e:
            logger.error(f'Ошибка при публикации итогов по дням: {e}')
    
    def _publish_daily_quality_report(self, df_day: pd.DataFrame, date: datetime.date) -> None:
        """
        Публикует качественный отчет за конкретный день.
        
        Args:
            df_day: DataFrame с данными за день
            date: Дата отчета
        """
        try:
            logger.info(f'Публикация качественного отчета за {date}')
            
            # Форматируем отчет
            report = self._format_daily_quality_report(df_day, date)
            
            if report:
                # Создаем сообщение для публикации
                message = {
                    'daily_quality': report,
                    'date': date.strftime('%Y-%m-%d'),
                    'report_type': 'quality'
                }
                
                # Публикуем через конформные публикаторы
                for publisher in self.conformal_publishers:
                    try:
                        publisher.publish(message)
                        logger.info(f'Качественный отчет за {date} опубликован через {type(publisher).__name__}')
                    except Exception as pub_error:
                        logger.error(f'Ошибка публикации качественного отчета за {date} через {type(publisher).__name__}: {pub_error}')
            
        except Exception as e:
            logger.error(f'Ошибка при публикации качественного отчета за {date}: {e}')
    
    def _publish_daily_regular_report(self, df_day: pd.DataFrame, date: datetime.date) -> None:
        """
        Публикует обычный отчет за конкретный день.
        
        Args:
            df_day: DataFrame с данными за день (из таблицы outcomes)
            date: Дата отчета
        """
        try:
            logger.info(f'Публикация обычного отчета за {date}')
            
            # Форматируем отчет для прогнозов из outcomes БЕЗ иконок статуса
            report = self._format_daily_regular_forecasts_from_outcomes(df_day, date)
            
            if report:
                # Создаем сообщение для публикации
                message = {
                    'daily_regular': report,
                    'date': date.strftime('%Y-%m-%d'),
                    'report_type': 'regular'
                }
                
                # Публикуем через конформные публикаторы
                for publisher in self.conformal_publishers:
                    try:
                        publisher.publish(message)
                        logger.info(f'Обычный отчет за {date} опубликован через {type(publisher).__name__}')
                    except Exception as pub_error:
                        logger.error(f'Ошибка публикации обычного отчета за {date} через {type(publisher).__name__}: {pub_error}')
            
        except Exception as e:
            logger.error(f'Ошибка при публикации обычного отчета за {date}: {e}')
    
    def _publish_daily_quality_outcome_report(self, df_day: pd.DataFrame, date: datetime.date) -> None:
        """
        Публикует качественный отчет по итогам за конкретный день.
        
        Args:
            df_day: DataFrame с данными за день
            date: Дата отчета
        """
        try:
            logger.info(f'Публикация качественного отчета по итогам за {date}')
            
            # Форматируем отчет (данные из таблицы statistics)
            report = self._format_daily_quality_outcome_report(df_day, date)
            
            if report:
                # Создаем сообщение для публикации
                message = {
                    'daily_quality_outcome': report,
                    'date': date.strftime('%Y-%m-%d'),
                    'report_type': 'quality_outcome'
                }
                
                # Публикуем через конформные публикаторы
                for publisher in self.conformal_publishers:
                    try:
                        publisher.publish(message)
                        logger.info(f'Качественный отчет по итогам за {date} опубликован через {type(publisher).__name__}')
                    except Exception as pub_error:
                        logger.error(f'Ошибка публикации качественного отчета по итогам за {date} через {type(publisher).__name__}: {pub_error}')
            
        except Exception as e:
            logger.error(f'Ошибка при публикации качественного отчета по итогам за {date}: {e}')
    
    def _publish_daily_regular_outcome_report(self, df_day: pd.DataFrame, date: datetime.date) -> None:
        """
        Публикует обычный отчет по итогам за конкретный день.
        
        Args:
            df_day: DataFrame с данными за день
            date: Дата отчета
        """
        try:
            logger.info(f'Публикация обычного отчета по итогам за {date}')
            
            # Форматируем отчет (данные из таблицы outcomes)
            report = self._format_daily_regular_outcomes_report(df_day, date)
            
            if report:
                # Создаем сообщение для публикации
                message = {
                    'daily_regular_outcome': report,
                    'date': date.strftime('%Y-%m-%d'),
                    'report_type': 'regular_outcome'
                }
                
                # Публикуем через конформные публикаторы
                for publisher in self.conformal_publishers:
                    try:
                        publisher.publish(message)
                        logger.info(f'Обычный отчет по итогам за {date} опубликован через {type(publisher).__name__}')
                    except Exception as pub_error:
                        logger.error(f'Ошибка публикации обычного отчета по итогам за {date} через {type(publisher).__name__}: {pub_error}')
            
        except Exception as e:
            logger.error(f'Ошибка при публикации обычного отчета по итогам за {date}: {e}')
    
    def _format_daily_regular_outcomes_report(self, df_day: pd.DataFrame, date: datetime.date) -> str:
        """
        Форматирует обычный отчет за день на основе данных из таблицы outcomes с расширенной статистикой.
        
        Args:
            df_day: DataFrame с данными за день из таблицы outcomes
            date: Дата отчета
            
        Returns:
            str: Отформатированный отчет
        """
        try:
            if df_day.empty:
                return ''
            
            report = f'📊 ОБЫЧНЫЕ ПРОГНОЗЫ - {date.strftime("%d.%m.%Y")}\n\n'
            
            # Группируем по матчам
            for match_id, match_group in df_day.groupby('match_id'):
                match_info = match_group.iloc[0]
                
                # Заголовок матча с match_id
                report += f'🆔 Match ID: {match_id}\n'
                report += f'🏆 {match_info.get("sportName", "Soccer")} - {match_info.get("championshipName", "Unknown")}\n'
                report += f'⚽ {match_info.get("team_home_name", "Home")} vs {match_info.get("team_away_name", "Away")}\n'
                
                # Время матча
                if 'gameData' in match_info and pd.notna(match_info['gameData']):
                    match_time = pd.to_datetime(match_info['gameData'])
                    report += f'🕐 {match_time.strftime("%H:%M")}\n'
                
                report += f'\n📊 ДЕТАЛЬНАЯ СТАТИСТИКА ПРОГНОЗА:\n\n'
                
                # Получаем данные регрессии из predictions для этого матча (один раз на матч)
                regression_data = self._get_regression_data_for_match(match_id)
                
                # Прогнозы и их результаты
                for _, outcome_row in match_group.iterrows():
                    feature = int(outcome_row['feature']) if pd.notna(outcome_row['feature']) else 0
                    forecast = outcome_row.get('forecast', 'Unknown')
                    outcome = outcome_row.get('outcome', 'Unknown')
                    probability = outcome_row.get('probability', 0)
                    confidence = outcome_row.get('confidence', 0)
                    uncertainty = outcome_row.get('uncertainty', 0)
                    lower_bound = outcome_row.get('lower_bound', 0)
                    upper_bound = outcome_row.get('upper_bound', 0)
                    
                    # Используем реальное описание прогноза из поля outcome
                    feature_description = self._get_feature_description_from_outcome(feature, outcome)
                    
                    # Определяем правильность прогноза на основе результата матча
                    status_icon = self._determine_prediction_status(feature, outcome, match_id)
                    
                    # Получаем историческую статистику для regular прогнозов
                    forecast_type, forecast_subtype = self._get_forecast_type_subtype_from_feature(feature, outcome)
                    historical_stats = self._get_historical_statistics_regular(forecast_type, forecast_subtype)
                    
                    # Для регрессионных прогнозов добавляем точное значение из predictions
                    regression_info = ''
                    if feature == 8 and regression_data and 'forecast_total_amount' in regression_data:
                        home_goals = match_info.get('numOfHeadsHome', 0) or 0
                        away_goals = match_info.get('numOfHeadsAway', 0) or 0
                        actual_total = float(home_goals) + float(away_goals) if home_goals is not None and away_goals is not None else None
                        regression_info = f' (прогноз: {regression_data["forecast_total_amount"]:.2f}, факт: {actual_total:.1f})' if actual_total is not None else f' (прогноз: {regression_data["forecast_total_amount"]:.2f})'
                    elif feature == 9 and regression_data and 'forecast_total_home_amount' in regression_data:
                        home_goals = match_info.get('numOfHeadsHome', 0) or 0
                        actual_home = float(home_goals) if home_goals is not None else None
                        regression_info = f' (прогноз: {regression_data["forecast_total_home_amount"]:.2f}, факт: {actual_home:.1f})' if actual_home is not None else f' (прогноз: {regression_data["forecast_total_home_amount"]:.2f})'
                    elif feature == 10 and regression_data and 'forecast_total_away_amount' in regression_data:
                        away_goals = match_info.get('numOfHeadsAway', 0) or 0
                        actual_away = float(away_goals) if away_goals is not None else None
                        regression_info = f' (прогноз: {regression_data["forecast_total_away_amount"]:.2f}, факт: {actual_away:.1f})' if actual_away is not None else f' (прогноз: {regression_data["forecast_total_away_amount"]:.2f})'
                    
                    # Расширенная статистика
                    report += f'{status_icon} • {feature_description}: {outcome}{regression_info}\n'
                    report += f'  🎯 Вероятность: {probability:.1%} | 🔒 Уверенность: {confidence:.1%} | 📊 Неопределенность: {uncertainty:.1%}\n'
                    report += f'  📈 Границы: [{lower_bound:.2f} - {upper_bound:.2f}] | ⚖️ Калибровка: {historical_stats["calibration"]:.1%} | 🛡️ Стабильность: {historical_stats["stability"]:.1%}\n'
                    report += f'  Историческая точность: {historical_stats["historical_accuracy"]} | 🔥 Последние 10: {historical_stats["recent_accuracy"]}\n\n'
                
                # Итоги матча
                report += f'🏁 ИТОГИ МАТЧА:\n'
                home_goals = match_info.get('numOfHeadsHome', 0)
                away_goals = match_info.get('numOfHeadsAway', 0)
                report += f'📊 Результат: {home_goals}:{away_goals} | ⭐ Качество прогноза: {self._calculate_match_quality_regular(match_group):.1f}/10\n'
                
                # Лучший и худший прогноз
                best_worst = self._get_best_worst_predictions_regular(match_group)
                report += f'🏆 Лучший прогноз: {best_worst["best"]} | 💥 Худший прогноз: {best_worst["worst"]}\n'
                
                # Средняя точность дня
                daily_accuracy = self._calculate_daily_accuracy_regular(df_day)
                report += f'📈 Средняя точность дня: {daily_accuracy:.1%}\n\n'
            
            return report
            
        except Exception as e:
            logger.error(f'Ошибка при форматировании обычного отчета за {date}: {e}')
            return f'❌ Ошибка форматирования обычного отчета за {date}: {e}'
    
    def _format_daily_quality_report(self, df_day: pd.DataFrame, date: datetime.date) -> str:
        """
        Форматирует качественный отчет за день с расширенной статистикой.
        
        Args:
            df_day: DataFrame с данными за день
            date: Дата отчета
            
        Returns:
            str: Отформатированный отчет
        """
        try:
            if df_day.empty:
                return ''
            
            report = f'🌟 КАЧЕСТВЕННЫЕ ПРОГНОЗЫ - {date.strftime("%d.%m.%Y")}\n\n'
            
            # Группируем по матчам
            for match_id, match_group in df_day.groupby('match_id'):
                match_info = match_group.iloc[0]
                
                # Заголовок матча с match_id
                report += f'🏆 {match_info.get("sportName", "Soccer")} - {match_info.get("championshipName", "Unknown")}\n'
                report += f'🆔 Match ID: {match_id}\n'
                report += f'⚽ {match_info.get("team_home_name", "Home")} vs {match_info.get("team_away_name", "Away")}\n'
                
                # Время матча
                if 'match_date' in match_info and pd.notna(match_info['match_date']):
                    match_time = pd.to_datetime(match_info['match_date'])
                    report += f'🕐 {match_time.strftime("%H:%M")}\n'
                
                report += f'\n📊 ДЕТАЛЬНАЯ СТАТИСТИКА ПРОГНОЗА:\n\n'
                
                # Прогнозы по типам
                for _, forecast_row in match_group.iterrows():
                    forecast_type = forecast_row.get('forecast_type', 'Unknown')
                    forecast_subtype = forecast_row.get('forecast_subtype', '')
                    probability = forecast_row.get('probability', 0) or forecast_row.get('prediction_accuracy', 0)
                    confidence = forecast_row.get('confidence', 0)
                    uncertainty = forecast_row.get('uncertainty', 0)
                    lower_bound = forecast_row.get('lower_bound', 0)
                    upper_bound = forecast_row.get('upper_bound', 0)
                    actual_value = forecast_row.get('actual_value', '')
                    is_correct = forecast_row.get('prediction_correct', False)
                    
                    # Форматируем тип прогноза
                    forecast_display = self._format_forecast_type(forecast_type, forecast_subtype, actual_value)
                    
                    # Получаем историческую статистику
                    historical_stats = self._get_historical_statistics(forecast_type, forecast_subtype)
                    
                    # Расширенная статистика (БЕЗ иконок статуса - это прогнозы, а не итоги)
                    report += f'• {forecast_display}\n'
                    report += f'  🎯 Вероятность: {probability or 0:.1%} | 🔒 Уверенность: {confidence or 0:.1%} | 📊 Неопределенность: {uncertainty or 0:.1%}\n'
                    report += f'  📈 Границы: [{lower_bound or 0:.2f} - {upper_bound or 0:.2f}] | ⚖️ Калибровка: {historical_stats["calibration"]:.1%} | 🛡️ Стабильность: {historical_stats["stability"]:.1%}\n'
                    report += f'  Историческая точность: {historical_stats["historical_accuracy"]} | 🔥 Последние 10: {historical_stats["recent_accuracy"]}\n\n'
                
                # Итоги матча удалены по запросу пользователя - в прогнозах не должно быть итогов
            
            return report
            
        except Exception as e:
            logger.error(f'Ошибка при форматировании качественного отчета за {date}: {e}')
            return f'❌ Ошибка форматирования качественного отчета за {date}: {e}'
    
    def _format_daily_regular_forecasts_from_outcomes(self, df_day: pd.DataFrame, date: datetime.date) -> str:
        """
        Форматирует обычные прогнозы за день на основе данных из таблицы outcomes БЕЗ иконок статуса.
        
        Args:
            df_day: DataFrame с данными за день из таблицы outcomes
            date: Дата отчета
            
        Returns:
            str: Отформатированный отчет
        """
        try:
            if df_day.empty:
                return ''
            
            report = f'📊 ОБЫЧНЫЕ ПРОГНОЗЫ - {date.strftime("%d.%m.%Y")}\n\n'
            
            # Группируем по матчам
            for match_id, match_group in df_day.groupby('match_id'):
                match_info = match_group.iloc[0]
                
                # Заголовок матча
                report += f'🆔 Match ID: {match_id}\n'
                report += f'🏆 {match_info.get("sportName", "Unknown")} - {match_info.get("championshipName", "Unknown")}\n'
                report += f'⚽ {match_info.get("team_home_name", "Unknown")} vs {match_info.get("team_away_name", "Unknown")}\n'
                
                # Время матча
                if 'gameData' in match_info and pd.notna(match_info['gameData']):
                    match_time = pd.to_datetime(match_info['gameData'])
                    report += f'🕐 {match_time.strftime("%H:%M")}\n\n'
                else:
                    report += '\n'
                
                report += f'📊 ДЕТАЛЬНАЯ СТАТИСТИКА ПРОГНОЗА:\n\n'
                
                # Прогнозы
                processed_count = 0
                for _, outcome_row in match_group.iterrows():
                    try:
                        feature = int(outcome_row['feature']) if pd.notna(outcome_row['feature']) else 0
                        outcome = outcome_row.get('outcome', '')
                        
                        
                        probability = outcome_row.get('probability', 0)
                        confidence = outcome_row.get('confidence', 0)
                        uncertainty = outcome_row.get('uncertainty', 0)
                        lower_bound = outcome_row.get('lower_bound', 0)
                        upper_bound = outcome_row.get('upper_bound', 0)
                        
                        # Получаем описание прогноза
                        feature_desc = self._get_feature_description_from_outcome(feature, outcome)
                        
                        # Получаем расширенную статистику
                        forecast_type, forecast_subtype = self._get_forecast_type_subtype_from_feature(feature, outcome)
                        historical_stats = self._get_historical_statistics_regular(forecast_type, forecast_subtype)
                        
                        # Форматируем прогноз БЕЗ иконки статуса
                        report += f'• {feature_desc}: {outcome}\n'
                        report += f'  🎯 Вероятность: {probability:.1%} | 🔒 Уверенность: {confidence:.1%} | 📊 Неопределенность: {uncertainty:.1%}\n'
                        report += f'  📈 Границы: [{lower_bound:.2f} - {upper_bound:.2f}] | ⚖️ Калибровка: {historical_stats["calibration"]:.1%} | 🛡️ Стабильность: {historical_stats["stability"]:.1%}\n'
                        report += f'  Историческая точность: {historical_stats["historical_accuracy"]} | 🔥 Последние 10: {historical_stats["recent_accuracy"]}\n\n'
                        processed_count += 1
                    except Exception as e:
                        logger.error(f'Ошибка при форматировании прогноза для матча {match_id}, feature {feature}: {e}', exc_info=True)
                        continue
                
                
                # НЕ добавляем блок "ИТОГИ МАТЧА" - это прогнозы, а не итоги
            
            return report
            
        except Exception as e:
            logger.error(f'Ошибка при форматировании обычных прогнозов за {date}: {e}')
            return f'❌ Ошибка форматирования обычных прогнозов за {date}: {e}'
    
    def _format_daily_regular_report(self, df_day: pd.DataFrame, date: datetime.date) -> str:
        """
        Форматирует обычный отчет за день с расширенной статистикой.
        
        Args:
            df_day: DataFrame с данными за день
            date: Дата отчета
            
        Returns:
            str: Отформатированный отчет
        """
        try:
            if df_day.empty:
                return ''
            
            report = f'📈 ОБЫЧНЫЕ ПРОГНОЗЫ - {date.strftime("%d.%m.%Y")}\n\n'
            
            # Группируем по матчам
            for match_id, match_group in df_day.groupby('match_id'):
                match_info = match_group.iloc[0]
                
                # Заголовок матча с match_id
                report += f'🏆 {match_info.get("sportName", "Soccer")} - {match_info.get("championshipName", "Unknown")}\n'
                report += f'🆔 Match ID: {match_id}\n'
                report += f'⚽ {match_info.get("team_home_name", "Home")} vs {match_info.get("team_away_name", "Away")}\n'
                
                # Время матча
                if 'gameData' in match_info and pd.notna(match_info['gameData']):
                    match_time = pd.to_datetime(match_info['gameData'])
                    report += f'🕐 {match_time.strftime("%H:%M")}\n'
                
                report += f'\n📊 ДЕТАЛЬНАЯ СТАТИСТИКА ПРОГНОЗА:\n\n'
                
                # Прогнозы по типам
                forecast_row = match_group.iloc[0]
                
                # WIN_DRAW_LOSS - показываем только максимальную вероятность
                win_prob = forecast_row.get('win_draw_loss_home_win', 0) or 0
                draw_prob = forecast_row.get('win_draw_loss_draw', 0) or 0
                away_prob = forecast_row.get('win_draw_loss_away_win', 0) or 0
                
                if win_prob > 0 or draw_prob > 0 or away_prob > 0:
                    max_prob = max(win_prob, draw_prob, away_prob)
                    if win_prob == max_prob:
                        forecast_type = 'WIN_DRAW_LOSS'
                        forecast_subtype = 'П1'
                        probability = win_prob
                        historical_stats = self._get_historical_statistics_regular(forecast_type, forecast_subtype)
                        report += f'• WIN_DRAW_LOSS: П1 (Победа хозяев)\n'
                        report += f'  🎯 Вероятность: {probability:.1%} | 🔒 Уверенность: {historical_stats["confidence"]:.1%} | 📊 Неопределенность: {historical_stats["uncertainty"]:.1%}\n'
                        report += f'  📈 Границы: [{historical_stats["lower_bound"]:.2f} - {historical_stats["upper_bound"]:.2f}] | ⚖️ Калибровка: {historical_stats["calibration"]:.1%} | 🛡️ Стабильность: {historical_stats["stability"]:.1%}\n'
                        report += f'  Историческая точность: {historical_stats["historical_accuracy"]} | 🔥 Последние 10: {historical_stats["recent_accuracy"]}\n\n'
                    elif draw_prob == max_prob:
                        forecast_type = 'WIN_DRAW_LOSS'
                        forecast_subtype = 'X'
                        probability = draw_prob
                        historical_stats = self._get_historical_statistics_regular(forecast_type, forecast_subtype)
                        report += f'• WIN_DRAW_LOSS: X (Ничья)\n'
                        report += f'  🎯 Вероятность: {probability:.1%} | 🔒 Уверенность: {historical_stats["confidence"]:.1%} | 📊 Неопределенность: {historical_stats["uncertainty"]:.1%}\n'
                        report += f'  📈 Границы: [{historical_stats["lower_bound"]:.2f} - {historical_stats["upper_bound"]:.2f}] | ⚖️ Калибровка: {historical_stats["calibration"]:.1%} | 🛡️ Стабильность: {historical_stats["stability"]:.1%}\n'
                        report += f'  Историческая точность: {historical_stats["historical_accuracy"]} | 🔥 Последние 10: {historical_stats["recent_accuracy"]}\n\n'
                    elif away_prob == max_prob:
                        forecast_type = 'WIN_DRAW_LOSS'
                        forecast_subtype = 'П2'
                        probability = away_prob
                        historical_stats = self._get_historical_statistics_regular(forecast_type, forecast_subtype)
                        report += f'• WIN_DRAW_LOSS: П2 (Победа гостей)\n'
                        report += f'  🎯 Вероятность: {probability:.1%} | 🔒 Уверенность: {historical_stats["confidence"]:.1%} | 📊 Неопределенность: {historical_stats["uncertainty"]:.1%}\n'
                        report += f'  📈 Границы: [{historical_stats["lower_bound"]:.2f} - {historical_stats["upper_bound"]:.2f}] | ⚖️ Калибровка: {historical_stats["calibration"]:.1%} | 🛡️ Стабильность: {historical_stats["stability"]:.1%}\n'
                        report += f'  Историческая точность: {historical_stats["historical_accuracy"]} | 🔥 Последние 10: {historical_stats["recent_accuracy"]}\n\n'
                
                # OZ (Обе забьют) - показываем только максимальную вероятность
                oz_yes = forecast_row.get('oz_yes', 0) or 0
                oz_no = forecast_row.get('oz_no', 0) or 0
                
                if oz_yes > 0 or oz_no > 0:
                    if oz_yes > oz_no:
                        forecast_type = 'OZ'
                        forecast_subtype = 'ДА'
                        probability = oz_yes
                        historical_stats = self._get_historical_statistics_regular(forecast_type, forecast_subtype)
                        report += f'• OZ: ОБЕ ЗАБЬЮТ - ДА\n'
                        report += f'  🎯 Вероятность: {probability:.1%} | 🔒 Уверенность: {historical_stats["confidence"]:.1%} | 📊 Неопределенность: {historical_stats["uncertainty"]:.1%}\n'
                        report += f'  📈 Границы: [{historical_stats["lower_bound"]:.2f} - {historical_stats["upper_bound"]:.2f}] | ⚖️ Калибровка: {historical_stats["calibration"]:.1%} | 🛡️ Стабильность: {historical_stats["stability"]:.1%}\n'
                        report += f'  Историческая точность: {historical_stats["historical_accuracy"]} | 🔥 Последние 10: {historical_stats["recent_accuracy"]}\n\n'
                    else:
                        forecast_type = 'OZ'
                        forecast_subtype = 'НЕТ'
                        probability = oz_no
                        historical_stats = self._get_historical_statistics_regular(forecast_type, forecast_subtype)
                        report += f'• OZ: ОБЕ ЗАБЬЮТ - НЕТ\n'
                        report += f'  🎯 Вероятность: {probability:.1%} | 🔒 Уверенность: {historical_stats["confidence"]:.1%} | 📊 Неопределенность: {historical_stats["uncertainty"]:.1%}\n'
                        report += f'  📈 Границы: [{historical_stats["lower_bound"]:.2f} - {historical_stats["upper_bound"]:.2f}] | ⚖️ Калибровка: {historical_stats["calibration"]:.1%} | 🛡️ Стабильность: {historical_stats["stability"]:.1%}\n'
                        report += f'  Историческая точность: {historical_stats["historical_accuracy"]} | 🔥 Последние 10: {historical_stats["recent_accuracy"]}\n\n'
                
                # TOTAL - показываем только максимальную вероятность
                total_yes = forecast_row.get('total_yes', 0) or 0
                total_no = forecast_row.get('total_no', 0) or 0
                
                if total_yes > 0 or total_no > 0:
                    if total_yes > total_no:
                        forecast_type = 'TOTAL'
                        forecast_subtype = 'ТБ'
                        probability = total_yes
                        historical_stats = self._get_historical_statistics_regular(forecast_type, forecast_subtype)
                        report += f'• TOTAL: ТБ (Тотал больше)\n'
                        report += f'  🎯 Вероятность: {probability:.1%} | 🔒 Уверенность: {historical_stats["confidence"]:.1%} | 📊 Неопределенность: {historical_stats["uncertainty"]:.1%}\n'
                        report += f'  📈 Границы: [{historical_stats["lower_bound"]:.2f} - {historical_stats["upper_bound"]:.2f}] | ⚖️ Калибровка: {historical_stats["calibration"]:.1%} | 🛡️ Стабильность: {historical_stats["stability"]:.1%}\n'
                        report += f'  Историческая точность: {historical_stats["historical_accuracy"]} | 🔥 Последние 10: {historical_stats["recent_accuracy"]}\n\n'
                    else:
                        forecast_type = 'TOTAL'
                        forecast_subtype = 'ТМ'
                        probability = total_no
                        historical_stats = self._get_historical_statistics_regular(forecast_type, forecast_subtype)
                        report += f'• TOTAL: ТМ (Тотал меньше)\n'
                        report += f'  🎯 Вероятность: {probability:.1%} | 🔒 Уверенность: {historical_stats["confidence"]:.1%} | 📊 Неопределенность: {historical_stats["uncertainty"]:.1%}\n'
                        report += f'  📈 Границы: [{historical_stats["lower_bound"]:.2f} - {historical_stats["upper_bound"]:.2f}] | ⚖️ Калибровка: {historical_stats["calibration"]:.1%} | 🛡️ Стабильность: {historical_stats["stability"]:.1%}\n'
                        report += f'  Историческая точность: {historical_stats["historical_accuracy"]} | 🔥 Последние 10: {historical_stats["recent_accuracy"]}\n\n'
                
                # TOTAL_HOME - показываем только максимальную вероятность
                total_home_yes = forecast_row.get('total_home_yes', 0) or 0
                total_home_no = forecast_row.get('total_home_no', 0) or 0
                
                if total_home_yes > 0 or total_home_no > 0:
                    if total_home_yes > total_home_no:
                        forecast_type = 'TOTAL_HOME'
                        forecast_subtype = 'ИТ1Б'
                        probability = total_home_yes
                        historical_stats = self._get_historical_statistics_regular(forecast_type, forecast_subtype)
                        report += f'• TOTAL_HOME: ИТ1Б (Инд. тотал хозяев больше)\n'
                        report += f'  🎯 Вероятность: {probability:.1%} | 🔒 Уверенность: {historical_stats["confidence"]:.1%} | 📊 Неопределенность: {historical_stats["uncertainty"]:.1%}\n'
                        report += f'  📈 Границы: [{historical_stats["lower_bound"]:.2f} - {historical_stats["upper_bound"]:.2f}] | ⚖️ Калибровка: {historical_stats["calibration"]:.1%} | 🛡️ Стабильность: {historical_stats["stability"]:.1%}\n'
                        report += f'  Историческая точность: {historical_stats["historical_accuracy"]} | 🔥 Последние 10: {historical_stats["recent_accuracy"]}\n\n'
                    else:
                        forecast_type = 'TOTAL_HOME'
                        forecast_subtype = 'ИТ1М'
                        probability = total_home_no
                        historical_stats = self._get_historical_statistics_regular(forecast_type, forecast_subtype)
                        report += f'• TOTAL_HOME: ИТ1М (Инд. тотал хозяев меньше)\n'
                        report += f'  🎯 Вероятность: {probability:.1%} | 🔒 Уверенность: {historical_stats["confidence"]:.1%} | 📊 Неопределенность: {historical_stats["uncertainty"]:.1%}\n'
                        report += f'  📈 Границы: [{historical_stats["lower_bound"]:.2f} - {historical_stats["upper_bound"]:.2f}] | ⚖️ Калибровка: {historical_stats["calibration"]:.1%} | 🛡️ Стабильность: {historical_stats["stability"]:.1%}\n'
                        report += f'  Историческая точность: {historical_stats["historical_accuracy"]} | 🔥 Последние 10: {historical_stats["recent_accuracy"]}\n\n'
                
                # TOTAL_AWAY - показываем только максимальную вероятность
                total_away_yes = forecast_row.get('total_away_yes', 0) or 0
                total_away_no = forecast_row.get('total_away_no', 0) or 0
                
                if total_away_yes > 0 or total_away_no > 0:
                    if total_away_yes > total_away_no:
                        forecast_type = 'TOTAL_AWAY'
                        forecast_subtype = 'ИТ2Б'
                        probability = total_away_yes
                        historical_stats = self._get_historical_statistics_regular(forecast_type, forecast_subtype)
                        report += f'• TOTAL_AWAY: ИТ2Б (Инд. тотал гостей больше)\n'
                        report += f'  🎯 Вероятность: {probability:.1%} | 🔒 Уверенность: {historical_stats["confidence"]:.1%} | 📊 Неопределенность: {historical_stats["uncertainty"]:.1%}\n'
                        report += f'  📈 Границы: [{historical_stats["lower_bound"]:.2f} - {historical_stats["upper_bound"]:.2f}] | ⚖️ Калибровка: {historical_stats["calibration"]:.1%} | 🛡️ Стабильность: {historical_stats["stability"]:.1%}\n'
                        report += f'  Историческая точность: {historical_stats["historical_accuracy"]} | 🔥 Последние 10: {historical_stats["recent_accuracy"]}\n\n'
                    else:
                        forecast_type = 'TOTAL_AWAY'
                        forecast_subtype = 'ИТ2М'
                        probability = total_away_no
                        historical_stats = self._get_historical_statistics_regular(forecast_type, forecast_subtype)
                        report += f'• TOTAL_AWAY: ИТ2М (Инд. тотал гостей меньше)\n'
                        report += f'  🎯 Вероятность: {probability:.1%} | 🔒 Уверенность: {historical_stats["confidence"]:.1%} | 📊 Неопределенность: {historical_stats["uncertainty"]:.1%}\n'
                        report += f'  📈 Границы: [{historical_stats["lower_bound"]:.2f} - {historical_stats["upper_bound"]:.2f}] | ⚖️ Калибровка: {historical_stats["calibration"]:.1%} | 🛡️ Стабильность: {historical_stats["stability"]:.1%}\n'
                        report += f'  Историческая точность: {historical_stats["historical_accuracy"]} | 🔥 Последние 10: {historical_stats["recent_accuracy"]}\n\n'
                
                # TOTAL_AMOUNT - показываем прогноз
                total_amount = forecast_row.get('forecast_total_amount', 0) or 0
                if total_amount > 0:
                    forecast_type = 'TOTAL_AMOUNT'
                    forecast_subtype = f'{total_amount:.2f}'
                    probability = 0.93  # Примерная вероятность для тотала
                    historical_stats = self._get_historical_statistics_regular(forecast_type, forecast_subtype)
                    report += f'• TOTAL_AMOUNT: {total_amount:.2f}\n'
                    report += f'  🎯 Вероятность: {probability:.1%} | 🔒 Уверенность: {historical_stats["confidence"]:.1%} | 📊 Неопределенность: {historical_stats["uncertainty"]:.1%}\n'
                    report += f'  📈 Границы: [{historical_stats["lower_bound"]:.2f} - {historical_stats["upper_bound"]:.2f}] | ⚖️ Калибровка: {historical_stats["calibration"]:.1%} | 🛡️ Стабильность: {historical_stats["stability"]:.1%}\n'
                    report += f'  Историческая точность: {historical_stats["historical_accuracy"]} | 🔥 Последние 10: {historical_stats["recent_accuracy"]}\n\n'
                
                # TOTAL_HOME_AMOUNT - показываем прогноз
                total_home_amount = forecast_row.get('forecast_total_home_amount', 0) or 0
                if total_home_amount > 0:
                    forecast_type = 'TOTAL_HOME_AMOUNT'
                    forecast_subtype = f'{total_home_amount:.2f}'
                    probability = 0.93  # Примерная вероятность для тотала
                    historical_stats = self._get_historical_statistics_regular(forecast_type, forecast_subtype)
                    report += f'• TOTAL_HOME_AMOUNT: {total_home_amount:.2f}\n'
                    report += f'  🎯 Вероятность: {probability:.1%} | 🔒 Уверенность: {historical_stats["confidence"]:.1%} | 📊 Неопределенность: {historical_stats["uncertainty"]:.1%}\n'
                    report += f'  📈 Границы: [{historical_stats["lower_bound"]:.2f} - {historical_stats["upper_bound"]:.2f}] | ⚖️ Калибровка: {historical_stats["calibration"]:.1%} | 🛡️ Стабильность: {historical_stats["stability"]:.1%}\n'
                    report += f'  Историческая точность: {historical_stats["historical_accuracy"]} | 🔥 Последние 10: {historical_stats["recent_accuracy"]}\n\n'
                
                # TOTAL_AWAY_AMOUNT - показываем прогноз
                total_away_amount = forecast_row.get('forecast_total_away_amount', 0) or 0
                if total_away_amount > 0:
                    forecast_type = 'TOTAL_AWAY_AMOUNT'
                    forecast_subtype = f'{total_away_amount:.2f}'
                    probability = 0.93  # Примерная вероятность для тотала
                    historical_stats = self._get_historical_statistics_regular(forecast_type, forecast_subtype)
                    report += f'• TOTAL_AWAY_AMOUNT: {total_away_amount:.2f}\n'
                    report += f'  🎯 Вероятность: {probability:.1%} | 🔒 Уверенность: {historical_stats["confidence"]:.1%} | 📊 Неопределенность: {historical_stats["uncertainty"]:.1%}\n'
                    report += f'  📈 Границы: [{historical_stats["lower_bound"]:.2f} - {historical_stats["upper_bound"]:.2f}] | ⚖️ Калибровка: {historical_stats["calibration"]:.1%} | 🛡️ Стабильность: {historical_stats["stability"]:.1%}\n'
                    report += f'  Историческая точность: {historical_stats["historical_accuracy"]} | 🔥 Последние 10: {historical_stats["recent_accuracy"]}\n\n'
            
            return report
            
        except Exception as e:
            logger.error(f'Ошибка при форматировании обычного отчета за {date}: {e}')
            return f'❌ Ошибка форматирования обычного отчета за {date}: {e}'
    
    def _format_forecasts_report(self, df_quality: pd.DataFrame, df_regular: pd.DataFrame, year: Optional[str] = None) -> str:
        """
        Форматирует детальный отчет по прогнозам для публикации.
        
        Args:
            df_quality: DataFrame с качественными прогнозами
            df_regular: DataFrame с обычными прогнозами
            year: Год для фильтрации (опционально)
            
        Returns:
            str: Отформатированный отчет по прогнозам
        """
        try:
            if df_quality.empty and df_regular.empty:
                return f'❌ Нет данных в прогнозах за {year or "весь период"}'
            
            period_title = f'за {year}' if year else 'за весь период'
            report = f'📊 ДЕТАЛЬНЫЕ ПРОГНОЗЫ {period_title.upper()}\n\n'
            
            # Группируем по датам для детального отчета
            if not df_quality.empty:
                report += self._format_detailed_quality_forecasts(df_quality)
            
            if not df_regular.empty:
                report += self._format_detailed_regular_forecasts(df_regular)
            
            # Общая статистика в конце
            report += self._format_summary_statistics(df_quality, df_regular)
            
            return report
            
        except Exception as e:
            logger.error(f'Ошибка при форматировании детального отчета по прогнозам: {e}')
            return f'❌ Ошибка форматирования: {e}'
    
    def _format_detailed_quality_forecasts(self, df_quality: pd.DataFrame) -> str:
        """
        Форматирует детальные качественные прогнозы по дням.
        
        Args:
            df_quality: DataFrame с качественными прогнозами
            
        Returns:
            str: Отформатированный детальный отчет
        """
        try:
            if df_quality.empty:
                return ''
            
            report = '🌟 КАЧЕСТВЕННЫЕ ПРОГНОЗЫ:\n\n'
            
            # Группируем по датам
            df_quality['match_date'] = pd.to_datetime(df_quality['match_date'], errors='coerce')
            df_quality = df_quality.sort_values(['match_date', 'match_id'])
            
            for date, day_group in df_quality.groupby(df_quality['match_date'].dt.date):
                report += f'📅 {date.strftime("%d.%m.%Y")}\n'
                
                # Группируем по матчам
                for match_id, match_group in day_group.groupby('match_id'):
                    match_info = match_group.iloc[0]
                    
                    # Заголовок матча
                    report += f'🏆 {match_info.get("sportName", "Soccer")} - {match_info.get("championshipName", "Unknown")}\n'
                    report += f'⚽ {match_info.get("team_home_name", "Home")} vs {match_info.get("team_away_name", "Away")}\n'
                    
                    # Время матча (если есть)
                    if 'match_date' in match_info and pd.notna(match_info['match_date']):
                        match_time = pd.to_datetime(match_info['match_date'])
                        report += f'🕐 {match_time.strftime("%H:%M")}\n'
                    
                    # Прогнозы по типам
                    for _, forecast_row in match_group.iterrows():
                        forecast_type = forecast_row.get('forecast_type', 'Unknown')
                        forecast_subtype = forecast_row.get('forecast_subtype', '')
                        probability = forecast_row.get('prediction_accuracy', 0)
                        actual_value = forecast_row.get('actual_value', '')
                        is_correct = forecast_row.get('prediction_correct', False)
                        
                        # Форматируем тип прогноза
                        forecast_display = self._format_forecast_type(forecast_type, forecast_subtype, actual_value)
                        
                        # Статус прогноза
                        status_icon = '✅' if is_correct else '❌'
                        
                        # Форматируем как в примере пользователя
                        report += f'  • {forecast_display} | 🎯 {probability:.1%} | 📊 {probability:.1%} | 📈 {probability:.1%}\n'
                    
                    report += '\n'
                
                report += '\n'
            
            return report
            
        except Exception as e:
            logger.error(f'Ошибка при форматировании детальных качественных прогнозов: {e}')
            return f'❌ Ошибка форматирования качественных прогнозов: {e}'
    
    def _format_detailed_regular_forecasts(self, df_regular: pd.DataFrame) -> str:
        """
        Форматирует детальные обычные прогнозы по дням.
        
        Args:
            df_regular: DataFrame с обычными прогнозами
            
        Returns:
            str: Отформатированный детальный отчет
        """
        try:
            if df_regular.empty:
                return ''
            
            report = '📈 ОБЫЧНЫЕ ПРОГНОЗЫ:\n\n'
            
            # Группируем по датам
            df_regular['gameData'] = pd.to_datetime(df_regular['gameData'], errors='coerce')
            df_regular = df_regular.sort_values(['gameData', 'match_id'])
            
            for date, day_group in df_regular.groupby(df_regular['gameData'].dt.date):
                report += f'📅 {date.strftime("%d.%m.%Y")}\n'
                
                # Группируем по матчам
                for match_id, match_group in day_group.groupby('match_id'):
                    match_info = match_group.iloc[0]
                    
                    # Заголовок матча
                    report += f'🏆 {match_info.get("sportName", "Soccer")} - {match_info.get("championshipName", "Unknown")}\n'
                    report += f'⚽ {match_info.get("team_home_name", "Home")} vs {match_info.get("team_away_name", "Away")}\n'
                    
                    # Время матча
                    if 'gameData' in match_info and pd.notna(match_info['gameData']):
                        match_time = pd.to_datetime(match_info['gameData'])
                        report += f'🕐 {match_time.strftime("%H:%M")}\n'
                    
                    # Прогнозы по типам
                    forecast_row = match_group.iloc[0]
                    
                    # WIN_DRAW_LOSS - показываем только максимальную вероятность
                    win_prob = forecast_row.get('win_draw_loss_home_win', 0)
                    draw_prob = forecast_row.get('win_draw_loss_draw', 0)
                    away_prob = forecast_row.get('win_draw_loss_away_win', 0)
                    
                    if win_prob > 0 or draw_prob > 0 or away_prob > 0:
                        max_prob = max(win_prob, draw_prob, away_prob)
                        if win_prob == max_prob:
                            report += f'  • WIN_DRAW_LOSS: П1 (Победа хозяев) | 🎯 {win_prob:.1%} | 📊 {win_prob:.1%} | 📈 {win_prob:.1%}\n'
                        elif draw_prob == max_prob:
                            report += f'  • WIN_DRAW_LOSS: X (Ничья) | 🎯 {draw_prob:.1%} | 📊 {draw_prob:.1%} | 📈 {draw_prob:.1%}\n'
                        elif away_prob == max_prob:
                            report += f'  • WIN_DRAW_LOSS: П2 (Победа гостей) | 🎯 {away_prob:.1%} | 📊 {away_prob:.1%} | 📈 {away_prob:.1%}\n'
                    
                    # OZ (Обе забьют) - показываем только максимальную вероятность
                    oz_yes = forecast_row.get('oz_yes', 0)
                    oz_no = forecast_row.get('oz_no', 0)
                    
                    if oz_yes > 0 or oz_no > 0:
                        if oz_yes > oz_no:
                            report += f'  • OZ: ОЗД (Обе забьют - Да) | 🎯 {oz_yes:.1%} | 📊 {oz_yes:.1%} | 📈 {oz_yes:.1%}\n'
                        else:
                            report += f'  • OZ: ОЗН (Обе забьют - Нет) | 🎯 {oz_no:.1%} | 📊 {oz_no:.1%} | 📈 {oz_no:.1%}\n'
                    
                    # GOAL_HOME - показываем только максимальную вероятность
                    goal_home_yes = forecast_row.get('goal_home_yes', 0)
                    goal_home_no = forecast_row.get('goal_home_no', 0)
                    
                    if goal_home_yes > 0 or goal_home_no > 0:
                        if goal_home_yes > goal_home_no:
                            report += f'  • GOAL_HOME: ДА | 🎯 {goal_home_yes:.1%} | 📊 {goal_home_yes:.1%} | 📈 {goal_home_yes:.1%}\n'
                        else:
                            report += f'  • GOAL_HOME: НЕТ | 🎯 {goal_home_no:.1%} | 📊 {goal_home_no:.1%} | 📈 {goal_home_no:.1%}\n'
                    
                    # GOAL_AWAY - показываем только максимальную вероятность
                    goal_away_yes = forecast_row.get('goal_away_yes', 0)
                    goal_away_no = forecast_row.get('goal_away_no', 0)
                    
                    if goal_away_yes > 0 or goal_away_no > 0:
                        if goal_away_yes > goal_away_no:
                            report += f'  • GOAL_AWAY: ДА | 🎯 {goal_away_yes:.1%} | 📊 {goal_away_yes:.1%} | 📈 {goal_away_yes:.1%}\n'
                        else:
                            report += f'  • GOAL_AWAY: НЕТ | 🎯 {goal_away_no:.1%} | 📊 {goal_away_no:.1%} | 📈 {goal_away_no:.1%}\n'
                    
                    # TOTAL - показываем только максимальную вероятность
                    total_yes = forecast_row.get('total_yes', 0)
                    total_no = forecast_row.get('total_no', 0)
                    
                    if total_yes > 0 or total_no > 0:
                        if total_yes > total_no:
                            report += f'  • TOTAL: БОЛЬШЕ | 🎯 {total_yes:.1%} | 📊 {total_yes:.1%} | 📈 {total_yes:.1%}\n'
                        else:
                            report += f'  • TOTAL: МЕНЬШЕ | 🎯 {total_no:.1%} | 📊 {total_no:.1%} | 📈 {total_no:.1%}\n'
                    
                    # TOTAL_HOME - показываем только максимальную вероятность
                    total_home_yes = forecast_row.get('total_home_yes', 0)
                    total_home_no = forecast_row.get('total_home_no', 0)
                    
                    if total_home_yes > 0 or total_home_no > 0:
                        if total_home_yes > total_home_no:
                            report += f'  • TOTAL_HOME: БОЛЬШЕ | 🎯 {total_home_yes:.1%} | 📊 {total_home_yes:.1%} | 📈 {total_home_yes:.1%}\n'
                        else:
                            report += f'  • TOTAL_HOME: МЕНЬШЕ | 🎯 {total_home_no:.1%} | 📊 {total_home_no:.1%} | 📈 {total_home_no:.1%}\n'
                    
                    # TOTAL_AWAY - показываем только максимальную вероятность
                    total_away_yes = forecast_row.get('total_away_yes', 0)
                    total_away_no = forecast_row.get('total_away_no', 0)
                    
                    if total_away_yes > 0 or total_away_no > 0:
                        if total_away_yes > total_away_no:
                            report += f'  • TOTAL_AWAY: БОЛЬШЕ | 🎯 {total_away_yes:.1%} | 📊 {total_away_yes:.1%} | 📈 {total_away_yes:.1%}\n'
                        else:
                            report += f'  • TOTAL_AWAY: МЕНЬШЕ | 🎯 {total_away_no:.1%} | 📊 {total_away_no:.1%} | 📈 {total_away_no:.1%}\n'
                    
                    # TOTAL_AMOUNT (регрессионные)
                    if pd.notna(forecast_row.get('forecast_total_amount')):
                        amount = forecast_row['forecast_total_amount']
                        report += f'  • TOTAL_AMOUNT: {amount:.2f} | 🎯 93.0% | 📊 50.0% | 📈 7.0%\n'
                    if pd.notna(forecast_row.get('forecast_total_home_amount')):
                        amount = forecast_row['forecast_total_home_amount']
                        report += f'  • TOTAL_HOME_AMOUNT: {amount:.2f} | 🎯 93.0% | 📊 50.0% | 📈 7.0%\n'
                    if pd.notna(forecast_row.get('forecast_total_away_amount')):
                        amount = forecast_row['forecast_total_away_amount']
                        report += f'  • TOTAL_AWAY_AMOUNT: {amount:.2f} | 🎯 93.0% | 📊 50.0% | 📈 20.0%\n'
                    
                    report += '\n'
                
                report += '\n'
            
            return report
            
        except Exception as e:
            logger.error(f'Ошибка при форматировании детальных обычных прогнозов: {e}')
            return f'❌ Ошибка форматирования обычных прогнозов: {e}'
    
    def _format_forecast_type(self, forecast_type: str, forecast_subtype: str, actual_value: str) -> str:
        """
        Форматирует тип прогноза для отображения.
        
        Args:
            forecast_type: Тип прогноза
            forecast_subtype: Подтип прогноза
            actual_value: Фактическое значение
            
        Returns:
            str: Отформатированный тип прогноза
        """
        try:
            type_mapping = {
                'win_draw_loss': 'WIN_DRAW_LOSS',
                'oz': 'OZ',
                'goal_home': 'GOAL_HOME',
                'goal_away': 'GOAL_AWAY',
                'total': 'TOTAL',
                'total_home': 'TOTAL_HOME',
                'total_away': 'TOTAL_AWAY',
                'total_amount': 'TOTAL_AMOUNT',
                'total_home_amount': 'TOTAL_HOME_AMOUNT',
                'total_away_amount': 'TOTAL_AWAY_AMOUNT'
            }
            
            display_type = type_mapping.get(forecast_type, forecast_type.upper())
            
            # Добавляем подтип если есть
            if forecast_subtype:
                subtype_mapping = {
                    'home_win': 'П1',
                    'draw': 'X',
                    'away_win': 'П2',
                    'yes': 'ДА',
                    'no': 'НЕТ',
                    'more': 'БОЛЬШЕ',
                    'less': 'МЕНЬШЕ'
                }
                display_subtype = subtype_mapping.get(forecast_subtype, forecast_subtype.upper())
                return f'{display_type}: {display_subtype}'
            
            return display_type
            
        except Exception as e:
            logger.error(f'Ошибка при форматировании типа прогноза: {e}')
            return f'{forecast_type}: {forecast_subtype}'
    
    def _format_summary_statistics(self, df_quality: pd.DataFrame, df_regular: pd.DataFrame) -> str:
        """
        Форматирует общую статистику в конце отчета.
        
        Args:
            df_quality: DataFrame с качественными прогнозами
            df_regular: DataFrame с обычными прогнозами
            
        Returns:
            str: Отформатированная статистика
        """
        try:
            report = '📊 ОБЩАЯ СТАТИСТИКА:\n\n'
            
            total_quality = len(df_quality)
            total_regular = len(df_regular)
            total_predictions = total_quality + total_regular
            
            if total_quality > 0:
                correct_quality = len(df_quality[df_quality['prediction_correct'] == True])
                accuracy_quality = (correct_quality / total_quality * 100) if total_quality > 0 else 0
            else:
                correct_quality = 0
                accuracy_quality = 0
            
            report += f'  • Всего качественных прогнозов: {total_quality}\n'
            report += f'  • Всего обычных прогнозов: {total_regular}\n'
            report += f'  • Общее количество: {total_predictions}\n'
            
            if total_quality > 0:
                report += f'  • Правильных качественных: {correct_quality}\n'
                report += f'  • Точность качественных: {accuracy_quality:.1f}%\n'
            
            return report
            
        except Exception as e:
            logger.error(f'Ошибка при форматировании общей статистики: {e}')
            return '❌ Ошибка форматирования статистики'
    
    def _format_outcomes_report(self, df_quality: pd.DataFrame, df_regular: pd.DataFrame, year: Optional[str] = None) -> str:
        """
        Форматирует отчет по итогам за весь период.
        
        Args:
            df_quality: DataFrame с качественными прогнозами
            df_regular: DataFrame с обычными прогнозами
            year: Год для фильтрации (опционально)
            
        Returns:
            str: Отформатированный отчет по итогам
        """
        try:
            if df_quality.empty and df_regular.empty:
                return f'❌ Нет данных в итогах за {year or "весь период"}'
            
            period_title = f'за {year}' if year else 'за весь период'
            report = f'📊 Итоги матчей {period_title}\n\n'
            
            # Общая статистика
            total_quality = len(df_quality)
            total_regular = len(df_regular)
            
            if total_quality > 0:
                correct_quality = len(df_quality[df_quality['prediction_correct'] == True])
                accuracy_quality = (correct_quality / total_quality * 100) if total_quality > 0 else 0
            else:
                correct_quality = 0
                accuracy_quality = 0
            
            report += f'📈 Общая статистика итогов:\n'
            report += f'  • Всего качественных итогов: {total_quality}\n'
            report += f'  • Всего обычных итогов: {total_regular}\n'
            
            if total_quality > 0:
                report += f'  • Правильных качественных: {correct_quality}\n'
                report += f'  • Точность качественных: {accuracy_quality:.1f}%\n'
            
            report += '\n'
            
            # Статистика по турнирам
            if not df_regular.empty:
                tournaments = df_regular['championshipName'].value_counts()
                report += f'🏆 Статистика по турнирам (итоги):\n'
                for tournament, count in tournaments.head(10).items():
                    report += f'  • {tournament}: {count} матчей\n'
            
            return report
            
        except Exception as e:
            logger.error(f'Ошибка при форматировании отчета по итогам: {e}')
            return f'❌ Ошибка форматирования: {e}'
    
    def _publish_forecasts_report(self, content: str, year: Optional[str] = None, file_date: Optional[datetime] = None) -> None:
        """
        Публикует отчет по прогнозам.
        
        Args:
            content: Содержимое отчета по прогнозам
            year: Год для фильтрации (опционально)
            file_date: Дата для файла (опционально)
        """
        try:
            logger.info(f'Публикация отчета по прогнозам за {year or "весь период"}')
            
            # Определяем дату для файла
            if file_date:
                date_str = file_date.strftime('%Y-%m-%d')
            elif year:
                date_str = f'{year}-12-31'
            else:
                date_str = datetime.now().strftime('%Y-%m-%d')
            
            message = {
                'forecasts': content,
                'date': date_str
            }
            
            for publisher in self.conformal_publishers:
                try:
                    publisher.publish(message)
                    logger.info(f'Отчет по прогнозам опубликован через {type(publisher).__name__}')
                except Exception as pub_error:
                    logger.error(f'Ошибка публикации прогнозов через {type(publisher).__name__}: {pub_error}')
            
        except Exception as e:
            logger.error(f'Ошибка при публикации отчета по прогнозам: {e}')
    
    def _publish_outcomes_report(self, content: str, year: Optional[str] = None, file_date: Optional[datetime] = None) -> None:
        """
        Публикует отчет по итогам.
        
        Args:
            content: Содержимое отчета по итогам
            year: Год для фильтрации (опционально)
            file_date: Дата для файла (опционально)
        """
        try:
            logger.info(f'Публикация отчета по итогам за {year or "весь период"}')
            
            # Определяем дату для файла
            if file_date:
                date_str = file_date.strftime('%Y-%m-%d')
            elif year:
                date_str = f'{year}-12-31'
            else:
                date_str = datetime.now().strftime('%Y-%m-%d')
            
            message = {
                'outcomes': content,
                'date': date_str
            }
            
            for publisher in self.conformal_publishers:
                try:
                    publisher.publish(message)
                    logger.info(f'Отчет по итогам опубликован через {type(publisher).__name__}')
                except Exception as pub_error:
                    logger.error(f'Ошибка публикации итогов через {type(publisher).__name__}: {pub_error}')
            
        except Exception as e:
            logger.error(f'Ошибка при публикации отчета по итогам: {e}')
    
    def _format_combined_forecast_report(self, df_quality: pd.DataFrame, df_regular: pd.DataFrame, period: str) -> str:
        """
        Форматирует объединенный отчет по прогнозам (качественные + обычные).
        
        Args:
            df_quality: DataFrame с качественными прогнозами
            df_regular: DataFrame с обычными прогнозами
            period: Период (сегодня, вчера, etc.)
            
        Returns:
            str: Отформатированный отчет
        """
        try:
            if df_quality.empty and df_regular.empty:
                return f'❌ Нет прогнозов на {period}'
            
            report = f'📊 Прогнозы на {period}\n\n'
            
            # Качественные прогнозы
            if not df_quality.empty:
                report += f'🌟 КАЧЕСТВЕННЫЕ ПРОГНОЗЫ ({len(df_quality)} шт.):\n'
                for match_id, group in df_quality.groupby('match_id'):
                    match_info = group.iloc[0]
                    report += f'⚽ {match_info.get("team_home_name", "Домашняя")} vs {match_info.get("team_away_name", "Гостевая")}\n'
                    report += f'📅 {match_info.get("match_date", "Дата")}\n'
                    
                    for _, row in group.iterrows():
                        forecast_type = row.get('forecast_type', 'Неизвестно')
                        forecast_subtype = row.get('forecast_subtype', '')
                        probability = row.get('prediction_accuracy', 0) or 0
                        
                        report += f'  • {forecast_type} {forecast_subtype}: {probability:.1%}\n'
                    
                    report += '\n'
            
            # Обычные прогнозы
            if not df_regular.empty:
                report += f'📈 ОБЫЧНЫЕ ПРОГНОЗЫ ({len(df_regular)} шт.):\n'
                for match_id, group in df_regular.groupby('match_id'):
                    match_info = group.iloc[0]
                    report += f'⚽ {match_info.get("team_home_name", "Домашняя")} vs {match_info.get("team_away_name", "Гостевая")}\n'
                    report += f'📅 {match_info.get("gameData", "Дата")}\n'
                    
                    for _, row in group.iterrows():
                        # Форматируем обычные прогнозы
                        if row.get('win_draw_loss_home_win', 0) > 0:
                            report += f'  • П1: {row["win_draw_loss_home_win"]:.1%}\n'
                        if row.get('win_draw_loss_draw', 0) > 0:
                            report += f'  • X: {row["win_draw_loss_draw"]:.1%}\n'
                        if row.get('win_draw_loss_away_win', 0) > 0:
                            report += f'  • П2: {row["win_draw_loss_away_win"]:.1%}\n'
                        if row.get('oz_yes', 0) > 0:
                            report += f'  • ОЗ Да: {row["oz_yes"]:.1%}\n'
                        if row.get('oz_no', 0) > 0:
                            report += f'  • ОЗ Нет: {row["oz_no"]:.1%}\n'
                    
                    report += '\n'
            
            return report
            
        except Exception as e:
            logger.error(f'Ошибка при форматировании объединенного отчета по прогнозам: {e}')
            return f'❌ Ошибка форматирования: {e}'
    
    def _format_combined_outcome_report(self, df_quality: pd.DataFrame, df_regular: pd.DataFrame, period: str) -> str:
        """
        Форматирует объединенный отчет по итогам матчей (качественные + обычные).
        
        Args:
            df_quality: DataFrame с качественными итогами
            df_regular: DataFrame с обычными итогами
            period: Период (сегодня, вчера, etc.)
            
        Returns:
            str: Отформатированный отчет
        """
        try:
            if df_quality.empty and df_regular.empty:
                return f'❌ Нет итогов за {period}'
            
            report = f'📊 Итоги матчей за {period}\n\n'
            
            # Качественные итоги
            if not df_quality.empty:
                report += f'🌟 КАЧЕСТВЕННЫЕ ИТОГИ ({len(df_quality)} шт.):\n'
                for match_id, group in df_quality.groupby('match_id'):
                    match_info = group.iloc[0]
                    report += f'⚽ {match_info.get("team_home_name", "Домашняя")} vs {match_info.get("team_away_name", "Гостевая")}\n'
                    report += f'📅 {match_info.get("match_date", "Дата")}\n'
                    report += f'🏆 Счет: {match_info.get("actual_value", "Неизвестно")}\n'
                    
                    correct_predictions = group[group['prediction_correct'] == True]
                    total_predictions = len(group)
                    correct_count = len(correct_predictions)
                    
                    report += f'✅ Правильных прогнозов: {correct_count}/{total_predictions}\n'
                    
                    if correct_count > 0:
                        report += '  Правильные прогнозы:\n'
                        for _, row in correct_predictions.iterrows():
                            forecast_type = row.get('forecast_type', 'Неизвестно')
                            forecast_subtype = row.get('forecast_subtype', '')
                            report += f'    • {forecast_type} {forecast_subtype}\n'
                    
                    report += '\n'
            
            # Обычные итоги
            if not df_regular.empty:
                report += f'📈 ОБЫЧНЫЕ ИТОГИ ({len(df_regular)} шт.):\n'
                for match_id, group in df_regular.groupby('match_id'):
                    match_info = group.iloc[0]
                    report += f'⚽ {match_info.get("team_home_name", "Домашняя")} vs {match_info.get("team_away_name", "Гостевая")}\n'
                    report += f'📅 {match_info.get("gameData", "Дата")}\n'
                    report += f'🏆 Счет: {match_info.get("numOfHeadsHome", "?")}:{match_info.get("numOfHeadsAway", "?")}\n'
                    report += f'📊 Прогнозы созданы: {len(group)} шт.\n\n'
            
            return report
            
        except Exception as e:
            logger.error(f'Ошибка при форматировании объединенного отчета по итогам: {e}')
            return f'❌ Ошибка форматирования: {e}'
    
    def _format_combined_all_time_report(self, df_quality: pd.DataFrame, df_regular: pd.DataFrame, year: Optional[str] = None) -> str:
        """
        Форматирует объединенный отчет по статистике за весь период (качественные + обычные).
        
        Args:
            df_quality: DataFrame с качественной статистикой
            df_regular: DataFrame с обычной статистикой
            year: Год для фильтрации (опционально)
            
        Returns:
            str: Отформатированный отчет
        """
        try:
            if df_quality.empty and df_regular.empty:
                return f'❌ Нет данных в статистике за {year or "весь период"}'
            
            period_title = f'за {year}' if year else 'за весь период'
            report = f'📊 Статистика прогнозов {period_title}\n\n'
            
            # Общая статистика
            total_quality = len(df_quality)
            total_regular = len(df_regular)
            total_predictions = total_quality + total_regular
            
            if total_quality > 0:
                correct_quality = len(df_quality[df_quality['prediction_correct'] == True])
                accuracy_quality = (correct_quality / total_quality * 100) if total_quality > 0 else 0
            else:
                correct_quality = 0
                accuracy_quality = 0
            
            report += f'📈 Общая статистика:\n'
            report += f'  • Всего качественных прогнозов: {total_quality}\n'
            report += f'  • Всего обычных прогнозов: {total_regular}\n'
            report += f'  • Общее количество: {total_predictions}\n'
            
            if total_quality > 0:
                report += f'  • Правильных качественных: {correct_quality}\n'
                report += f'  • Точность качественных: {accuracy_quality:.1f}%\n'
            
            report += '\n'
            
            # Статистика по типам качественных прогнозов
            if not df_quality.empty:
                forecast_types = df_quality['forecast_type'].value_counts()
                report += f'📊 Статистика по типам качественных прогнозов:\n'
                for forecast_type, count in forecast_types.items():
                    type_df = df_quality[df_quality['forecast_type'] == forecast_type]
                    type_correct = len(type_df[type_df['prediction_correct'] == True])
                    type_accuracy = (type_correct / count * 100) if count > 0 else 0
                    report += f'  • {forecast_type}: {type_correct}/{count} ({type_accuracy:.1f}%)\n'
                
                report += '\n'
            
            # Статистика по турнирам
            if not df_regular.empty:
                tournaments = df_regular['championshipName'].value_counts()
                report += f'🏆 Статистика по турнирам (обычные прогнозы):\n'
                for tournament, count in tournaments.head(10).items():
                    report += f'  • {tournament}: {count} прогнозов\n'
                
                report += '\n'
            
            return report
            
        except Exception as e:
            logger.error(f'Ошибка при форматировании объединенного отчета по статистике: {e}')
            return f'❌ Ошибка форматирования: {e}'
    
    def _publish_daily_outcomes(self, df_regular_outcomes: pd.DataFrame, df_quality_statistics: pd.DataFrame, year: Optional[str] = None) -> None:
        """
        Публикует итоги по дням с разделением на regular и quality.
        
        Args:
            df_regular_outcomes: DataFrame с обычными итогами из таблицы outcomes
            df_quality_statistics: DataFrame с качественными итогами из таблицы statistics
            year: Год для фильтрации (опционально)
        """
        try:
            logger.info('Публикация итогов по дням с разделением на regular и quality')
            
            # Обрабатываем regular итоги (из таблицы outcomes)
            if not df_regular_outcomes.empty:
                df_regular_outcomes['gameData'] = pd.to_datetime(df_regular_outcomes['gameData'], errors='coerce')
                regular_dates = df_regular_outcomes['gameData'].dt.date.dropna().unique()
                
                for date_item in sorted(regular_dates):
                    day_regular_outcomes = df_regular_outcomes[df_regular_outcomes['gameData'].dt.date == date_item]
                    self._publish_daily_regular_outcome_report(day_regular_outcomes, date_item)
            
            # Обрабатываем quality итоги (из таблицы statistics)
            if not df_quality_statistics.empty:
                df_quality_statistics['match_date'] = pd.to_datetime(df_quality_statistics['match_date'], errors='coerce')
                quality_dates = df_quality_statistics['match_date'].dt.date.dropna().unique()
                
                for date_item in sorted(quality_dates):
                    day_quality_outcomes = df_quality_statistics[df_quality_statistics['match_date'].dt.date == date_item]
                    self._publish_daily_quality_outcome_report(day_quality_outcomes, date_item)
            
            logger.info('Итоги по дням опубликованы с разделением на regular и quality')
            
        except Exception as e:
            logger.error(f'Ошибка при публикации итогов по дням: {e}')
    
    def _publish_daily_regular_outcome_report(self, df_day: pd.DataFrame, date: datetime.date) -> None:
        """
        Публикует отчет с обычными итогами за конкретный день (из таблицы outcomes).
        
        Args:
            df_day: DataFrame с данными за день из таблицы outcomes
            date: Дата отчета
        """
        try:
            logger.info(f'Публикация отчета с обычными итогами за {date}')
            
            # Форматируем отчет
            report = self._format_daily_outcome_report(df_day, date)
            
            if report:
                # Создаем сообщение для публикации
                message = {
                    'daily_regular_outcome': report,
                    'date': date.strftime('%Y-%m-%d'),
                    'report_type': 'regular_outcome'
                }
                
                # Публикуем через конформные публикаторы
                for publisher in self.conformal_publishers:
                    try:
                        publisher.publish(message)
                        logger.info(f'Отчет с обычными итогами за {date} опубликован через {type(publisher).__name__}')
                    except Exception as pub_error:
                        logger.error(f'Ошибка публикации отчета с обычными итогами за {date} через {type(publisher).__name__}: {pub_error}')
            
        except Exception as e:
            logger.error(f'Ошибка при публикации отчета с обычными итогами за {date}: {e}')
    
    def _publish_daily_quality_outcome_report(self, df_day: pd.DataFrame, date: datetime.date) -> None:
        """
        Публикует отчет с качественными итогами за конкретный день (из таблицы statistics).
        
        Args:
            df_day: DataFrame с данными за день из таблицы statistics
            date: Дата отчета
        """
        try:
            logger.info(f'Публикация отчета с качественными итогами за {date}')
            
            # Форматируем отчет
            report = self._format_daily_quality_outcome_report(df_day, date)
            
            if report:
                # Создаем сообщение для публикации
                message = {
                    'daily_quality_outcome': report,
                    'date': date.strftime('%Y-%m-%d'),
                    'report_type': 'quality_outcome'
                }
                
                # Публикуем через конформные публикаторы
                for publisher in self.conformal_publishers:
                    try:
                        publisher.publish(message)
                        logger.info(f'Отчет с качественными итогами за {date} опубликован через {type(publisher).__name__}')
                    except Exception as pub_error:
                        logger.error(f'Ошибка публикации отчета с качественными итогами за {date} через {type(publisher).__name__}: {pub_error}')
            
        except Exception as e:
            logger.error(f'Ошибка при публикации отчета с качественными итогами за {date}: {e}')
    
    def _publish_daily_outcome_report(self, df_day: pd.DataFrame, date: datetime.date) -> None:
        """
        Публикует отчет с итогами за конкретный день.
        
        Args:
            df_day: DataFrame с данными за день
            date: Дата отчета
        """
        try:
            logger.info(f'Публикация отчета с итогами за {date}')
            
            # Форматируем отчет
            report = self._format_daily_outcome_report(df_day, date)
            
            if report:
                # Создаем сообщение для публикации
                message = {
                    'daily_outcome': report,
                    'date': date.strftime('%Y-%m-%d'),
                    'report_type': 'outcome'
                }
                
                # Публикуем через конформные публикаторы
                for publisher in self.conformal_publishers:
                    try:
                        publisher.publish(message)
                        logger.info(f'Отчет с итогами за {date} опубликован через {type(publisher).__name__}')
                    except Exception as pub_error:
                        logger.error(f'Ошибка публикации отчета с итогами за {date} через {type(publisher).__name__}: {pub_error}')
            
        except Exception as e:
            logger.error(f'Ошибка при публикации отчета с итогами за {date}: {e}')
    
    def _format_daily_outcome_report(self, df_day: pd.DataFrame, date: datetime.date) -> str:
        """
        Форматирует отчет с итогами за день.
        
        Args:
            df_day: DataFrame с данными за день
            date: Дата отчета
            
        Returns:
            str: Отформатированный отчет
        """
        try:
            if df_day.empty:
                return ''
            
            report = f'🏁 ИТОГИ МАТЧЕЙ - {date.strftime("%d.%m.%Y")}\n\n'
            
            # Группируем по матчам
            for match_id, match_group in df_day.groupby('match_id'):
                match_info = match_group.iloc[0]
                
                # Заголовок матча с match_id
                report += f'🆔 Match ID: {match_id}\n'
                report += f'🏆 {match_info.get("sportName", "Soccer")} - {match_info.get("championshipName", "Unknown")}\n'
                report += f'⚽ {match_info.get("team_home_name", "Home")} vs {match_info.get("team_away_name", "Away")}\n'
                
                # Результат матча
                home_goals = match_info.get('numOfHeadsHome', 0)
                away_goals = match_info.get('numOfHeadsAway', 0)
                report += f'📊 Результат: {home_goals}:{away_goals}\n'
                
                # Время матча
                if 'gameData' in match_info and pd.notna(match_info['gameData']):
                    match_time = pd.to_datetime(match_info['gameData'])
                    report += f'🕐 {match_time.strftime("%H:%M")}\n'
                
                # Получаем данные регрессии из predictions для этого матча (один раз на матч)
                regression_data = self._get_regression_data_for_match(match_id)
                
                # Прогнозы и их результаты
                for _, outcome_row in match_group.iterrows():
                    feature = int(outcome_row['feature']) if pd.notna(outcome_row['feature']) else 0
                    forecast = outcome_row.get('forecast', 'Unknown')
                    outcome = outcome_row.get('outcome', 'Unknown')
                    probability = outcome_row.get('probability', 0)
                    confidence = outcome_row.get('confidence', 0)
                    uncertainty = outcome_row.get('uncertainty', 0)
                    lower_bound = outcome_row.get('lower_bound', 0)
                    upper_bound = outcome_row.get('upper_bound', 0)
                    
                    # Используем реальное описание прогноза из поля outcome
                    feature_description = self._get_feature_description_from_outcome(feature, outcome)
                    
                    # Определяем правильность прогноза на основе результата матча
                    status_icon = self._determine_prediction_status(feature, outcome, match_id)
                    
                    # Получаем историческую статистику для regular прогнозов
                    forecast_type, forecast_subtype = self._get_forecast_type_subtype_from_feature(feature, outcome)
                    historical_stats = self._get_historical_statistics_regular(forecast_type, forecast_subtype)
                    
                    # Для регрессионных прогнозов добавляем точное значение из predictions
                    regression_info = ''
                    if feature == 8 and regression_data and 'forecast_total_amount' in regression_data:
                        home_goals_val = match_info.get('numOfHeadsHome', 0) or 0
                        away_goals_val = match_info.get('numOfHeadsAway', 0) or 0
                        actual_total = float(home_goals_val) + float(away_goals_val) if home_goals_val is not None and away_goals_val is not None else None
                        regression_info = f' (прогноз: {regression_data["forecast_total_amount"]:.2f}, факт: {actual_total:.1f})' if actual_total is not None else f' (прогноз: {regression_data["forecast_total_amount"]:.2f})'
                    elif feature == 9 and regression_data and 'forecast_total_home_amount' in regression_data:
                        home_goals_val = match_info.get('numOfHeadsHome', 0) or 0
                        actual_home = float(home_goals_val) if home_goals_val is not None else None
                        regression_info = f' (прогноз: {regression_data["forecast_total_home_amount"]:.2f}, факт: {actual_home:.1f})' if actual_home is not None else f' (прогноз: {regression_data["forecast_total_home_amount"]:.2f})'
                    elif feature == 10 and regression_data and 'forecast_total_away_amount' in regression_data:
                        away_goals_val = match_info.get('numOfHeadsAway', 0) or 0
                        actual_away = float(away_goals_val) if away_goals_val is not None else None
                        regression_info = f' (прогноз: {regression_data["forecast_total_away_amount"]:.2f}, факт: {actual_away:.1f})' if actual_away is not None else f' (прогноз: {regression_data["forecast_total_away_amount"]:.2f})'
                    
                    # Расширенная статистика
                    report += f'{status_icon} • {feature_description}: {outcome}{regression_info}\n'
                    report += f'  🎯 Вероятность: {probability:.1%} | 🔒 Уверенность: {confidence:.1%} | 📊 Неопределенность: {uncertainty:.1%}\n'
                    report += f'  📈 Границы: [{lower_bound:.2f} - {upper_bound:.2f}] | ⚖️ Калибровка: {historical_stats["calibration"]:.1%} | 🛡️ Стабильность: {historical_stats["stability"]:.1%}\n'
                    report += f'  Историческая точность: {historical_stats["historical_accuracy"]} | 🔥 Последние 10: {historical_stats["recent_accuracy"]}\n'
                
                report += '\n'
            
            return report
            
        except Exception as e:
            logger.error(f'Ошибка при форматировании отчета с итогами за {date}: {e}')
            return f'❌ Ошибка форматирования отчета с итогами за {date}: {e}'
    
    def _format_daily_quality_outcome_report(self, df_day: pd.DataFrame, date: datetime.date) -> str:
        """
        Форматирует отчет с качественными итогами за день (из таблицы statistics).
        
        Args:
            df_day: DataFrame с данными за день из таблицы statistics
            date: Дата отчета
            
        Returns:
            str: Отформатированный отчет
        """
        try:
            if df_day.empty:
                return ''
            
            report = f'🏁 КАЧЕСТВЕННЫЕ ИТОГИ МАТЧЕЙ - {date.strftime("%d.%m.%Y")}\n\n'
            
            # Группируем по матчам
            for match_id, match_group in df_day.groupby('match_id'):
                match_info = match_group.iloc[0]
                
                # Заголовок матча с match_id
                report += f'🆔 Match ID: {match_id}\n'
                report += f'🏆 {match_info.get("sportName", "Soccer")} - {match_info.get("championshipName", "Unknown")}\n'
                report += f'⚽ {match_info.get("team_home_name", "Home")} vs {match_info.get("team_away_name", "Away")}\n'
                
                # Результат матча
                home_goals = match_info.get('numOfHeadsHome', 0)
                away_goals = match_info.get('numOfHeadsAway', 0)
                report += f'📊 Результат: {home_goals}:{away_goals}\n'
                
                # Время матча
                if 'gameData' in match_info and pd.notna(match_info['gameData']):
                    match_time = pd.to_datetime(match_info['gameData'])
                    report += f'🕐 {match_time.strftime("%H:%M")}\n'
                
                # Прогнозы и их результаты
                for _, stat_row in match_group.iterrows():
                    forecast_type = stat_row.get('forecast_type', 'Unknown')
                    forecast_subtype = stat_row.get('forecast_subtype', 'Unknown')
                    actual_value = stat_row.get('actual_value', 0)
                    prediction_correct = stat_row.get('prediction_correct', False)
                    probability = stat_row.get('probability', 0)
                    confidence = stat_row.get('confidence', 0)
                    uncertainty = stat_row.get('uncertainty', 0)
                    lower_bound = stat_row.get('lower_bound', 0)
                    upper_bound = stat_row.get('upper_bound', 0)
                    
                    # Форматируем описание прогноза с итоговым прогнозом
                    forecast_description = f'{forecast_type.upper()}: {forecast_subtype}'
                    
                    # Статус прогноза
                    status_icon = '✅' if prediction_correct else '❌'
                    
                    # Получаем историческую статистику для quality прогнозов
                    historical_stats = self._get_historical_statistics(forecast_type, forecast_subtype)
                    
                    # Расширенная статистика - выводим forecast_subtype как итоговый прогноз
                    report += f'{status_icon} • {forecast_description}\n'
                    report += f'  🎯 Вероятность: {probability:.1%} | 🔒 Уверенность: {confidence:.1%} | 📊 Неопределенность: {uncertainty:.1%}\n'
                    report += f'  📈 Границы: [{lower_bound:.2f} - {upper_bound:.2f}] | ⚖️ Калибровка: {historical_stats["calibration"]:.1%} | 🛡️ Стабильность: {historical_stats["stability"]:.1%}\n'
                    
                    # Форматируем историческую точность с иконками
                    acc_mark = "📊" if historical_stats.get('historical_accuracy', 0) >= 0.7 else "📉"
                    recent_mark = "🔥" if historical_stats.get('recent_accuracy', 0) >= 0.7 else "❄️"
                    report += f'  {acc_mark} Историческая точность: {historical_stats["historical_correct"]}/{historical_stats["historical_total"]} ({historical_stats["historical_accuracy"]*100:.1f}%)'
                    report += f' | {recent_mark} Последние 10: {historical_stats["recent_correct"]}/10 ({historical_stats["recent_accuracy"]*100:.1f}%)\n'
                
                report += '\n'
            
            return report
            
        except Exception as e:
            logger.error(f'Ошибка при форматировании отчета с качественными итогами за {date}: {e}')
            return f'❌ Ошибка форматирования отчета с качественными итогами за {date}: {e}'
    
    def _get_feature_description(self, feature: int, match_info: dict) -> str:
        """
        Получает описание прогноза по feature коду.
        
        Args:
            feature: Код feature
            match_info: Информация о матче
            
        Returns:
            str: Описание прогноза
        """
        try:
            # Получаем outcome из match_info
            outcome = match_info.get('outcome', '')
            
            # Получаем тип и подтип прогноза
            forecast_type, forecast_subtype = self._get_forecast_type_subtype_from_feature(feature, outcome)
            
            # Формируем описание
            if forecast_type.startswith('Unknown'):
                return f'Unknown Feature {feature}'
            
            # Базовые описания типов прогнозов
            type_descriptions = {
                'WIN_DRAW_LOSS': 'WIN_DRAW_LOSS',
                'OZ': 'OZ (Обе забьют)',
                'GOAL_HOME': 'GOAL_HOME (Гол хозяев)',
                'GOAL_AWAY': 'GOAL_AWAY (Гол гостей)',
                'TOTAL': 'TOTAL (Общий тотал)',
                'TOTAL_HOME': 'TOTAL_HOME (Тотал хозяев)',
                'TOTAL_AWAY': 'TOTAL_AWAY (Тотал гостей)',
                'TOTAL_AMOUNT': 'TOTAL_AMOUNT (Общий тотал)',
                'TOTAL_HOME_AMOUNT': 'TOTAL_HOME_AMOUNT (Тотал хозяев)',
                'TOTAL_AWAY_AMOUNT': 'TOTAL_AWAY_AMOUNT (Тотал гостей)'
            }
            
            description = type_descriptions.get(forecast_type, forecast_type)
            
            # Добавляем подтип
            if forecast_subtype:
                description += f': {forecast_subtype}'
            
            # Для регрессионных прогнозов (8, 9, 10) добавляем значение
            if feature in [8, 9, 10]:
                forecast_value = match_info.get('forecast', 0)
                if forecast_value:
                    description += f' ({forecast_value:.2f})'
            
            return description
            
        except Exception as e:
            logger.error(f'Ошибка при получении описания feature {feature}: {e}')
            return f'Feature {feature}'
    
    def _get_forecast_type_subtype_from_feature(self, feature: int, outcome: str) -> tuple:
        """
        Получает тип и подтип прогноза из feature кода и outcome.
        
        Args:
            feature: Код feature
            outcome: Реальный outcome из таблицы outcomes
            
        Returns:
            tuple: (forecast_type, forecast_subtype)
        """
        try:
            # Базовые типы прогнозов
            feature_types = {
                1: 'WIN_DRAW_LOSS',
                2: 'OZ',
                3: 'GOAL_HOME',
                4: 'GOAL_AWAY',
                5: 'TOTAL',
                6: 'TOTAL_HOME',
                7: 'TOTAL_AWAY',
                8: 'TOTAL_AMOUNT',
                9: 'TOTAL_HOME_AMOUNT',
                10: 'TOTAL_AWAY_AMOUNT'
            }
            
            # Получаем базовый тип
            forecast_type = feature_types.get(feature, f'Unknown Feature {feature}')
            
            # Определяем подтип на основе outcome (русские названия)
            if outcome and outcome != 'Unknown':
                outcome_lower = outcome.lower().strip()
                
                # Для WIN_DRAW_LOSS (feature 1)
                if feature == 1:
                    if 'п1' in outcome_lower:
                        return (forecast_type, 'П1')
                    elif 'х' in outcome_lower:
                        return (forecast_type, 'X')
                    elif 'п2' in outcome_lower:
                        return (forecast_type, 'П2')
                
                # Для OZ (feature 2)
                elif feature == 2:
                    if 'да' in outcome_lower:
                        return (forecast_type, 'ДА')
                    elif 'нет' in outcome_lower:
                        return (forecast_type, 'НЕТ')
                
                # Для GOAL_HOME (feature 3)
                elif feature == 3:
                    if 'да' in outcome_lower:
                        return (forecast_type, 'ДА')
                    elif 'нет' in outcome_lower:
                        return (forecast_type, 'НЕТ')
                
                # Для GOAL_AWAY (feature 4)
                elif feature == 4:
                    if 'да' in outcome_lower:
                        return (forecast_type, 'ДА')
                    elif 'нет' in outcome_lower:
                        return (forecast_type, 'НЕТ')
                
                # Для TOTAL (feature 5)
                elif feature == 5:
                    if 'тб' in outcome_lower or 'больше' in outcome_lower:
                        return (forecast_type, 'БОЛЬШЕ')
                    elif 'тм' in outcome_lower or 'меньше' in outcome_lower:
                        return (forecast_type, 'МЕНЬШЕ')
                
                # Для TOTAL_HOME (feature 6)
                elif feature == 6:
                    if 'ит1б' in outcome_lower or 'больше' in outcome_lower:
                        return (forecast_type, 'БОЛЬШЕ')
                    elif 'ит1м' in outcome_lower or 'меньше' in outcome_lower:
                        return (forecast_type, 'МЕНЬШЕ')
                
                # Для TOTAL_AWAY (feature 7)
                elif feature == 7:
                    if 'ит2б' in outcome_lower or 'больше' in outcome_lower:
                        return (forecast_type, 'БОЛЬШЕ')
                    elif 'ит2м' in outcome_lower or 'меньше' in outcome_lower:
                        return (forecast_type, 'МЕНЬШЕ')
                
                # Для регрессионных прогнозов (8, 9, 10) - возвращаем как есть в UPPERCASE для отображения
                # НО для получения статистики нужно использовать lowercase версию
                elif feature in [8, 9, 10]:
                    # Для регрессионных моделей нужно преобразовать outcome в категорию
                    # если это числовое значение (уже должно быть преобразовано в БД)
                    return (forecast_type, outcome.upper())
            
            # Если не удалось определить подтип, возвращаем базовый тип
            return (forecast_type, outcome.upper() if outcome else 'UNKNOWN')
            
        except Exception as e:
            logger.error(f'Ошибка при определении типа прогноза для feature {feature}, outcome {outcome}: {e}')
            return (f'Unknown Feature {feature}', 'UNKNOWN')

    def _get_feature_description_from_outcome(self, feature: int, outcome: str) -> str:
        """
        Получает описание прогноза по feature коду и реальному outcome.
        
        Args:
            feature: Код feature
            outcome: Реальный outcome из таблицы outcomes
            
        Returns:
            str: Описание прогноза
        """
        try:
            # Получаем тип и подтип прогноза
            forecast_type, forecast_subtype = self._get_forecast_type_subtype_from_feature(feature, outcome)
            
            # Формируем описание
            if forecast_type.startswith('Unknown'):
                return f'Unknown Feature {feature}'
            
            # Базовые описания типов прогнозов
            type_descriptions = {
                'WIN_DRAW_LOSS': 'WIN_DRAW_LOSS',
                'OZ': 'OZ (Обе забьют)',
                'GOAL_HOME': 'GOAL_HOME (Гол хозяев)',
                'GOAL_AWAY': 'GOAL_AWAY (Гол гостей)',
                'TOTAL': 'TOTAL (Общий тотал)',
                'TOTAL_HOME': 'TOTAL_HOME (Тотал хозяев)',
                'TOTAL_AWAY': 'TOTAL_AWAY (Тотал гостей)',
                'TOTAL_AMOUNT': 'TOTAL_AMOUNT (Общий тотал)',
                'TOTAL_HOME_AMOUNT': 'TOTAL_HOME_AMOUNT (Тотал хозяев)',
                'TOTAL_AWAY_AMOUNT': 'TOTAL_AWAY_AMOUNT (Тотал гостей)'
            }
            
            description = type_descriptions.get(forecast_type, forecast_type)
            
            # Добавляем подтип
            if forecast_subtype:
                description += f': {forecast_subtype}'
            
            return description
            
        except Exception as e:
            logger.error(f'Ошибка при получении описания feature {feature} с outcome {outcome}: {e}')
            return f'Feature {feature}'
    
    def _get_regression_data_for_match(self, match_id: int) -> Optional[Dict[str, float]]:
        """
        Получает данные регрессии из таблицы predictions для матча.
        
        Args:
            match_id: ID матча
            
        Returns:
            Optional[Dict[str, float]]: Словарь с данными регрессии или None
        """
        try:
            with Session_pool() as session:
                prediction = session.query(Prediction).filter(Prediction.match_id == match_id).first()
                if prediction:
                    return {
                        'forecast_total_amount': prediction.forecast_total_amount,
                        'forecast_total_home_amount': prediction.forecast_total_home_amount,
                        'forecast_total_away_amount': prediction.forecast_total_away_amount
                    }
                return None
        except Exception as e:
            logger.error(f'Ошибка при получении данных регрессии для матча {match_id}: {e}')
            return None
    
    def _determine_prediction_status(self, feature: int, outcome: str, match_id: int) -> str:
        """
        Определяет правильность прогноза на основе target из БД.
        
        Args:
            feature: Код feature (1-10)
            outcome: Прогноз из таблицы outcomes
            match_id: ID матча
            
        Returns:
            str: ✅, ❌ или ⏳ (если матч еще не состоялся)
        """
        try:
            target = get_target_by_match_id(match_id)
            return get_prediction_status_from_target(feature, outcome, target)
            
        except Exception as e:
            logger.error(f'Ошибка при определении статуса прогноза для feature {feature}, match {match_id}: {e}')
            return '❌'

    def _get_historical_statistics(self, forecast_type: str, forecast_subtype: str) -> Dict[str, Any]:
        """
        Получает историческую статистику для типа прогноза из БД.
        
        Args:
            forecast_type: Тип прогноза
            forecast_subtype: Подтип прогноза
            
        Returns:
            Dict[str, Any]: Словарь с исторической статистикой из реальных данных БД
        """
        try:
            # Получаем реальную статистику из БД
            stats = get_complete_statistics(forecast_type, forecast_subtype)
            return stats
            
        except Exception as e:
            logger.error(f'Ошибка при получении исторической статистики из БД: {e}')
            # Возвращаем минимальные значения при ошибке
            return {
                'calibration': 0.75,
                'stability': 0.80,
                'confidence': 0.78,
                'uncertainty': 0.22,
                'lower_bound': 0.50,
                'upper_bound': 0.80,
                'historical_correct': 0,
                'historical_total': 0,
                'historical_accuracy': 0.0,
                'recent_correct': 0,
                'recent_accuracy': 0.0
            }

    def _calculate_match_quality(self, match_group: pd.DataFrame) -> float:
        """
        Рассчитывает качество прогноза для матча.
        
        Args:
            match_group: DataFrame с прогнозами для матча
            
        Returns:
            float: Качество прогноза от 0 до 10
        """
        try:
            if match_group.empty:
                return 0.0
            
            # Считаем количество правильных прогнозов
            correct_predictions = match_group['prediction_correct'].sum()
            total_predictions = len(match_group)
            
            # Базовое качество на основе точности
            base_quality = (correct_predictions / total_predictions) * 10
            
            # Бонус за высокую уверенность
            avg_confidence = match_group['confidence'].mean()
            confidence_bonus = avg_confidence * 0.5
            
            # Бонус за низкую неопределенность
            avg_uncertainty = match_group['uncertainty'].mean()
            uncertainty_bonus = (1 - avg_uncertainty) * 0.3
            
            # Итоговое качество
            final_quality = min(10.0, base_quality + confidence_bonus + uncertainty_bonus)
            
            return final_quality
            
        except Exception as e:
            logger.error(f'Ошибка при расчете качества матча: {e}')
            return 5.0

    def _get_best_worst_predictions(self, match_group: pd.DataFrame) -> Dict[str, str]:
        """
        Определяет лучший и худший прогнозы для матча.
        
        Args:
            match_group: DataFrame с прогнозами для матча
            
        Returns:
            Dict[str, str]: Словарь с лучшим и худшим прогнозами
        """
        try:
            if match_group.empty:
                return {'best': 'N/A', 'worst': 'N/A'}
            
            # Сортируем по точности и уверенности
            sorted_group = match_group.sort_values(['prediction_correct', 'confidence'], ascending=[False, False])
            
            # Лучший прогноз
            best_row = sorted_group.iloc[0]
            best_type = best_row.get('forecast_type', 'Unknown')
            best_subtype = best_row.get('forecast_subtype', '')
            best_display = self._format_forecast_type(best_type, best_subtype, best_row.get('actual_value', ''))
            
            # Худший прогноз
            worst_row = sorted_group.iloc[-1]
            worst_type = worst_row.get('forecast_type', 'Unknown')
            worst_subtype = worst_row.get('forecast_subtype', '')
            worst_display = self._format_forecast_type(worst_type, worst_subtype, worst_row.get('actual_value', ''))
            
            return {
                'best': f'{best_type.upper()}: {best_display}',
                'worst': f'{worst_type.upper()}: {worst_display}'
            }
            
        except Exception as e:
            logger.error(f'Ошибка при определении лучшего/худшего прогноза: {e}')
            return {'best': 'N/A', 'worst': 'N/A'}

    def _calculate_daily_accuracy(self, df_day: pd.DataFrame) -> float:
        """
        Рассчитывает среднюю точность за день.
        
        Args:
            df_day: DataFrame с данными за день
            
        Returns:
            float: Средняя точность в процентах
        """
        try:
            if df_day.empty:
                return 0.0
            
            # Считаем общую точность
            correct_predictions = df_day['prediction_correct'].sum()
            total_predictions = len(df_day)
            
            return correct_predictions / total_predictions if total_predictions > 0 else 0.0
            
        except Exception as e:
            logger.error(f'Ошибка при расчете дневной точности: {e}')
            return 0.0

    def _get_historical_statistics_regular(self, forecast_type: str, forecast_subtype: str) -> Dict[str, Any]:
        """
        Получает историческую статистику для regular прогнозов по типу и подтипу прогноза.
        
        Args:
            forecast_type: Тип прогноза (WIN_DRAW_LOSS, OZ, TOTAL, etc.)
            forecast_subtype: Подтип прогноза (П1, X, П2, ДА, НЕТ, etc.)
            
        Returns:
            Dict[str, Any]: Словарь с исторической статистикой
        """
        try:
            # Получаем реальные данные из БД
            hist = get_historical_accuracy_regular(forecast_type, forecast_subtype)
            recent = get_recent_accuracy(forecast_type, forecast_subtype, limit=10)
            calibration = get_calibration(forecast_type, forecast_subtype)
            stability = get_stability(forecast_type, forecast_subtype)
            bounds = get_confidence_bounds(forecast_type, forecast_subtype)
            
            return {
                'calibration': calibration,
                'stability': stability,
                'confidence': bounds['confidence'],
                'uncertainty': bounds['uncertainty'],
                'lower_bound': bounds['lower_bound'],
                'upper_bound': bounds['upper_bound'],
                'historical_accuracy': hist['formatted'],
                'recent_accuracy': recent['formatted']
            }
            
        except Exception as e:
            logger.error(f'Ошибка при получении исторической статистики для {forecast_type}/{forecast_subtype} из БД: {e}')
            return {
                'calibration': 0.75,
                'stability': 0.80,
                'confidence': 0.78,
                'uncertainty': 0.22,
                'lower_bound': 0.50,
                'upper_bound': 0.90,
                'historical_accuracy': '0/0 (0.0%)',
                'recent_accuracy': '0/10 (0.0%)'
            }

    def _calculate_match_quality_regular(self, match_group: pd.DataFrame) -> float:
        """
        Рассчитывает качество прогноза для regular матча.
        
        Args:
            match_group: DataFrame с прогнозами для матча
            
        Returns:
            float: Качество прогноза от 0 до 10
        """
        try:
            if match_group.empty:
                return 0.0
            
            # Считаем количество правильных прогнозов на основе статуса
            correct_count = 0
            total_count = len(match_group)
            
            for _, row in match_group.iterrows():
                feature = row.get('feature', 0)
                outcome = row.get('outcome', '')
                match_id = row.get('match_id', row.get('id', 0))
                
                # Определяем правильность прогноза
                status = self._determine_prediction_status(feature, outcome, match_id)
                if status == '✅':
                    correct_count += 1
            
            # Базовое качество на основе точности
            base_quality = (correct_count / total_count) * 10 if total_count > 0 else 0
            
            # Бонус за высокую уверенность
            avg_confidence = match_group['confidence'].mean()
            confidence_bonus = avg_confidence * 0.5
            
            # Бонус за низкую неопределенность
            avg_uncertainty = match_group['uncertainty'].mean()
            uncertainty_bonus = (1 - avg_uncertainty) * 0.3
            
            # Итоговое качество
            final_quality = min(10.0, base_quality + confidence_bonus + uncertainty_bonus)
            
            return final_quality
            
        except Exception as e:
            logger.error(f'Ошибка при расчете качества regular матча: {e}')
            return 5.0

    def _get_best_worst_predictions_regular(self, match_group: pd.DataFrame) -> Dict[str, str]:
        """
        Определяет лучший и худший прогнозы для regular матча.
        
        Args:
            match_group: DataFrame с прогнозами для матча
            
        Returns:
            Dict[str, str]: Словарь с лучшим и худшим прогнозами
        """
        try:
            if match_group.empty:
                return {'best': 'N/A', 'worst': 'N/A'}
            
            # Сортируем по уверенности
            sorted_group = match_group.sort_values('confidence', ascending=False)
            
            # Лучший прогноз
            best_row = sorted_group.iloc[0]
            best_feature = best_row.get('feature', 0)
            best_outcome = best_row.get('outcome', '')
            best_description = self._get_feature_description_from_outcome(best_feature, best_outcome)
            
            # Худший прогноз
            worst_row = sorted_group.iloc[-1]
            worst_feature = worst_row.get('feature', 0)
            worst_outcome = worst_row.get('outcome', '')
            worst_description = self._get_feature_description_from_outcome(worst_feature, worst_outcome)
            
            return {
                'best': best_description,
                'worst': worst_description
            }
            
        except Exception as e:
            logger.error(f'Ошибка при определении лучшего/худшего regular прогноза: {e}')
            return {'best': 'N/A', 'worst': 'N/A'}

    def _calculate_daily_accuracy_regular(self, df_day: pd.DataFrame) -> float:
        """
        Рассчитывает среднюю точность за день для regular прогнозов.
        
        Args:
            df_day: DataFrame с данными за день
            
        Returns:
            float: Средняя точность в процентах
        """
        try:
            if df_day.empty:
                return 0.0
            
            # Считаем общую точность на основе статуса прогнозов
            correct_count = 0
            total_count = len(df_day)
            
            for _, row in df_day.iterrows():
                feature = row.get('feature', default=0)
                outcome = row.get('outcome', default='')
                match_id = row.get('match_id', row.get('id', default=0))
                
                # Определяем правильность прогноза
                status = self._determine_prediction_status(feature, outcome, match_id)
                if status == '✅':
                    correct_count += 1
            
            return correct_count / total_count if total_count > 0 else 0.0
            
        except Exception as e:
            logger.error(f'Ошибка при расчете дневной точности для regular прогнозов: {e}')
            return 0.0


def create_statistics_publisher() -> StatisticsPublisher:
    """
    Создает и настраивает публикатор статистики.
    
    Returns:
        StatisticsPublisher: Настроенный публикатор
    """
    return StatisticsPublisher()
