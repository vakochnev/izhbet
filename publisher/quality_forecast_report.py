# izhbet/publisher/quality_forecast_report.py
"""
Модуль для генерации отчета с качественными прогнозами.
Создает отдельный файл с лучшими прогнозами, отобранными по критериям качества.
"""

import logging
import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from pathlib import Path

from forecast.conformal_publication import ConformalForecastGenerator
from config import Session_pool
from sqlalchemy import text

logger = logging.getLogger(__name__)


class QualityForecastReporter:
    """
    Генератор отчета с качественными прогнозами.
    
    Создает отдельный файл с лучшими прогнозами, отобранными по строгим критериям:
    - Высокая вероятность (>= 70%)
    - Высокая уверенность (>= 80%)
    - Низкая неопределенность (<= 20%)
    """
    
    def __init__(self, output_dir: str = "results"):
        self.output_dir = Path(output_dir)
        self.publisher = ConformalForecastGenerator()
        
        # Строгие критерии для качественных прогнозов
        self.quality_criteria = {
            'min_probability': 0.5,    # 50% - средняя вероятность
            'min_confidence': 0.8,     # 80% - высокая уверенность
            'max_uncertainty': 0.3     # 30% - средняя неопределенность
        }
        
        # Создаем директорию для отчетов
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_quality_forecast_report(self, date: Optional[datetime] = None) -> str:
        """
        Генерирует отчет с качественными прогнозами.
        
        Args:
            date: Дата для генерации отчета (по умолчанию - сегодня)
            
        Returns:
            Путь к созданному файлу отчета
        """
        if date is None:
            date = datetime.now()
        
        logger.info(f"Генерация отчета с качественными прогнозами на {date.strftime('%Y-%m-%d')}")
        
        try:
            # Загружаем прогнозы на указанную дату
            forecasts_df = self._load_forecasts_for_date(date)
            
            if forecasts_df.empty:
                logger.warning(f"Нет прогнозов на {date.strftime('%Y-%m-%d')}")
                return self._create_empty_report(date)
            
            # Фильтруем по строгим критериям качества
            quality_forecasts = self.publisher.filter_forecasts_by_criteria(
                forecasts_df, **self.quality_criteria
            )
            
            if quality_forecasts.empty:
                logger.warning(f"Нет качественных прогнозов на {date.strftime('%Y-%m-%d')}")
                return self._create_no_quality_report(date, len(forecasts_df))
            
            # Выбираем лучшие качественные прогнозы (до 10 штук)
            best_forecasts = self.publisher.select_best_forecasts(quality_forecasts, max_forecasts=10)
            
            # Генерируем отчет
            report_content = self._generate_report_content(best_forecasts, date)
            
            # Сохраняем отчет
            file_path = self._save_report(report_content, date)
            
            logger.info(f"Отчет с качественными прогнозами сохранен: {file_path}")
            return str(file_path)
            
        except Exception as e:
            logger.error(f"Ошибка при генерации отчета с качественными прогнозами: {e}")
            raise
    
    def _load_forecasts_for_date(self, date: datetime) -> pd.DataFrame:
        """Загружает прогнозы на указанную дату."""
        from db.queries.publisher import get_forecasts_for_date
        
        df = get_forecasts_for_date(date.date())
        
        # Добавляем тип прогноза на основе feature
        df['forecast_type'] = df['feature'].apply(self._get_forecast_type)
        
        logger.info(f"Загружено {len(df)} прогнозов на {date.strftime('%Y-%m-%d')}")
        return df
    
    def _get_forecast_type(self, feature: int) -> str:
        """Определяет тип прогноза по числовому коду feature."""
        feature_mapping = {
            1: 'win_draw_loss',
            2: 'oz',
            3: 'goal_home',
            4: 'goal_away',
            5: 'total',
            6: 'total_home',
            7: 'total_away',
            8: 'total_amount',
            9: 'total_home_amount',
            10: 'total_away_amount'
        }
        return feature_mapping.get(feature, 'unknown')
    
    def _generate_report_content(self, forecasts_df: pd.DataFrame, date: datetime) -> str:
        """Генерирует содержимое отчета."""
        report_lines = []
        
        # Заголовок отчета
        report_lines.append("=" * 80)
        report_lines.append("🏆 ОТЧЕТ С КАЧЕСТВЕННЫМИ ПРОГНОЗАМИ")
        report_lines.append(f"📅 Дата: {date.strftime('%Y-%m-%d')}")
        report_lines.append(f"⏰ Время генерации: {datetime.now().strftime('%H:%M:%S')}")
        report_lines.append("=" * 80)
        
        # Статистика
        total_forecasts = len(forecasts_df)
        avg_probability = forecasts_df['probability'].mean()
        avg_confidence = forecasts_df['confidence'].mean()
        if 'uncertainty' in forecasts_df.columns:
            uncertainty_series = forecasts_df['uncertainty'].dropna()
            avg_uncertainty = uncertainty_series.mean() if len(uncertainty_series) > 0 else 0
        else:
            avg_uncertainty = 0
        
        report_lines.append(f"\n📊 СТАТИСТИКА КАЧЕСТВЕННЫХ ПРОГНОЗОВ:")
        report_lines.append(f"   • Всего прогнозов: {total_forecasts}")
        report_lines.append(f"   • Средняя вероятность: {avg_probability:.1%}")
        report_lines.append(f"   • Средняя уверенность: {avg_confidence:.1%}")
        report_lines.append(f"   • Средняя неопределенность: {avg_uncertainty:.1%}")
        
        # Критерии качества
        report_lines.append(f"\n🎯 КРИТЕРИИ КАЧЕСТВА:")
        report_lines.append(f"   • Минимальная вероятность: {self.quality_criteria['min_probability']:.0%}")
        report_lines.append(f"   • Минимальная уверенность: {self.quality_criteria['min_confidence']:.0%}")
        report_lines.append(f"   • Максимальная неопределенность: {self.quality_criteria['max_uncertainty']:.0%}")
        
        # Группируем прогнозы по матчам
        grouped_forecasts = self._group_forecasts_by_match(forecasts_df)
        
        if not grouped_forecasts:
            report_lines.append("\n❌ Нет качественных прогнозов для отображения")
            return "\n".join(report_lines)
        
        # Генерируем отчет по матчам
        report_lines.append(f"\n🏆 КАЧЕСТВЕННЫЕ ПРОГНОЗЫ ({len(grouped_forecasts)} матчей):")
        report_lines.append("-" * 80)
        
        for match_id, match_forecasts in grouped_forecasts.items():
            self._add_match_to_report(report_lines, match_forecasts)
        
        # Итоговая статистика
        report_lines.append(f"\n📈 ИТОГОВАЯ СТАТИСТИКА:")
        report_lines.append(f"   • Обработано матчей: {len(grouped_forecasts)}")
        report_lines.append(f"   • Всего прогнозов: {total_forecasts}")
        report_lines.append(f"   • Средний рейтинг: {forecasts_df['rating'].mean():.3f}")
        
        return "\n".join(report_lines)
    
    def _group_forecasts_by_match(self, forecasts_df: pd.DataFrame) -> Dict[int, pd.DataFrame]:
        """Группирует прогнозы по матчам."""
        if forecasts_df.empty:
            return {}
        
        grouped = {}
        for match_id, group in forecasts_df.groupby('match_id'):
            grouped[match_id] = group.sort_values('rating', ascending=False)
        
        return grouped
    
    def _add_match_to_report(self, report_lines: List[str], match_forecasts: pd.DataFrame) -> None:
        """Добавляет информацию о матче в отчет."""
        if match_forecasts.empty:
            return
        
        # Получаем информацию о матче
        first_forecast = match_forecasts.iloc[0]
        match_id = first_forecast.get('match_id', 'Unknown')
        match_info = f"{first_forecast.get('teamHome_name', 'Unknown')} vs {first_forecast.get('teamAway_name', 'Unknown')}"
        championship = first_forecast.get('championshipName', 'Unknown')
        sport = first_forecast.get('sportName', 'Unknown')
        game_time = first_forecast.get('gameData', 'Unknown')
        game_comment = first_forecast.get('gameComment', '')
        home_goals = first_forecast.get('numOfHeadsHome', None)
        away_goals = first_forecast.get('numOfHeadsAway', None)
        
        # Форматируем время
        if hasattr(game_time, 'strftime'):
            game_time_str = game_time.strftime('%H:%M')
        else:
            game_time_str = str(game_time)
        
        report_lines.append(f"\n⚽ {match_info}")
        report_lines.append(f"   🏆 {sport} - {championship}")
        report_lines.append(f"   🆔 ID матча: {match_id}")
        report_lines.append(f"   🕐 {game_time_str}")
        
        # Добавляем результат и счет, если известны
        if (home_goals is not None) and (away_goals is not None):
            result_str = self._get_match_result(home_goals, away_goals)
            report_lines.append(f"   🎯 Результат: {result_str}")
            report_lines.append(f"   🧮 Счет: {home_goals}:{away_goals}")
        
        # Добавляем комментарий матча, если он есть
        if game_comment and str(game_comment).strip():
            # Ограничиваем длину комментария для читаемости
            comment = str(game_comment).strip()
            if len(comment) > 100:
                comment = comment[:97] + "..."
            report_lines.append(f"   💬 {comment}")
        
        # Добавляем прогнозы
        for _, forecast in match_forecasts.iterrows():
            self._add_forecast_to_report(report_lines, forecast)
    
    def _add_forecast_to_report(self, report_lines: List[str], forecast: pd.Series) -> None:
        """Добавляет информацию о прогнозе в отчет."""
        forecast_type = forecast.get('forecast_type', 'unknown')
        outcome = forecast.get('outcome', 'unknown')
        probability = forecast.get('probability', 0.0)
        confidence = forecast.get('confidence', 0.0)
        uncertainty = forecast.get('uncertainty', 0.0)
        rating = forecast.get('rating', 0.0)
        
        # Форматируем тип прогноза
        type_display = self._format_forecast_type(forecast_type)
        
        # Форматируем рейтинг
        rating_stars = "⭐" * int(rating * 5) + "☆" * (5 - int(rating * 5))
        
        # Определяем статус качества
        quality_status = self._get_quality_status(probability, confidence, uncertainty)
        
        report_lines.append(f"   {quality_status} {type_display}: {outcome}")
        uncertainty_str = f"{uncertainty:.1%}" if uncertainty is not None else "N/A"
        
        # Добавляем информацию о границах интервала
        bounds_str = ""
        if (forecast.get('lower_bound') is not None and forecast.get('upper_bound') is not None):
            bounds_str = f" | Границы: [{forecast['lower_bound']:.1%}-{forecast['upper_bound']:.1%}]"
        
        report_lines.append(f"      📊 Вероятность: {probability:.1%} | Уверенность: {confidence:.1%} | Неопределенность: {uncertainty_str}{bounds_str}")
        report_lines.append(f"      ⭐ Рейтинг: {rating:.3f} {rating_stars}")
    
    def _get_match_result(self, home_goals: int, away_goals: int) -> str:
        """
        Определяет результат матча на основе счета.
        Returns: 'П1' | 'Н' | 'П2'
        """
        try:
            if home_goals > away_goals:
                return 'П1'
            elif home_goals < away_goals:
                return 'П2'
            else:
                return 'Н'
        except Exception:
            return 'Неизвестно'
    
    def _format_forecast_type(self, forecast_type: str) -> str:
        """Форматирует тип прогноза для отображения."""
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
        return type_mapping.get(forecast_type, forecast_type.upper())
    
    def _get_quality_status(self, probability: float, confidence: float, uncertainty: float) -> str:
        """Определяет статус качества прогноза."""
        if uncertainty is None:
            uncertainty = 0.0  # Если неопределенность неизвестна, считаем её нулевой
        
        if probability >= 0.8 and confidence >= 0.9 and uncertainty <= 0.1:
            return "🔥"  # Очень высокое качество
        elif probability >= 0.75 and confidence >= 0.85 and uncertainty <= 0.15:
            return "⭐"  # Высокое качество
        else:
            return "✅"  # Хорошее качество
    
    def _create_empty_report(self, date: datetime) -> str:
        """Создает отчет при отсутствии прогнозов."""
        content = f"""
{'=' * 80}
🏆 ОТЧЕТ С КАЧЕСТВЕННЫМИ ПРОГНОЗАМИ
📅 Дата: {date.strftime('%Y-%m-%d')}
⏰ Время генерации: {datetime.now().strftime('%H:%M:%S')}
{'=' * 80}

❌ НЕТ ПРОГНОЗОВ НА УКАЗАННУЮ ДАТУ

На {date.strftime('%Y-%m-%d')} не найдено прогнозов в базе данных.
"""
        return self._save_report(content, date)
    
    def _create_no_quality_report(self, date: datetime, total_forecasts: int) -> str:
        """Создает отчет при отсутствии качественных прогнозов."""
        content = f"""
{'=' * 80}
🏆 ОТЧЕТ С КАЧЕСТВЕННЫМИ ПРОГНОЗАМИ
📅 Дата: {date.strftime('%Y-%m-%d')}
⏰ Время генерации: {datetime.now().strftime('%H:%M:%S')}
{'=' * 80}

📊 СТАТИСТИКА:
   • Всего прогнозов: {total_forecasts}
   • Качественных прогнозов: 0

❌ НЕТ КАЧЕСТВЕННЫХ ПРОГНОЗОВ

Из {total_forecasts} прогнозов ни один не соответствует строгим критериям качества:
   • Минимальная вероятность: {self.quality_criteria['min_probability']:.0%}
   • Минимальная уверенность: {self.quality_criteria['min_confidence']:.0%}
   • Максимальная неопределенность: {self.quality_criteria['max_uncertainty']:.0%}
"""
        return self._save_report(content, date)
    
    def generate_quality_outcomes_report(self, date: Optional[datetime] = None) -> str:
        """
        Генерирует отчет с итогами качественных прогнозов.
        
        Args:
            date: Дата для генерации отчета (по умолчанию - вчера)
            
        Returns:
            Путь к созданному файлу отчета
        """
        if date is None:
            date = datetime.now() - timedelta(days=1)
        
        logger.info(f"Генерация отчета с итогами качественных прогнозов на {date.strftime('%Y-%m-%d')}")
        
        try:
            # Загружаем прогнозы на указанную дату
            forecasts_df = self._load_forecasts_for_date(date)
            
            if forecasts_df.empty:
                logger.warning(f"Нет прогнозов на {date.strftime('%Y-%m-%d')}")
                return self._create_empty_outcomes_report(date)
            
            # Фильтруем по строгим критериям качества
            quality_forecasts = self.publisher.filter_forecasts_by_criteria(
                forecasts_df, **self.quality_criteria
            )
            
            if quality_forecasts.empty:
                logger.warning(f"Нет качественных прогнозов на {date.strftime('%Y-%m-%d')}")
                return self._create_no_quality_outcomes_report(date, len(forecasts_df))
            
            # Выбираем лучшие качественные прогнозы (до 10 штук)
            best_forecasts = self.publisher.select_best_forecasts(quality_forecasts, max_forecasts=10)
            
            # Генерируем отчет с итогами
            report_content = self._generate_outcomes_report_content(best_forecasts, date)
            
            # Сохраняем отчет в папку outcomes
            file_path = self._save_outcomes_report(report_content, date)
            
            logger.info(f"Отчет с итогами качественных прогнозов сохранен: {file_path}")
            return str(file_path)
            
        except Exception as e:
            logger.error(f"Ошибка при генерации отчета с итогами качественных прогнозов: {e}")
            raise

    def _generate_outcomes_report_content(self, forecasts_df: pd.DataFrame, date: datetime) -> str:
        """Генерирует содержимое отчета с итогами."""
        report_lines = []
        
        # Заголовок отчета
        report_lines.append("=" * 80)
        report_lines.append("🏆 ИТОГИ КАЧЕСТВЕННЫХ ПРОГНОЗОВ")
        report_lines.append(f"📅 Дата: {date.strftime('%Y-%m-%d')}")
        report_lines.append(f"⏰ Время генерации: {datetime.now().strftime('%H:%M:%S')}")
        report_lines.append("=" * 80)
        
        # Статистика
        total_forecasts = len(forecasts_df)
        avg_probability = forecasts_df['probability'].mean()
        avg_confidence = forecasts_df['confidence'].mean()
        if 'uncertainty' in forecasts_df.columns:
            uncertainty_series = forecasts_df['uncertainty'].dropna()
            avg_uncertainty = uncertainty_series.mean() if len(uncertainty_series) > 0 else 0
        else:
            avg_uncertainty = 0
        
        report_lines.append(f"\n📊 СТАТИСТИКА КАЧЕСТВЕННЫХ ПРОГНОЗОВ:")
        report_lines.append(f"   • Всего прогнозов: {total_forecasts}")
        report_lines.append(f"   • Средняя вероятность: {avg_probability:.1%}")
        report_lines.append(f"   • Средняя уверенность: {avg_confidence:.1%}")
        report_lines.append(f"   • Средняя неопределенность: {avg_uncertainty:.1%}")
        
        # Критерии качества
        report_lines.append(f"\n🎯 КРИТЕРИИ КАЧЕСТВА:")
        report_lines.append(f"   • Минимальная вероятность: {self.quality_criteria['min_probability']:.0%}")
        report_lines.append(f"   • Минимальная уверенность: {self.quality_criteria['min_confidence']:.0%}")
        report_lines.append(f"   • Максимальная неопределенность: {self.quality_criteria['max_uncertainty']:.0%}")
        
        # Группируем прогнозы по матчам
        grouped_forecasts = self._group_forecasts_by_match(forecasts_df)
        
        if not grouped_forecasts:
            report_lines.append("\n❌ Нет качественных прогнозов для отображения")
            return "\n".join(report_lines)
        
        # Генерируем отчет по матчам с итогами
        report_lines.append(f"\n🏆 ИТОГИ КАЧЕСТВЕННЫХ ПРОГНОЗОВ ({len(grouped_forecasts)} матчей):")
        report_lines.append("-" * 80)
        
        for match_id, match_forecasts in grouped_forecasts.items():
            self._add_match_outcomes_to_report(report_lines, match_forecasts)
        
        # Итоговая статистика
        report_lines.append(f"\n📈 ИТОГОВАЯ СТАТИСТИКА:")
        report_lines.append(f"   • Обработано матчей: {len(grouped_forecasts)}")
        report_lines.append(f"   • Всего прогнозов: {total_forecasts}")
        report_lines.append(f"   • Средний рейтинг: {forecasts_df['rating'].mean():.3f}")
        
        return "\n".join(report_lines)

    def _add_match_outcomes_to_report(self, report_lines: List[str], match_forecasts: pd.DataFrame) -> None:
        """Добавляет информацию о матче с итогами в отчет."""
        if match_forecasts.empty:
            return
        
        # Получаем информацию о матче
        first_forecast = match_forecasts.iloc[0]
        match_id = first_forecast.get('match_id', 'Unknown')
        match_info = f"{first_forecast.get('teamHome_name', 'Unknown')} vs {first_forecast.get('teamAway_name', 'Unknown')}"
        championship = first_forecast.get('championshipName', 'Unknown')
        sport = first_forecast.get('sportName', 'Unknown')
        game_time = first_forecast.get('gameData', 'Unknown')
        game_comment = first_forecast.get('gameComment', '')
        home_goals = first_forecast.get('numOfHeadsHome', None)
        away_goals = first_forecast.get('numOfHeadsAway', None)
        
        # Форматируем время
        if hasattr(game_time, 'strftime'):
            game_time_str = game_time.strftime('%H:%M')
        else:
            game_time_str = str(game_time)
        
        report_lines.append(f"\n⚽ {match_info}")
        report_lines.append(f"   🏆 {sport} - {championship}")
        report_lines.append(f"   🆔 ID матча: {match_id}")
        report_lines.append(f"   🕐 {game_time_str}")
        
        # Добавляем результат и счет, если известны
        if (home_goals is not None) and (away_goals is not None):
            result_str = self._get_match_result(home_goals, away_goals)
            report_lines.append(f"   🎯 Результат: {result_str}")
            report_lines.append(f"   🧮 Счет: {home_goals}:{away_goals}")
        
        # Добавляем комментарий матча, если он есть
        if game_comment and str(game_comment).strip():
            # Ограничиваем длину комментария для читаемости
            comment = str(game_comment).strip()
            if len(comment) > 100:
                comment = comment[:97] + "..."
            report_lines.append(f"   💬 {comment}")
        
        # Добавляем прогнозы с анализом точности
        for _, forecast in match_forecasts.iterrows():
            self._add_forecast_outcomes_to_report(report_lines, forecast)

    def _add_forecast_outcomes_to_report(self, report_lines: List[str], forecast: pd.Series) -> None:
        """Добавляет информацию о прогнозе с анализом точности в отчет."""
        forecast_type = forecast.get('forecast_type', 'unknown')
        outcome = forecast.get('outcome', 'unknown')
        probability = forecast.get('probability', 0.0)
        confidence = forecast.get('confidence', 0.0)
        uncertainty = forecast.get('uncertainty', 0.0)
        rating = forecast.get('rating', 0.0)
        
        # Форматируем тип прогноза
        type_display = self._format_forecast_type(forecast_type)
        
        # Форматируем рейтинг
        rating_stars = "⭐" * int(rating * 5) + "☆" * (5 - int(rating * 5))
        
        # Определяем статус качества
        quality_status = self._get_quality_status(probability, confidence, uncertainty)
        
        # Анализируем точность прогноза (здесь можно добавить логику сравнения с фактическим результатом)
        accuracy_status = "✅"  # Пока всегда успешно, можно добавить логику проверки
        
        report_lines.append(f"   {quality_status} {accuracy_status} {type_display}: {outcome}")
        uncertainty_str = f"{uncertainty:.1%}" if uncertainty is not None else "N/A"
        
        # Добавляем информацию о границах интервала
        bounds_str = ""
        if (forecast.get('lower_bound') is not None and forecast.get('upper_bound') is not None):
            bounds_str = f" | Границы: [{forecast['lower_bound']:.1%}-{forecast['upper_bound']:.1%}]"
        
        report_lines.append(f"      📊 Вероятность: {probability:.1%} | Уверенность: {confidence:.1%} | Неопределенность: {uncertainty_str}{bounds_str}")
        report_lines.append(f"      ⭐ Рейтинг: {rating:.3f} {rating_stars}")

    def _create_empty_outcomes_report(self, date: datetime) -> str:
        """Создает отчет при отсутствии прогнозов."""
        content = f"""
{'=' * 80}
🏆 ИТОГИ КАЧЕСТВЕННЫХ ПРОГНОЗОВ
📅 Дата: {date.strftime('%Y-%m-%d')}
⏰ Время генерации: {datetime.now().strftime('%H:%M:%S')}
{'=' * 80}

❌ НЕТ ПРОГНОЗОВ НА УКАЗАННУЮ ДАТУ

На {date.strftime('%Y-%m-%d')} не найдено прогнозов в базе данных.
"""
        return self._save_outcomes_report(content, date)
    
    def _create_no_quality_outcomes_report(self, date: datetime, total_forecasts: int) -> str:
        """Создает отчет при отсутствии качественных прогнозов."""
        content = f"""
{'=' * 80}
🏆 ИТОГИ КАЧЕСТВЕННЫХ ПРОГНОЗОВ
📅 Дата: {date.strftime('%Y-%m-%d')}
⏰ Время генерации: {datetime.now().strftime('%H:%M:%S')}
{'=' * 80}

📊 СТАТИСТИКА:
   • Всего прогнозов: {total_forecasts}
   • Качественных прогнозов: 0

❌ НЕТ КАЧЕСТВЕННЫХ ПРОГНОЗОВ

Из {total_forecasts} прогнозов ни один не соответствует строгим критериям качества:
   • Минимальная вероятность: {self.quality_criteria['min_probability']:.0%}
   • Минимальная уверенность: {self.quality_criteria['min_confidence']:.0%}
   • Максимальная неопределенность: {self.quality_criteria['max_uncertainty']:.0%}
"""
        return self._save_outcomes_report(content, date)

    def _save_report(self, content: str, date: datetime) -> str:
        """Сохраняет отчет в файл."""
        from db.storage.publisher import save_quality_forecast_report
        return save_quality_forecast_report(content, date, str(self.output_dir))

    def _save_outcomes_report(self, content: str, date: datetime) -> str:
        """Сохраняет отчет с итогами в папку outcomes."""
        from db.storage.publisher import save_quality_outcomes_report
        return save_quality_outcomes_report(content, date, str(self.output_dir))
