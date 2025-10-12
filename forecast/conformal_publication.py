# izhbet/forecast/conformal_publication.py
"""
Модуль для генерации конформных прогнозов из таблицы outcomes.
Использует конформные интервалы неопределенности для более точных прогнозов.
"""

import warnings
import logging
import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from pathlib import Path

from .forecast import ForecastFormatter
from core.constants import today, yesterday
from config import Session_pool
from sqlalchemy import text

warnings.filterwarnings(
    action='ignore',
    category=FutureWarning
)

logger = logging.getLogger(__name__)


class ConformalForecastGenerator:
    """
    Генератор конформных прогнозов с интервалами неопределенности.
    
    Использует данные из таблицы outcomes, которые содержат:
    - Конформные интервалы неопределенности
    - Вероятности с учетом неопределенности
    - Уверенность в прогнозах
    """
    
    def __init__(self):
        self.formatter = ForecastFormatter()
        
        # Маппинг типов прогнозов
        self.feature_mapping = {
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


    def load_conformal_forecasts(self, date_filter: Optional[datetime] = None) -> pd.DataFrame:
        """
        Загружает конформные прогнозы из таблицы outcomes.
        
        Args:
            date_filter: Дата для фильтрации (если None, загружает все)
            
        Returns:
            DataFrame с конформными прогнозами
        """
        logger.info('Загрузка конформных прогнозов из таблицы outcomes')
        
        from db.queries.forecast import get_conformal_forecasts_for_today, get_forecasts_for_date
        
        if date_filter:
            df = get_forecasts_for_date(date_filter.date())
        else:
            df = get_conformal_forecasts_for_today()
        
        if not df.empty:
            # Добавляем название типа прогноза
            df['forecast_type'] = df['feature'].map(self.feature_mapping)
            
            # Преобразуем дату
            df['gameData'] = pd.to_datetime(df['gameData'])
            
            logger.info(f'Загружено {len(df)} конформных прогнозов')
        else:
            logger.warning('Конформные прогнозы не найдены')
        
        return df

    def load_today_forecasts(self) -> pd.DataFrame:
        """Загружает прогнозы на сегодня"""
        return self.load_conformal_forecasts(today)

    def load_yesterday_outcomes(self) -> pd.DataFrame:
        """Загружает исходы вчерашних матчей с завершенными результатами"""
        logger.info('Загрузка исходов вчерашних матчей с завершенными результатами')
        
        from db.queries.forecast import get_yesterday_outcomes
        
        df = get_yesterday_outcomes()
        
        if not df.empty:
            # Добавляем название типа прогноза
            df['forecast_type'] = df['feature'].map(self.feature_mapping)
            
            # Преобразуем дату
            df['gameData'] = pd.to_datetime(df['gameData'])
            
            logger.info(f'Загружено {len(df)} исходов вчерашних матчей с завершенными результатами')
        else:
            logger.warning('Исходы вчерашних матчей с завершенными результатами не найдены')
        
        return df


    def group_forecasts_by_match(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Группирует прогнозы по матчам для удобного отображения.
        
        Args:
            df: DataFrame с конформными прогнозами
            
        Returns:
            DataFrame с сгруппированными прогнозами
        """
        if df.empty:
            return df
        
        # Группируем по match_id и создаем сводку прогнозов
        grouped = []
        
        for match_id, group in df.groupby('match_id'):
            match_info = {
                'match_id': match_id,
                'gameData': group['gameData'].iloc[0],
                'tournament_id': group['tournament_id'].iloc[0],
                'teamHome_id': group['teamHome_id'].iloc[0],
                'teamAway_id': group['teamAway_id'].iloc[0],
                'teamHome_name': group['teamHome_name'].iloc[0],
                'teamAway_name': group['teamAway_name'].iloc[0],
                'championshipName': group['championshipName'].iloc[0],
                'sportName': group['sportName'].iloc[0],
                'numOfHeadsHome': group['numOfHeadsHome'].iloc[0],
                'numOfHeadsAway': group['numOfHeadsAway'].iloc[0],
                'typeOutcome': group['typeOutcome'].iloc[0],
                'gameComment': group['gameComment'].iloc[0],
                'forecasts': {}
            }
            
            # Добавляем прогнозы по типам
            for _, row in group.iterrows():
                forecast_type = row['forecast_type']
                match_info['forecasts'][forecast_type] = {
                    'forecast': row['forecast'],
                    'outcome': row['outcome'],
                    'probability': row['probability'],
                    'confidence': row['confidence'],
                    'uncertainty': row.get('uncertainty'),
                    'lower_bound': row.get('lower_bound'),
                    'upper_bound': row.get('upper_bound')
                }
            
            grouped.append(match_info)
        
        return pd.DataFrame(grouped)
    
    def generate_quality_outcomes_report(self, date: datetime) -> str:
        """
        Генерирует отчет с итогами качественных прогнозов на указанную дату.
        
        Args:
            date: Дата для анализа качественных прогнозов
            
        Returns:
            Путь к созданному файлу отчета
        """
        logger.info(f'Генерация отчета с итогами качественных прогнозов на {date.strftime("%Y-%m-%d")}')
        
        try:
            # Загружаем исходы матчей на указанную дату
            outcomes_df = self.load_yesterday_outcomes()
            
            if outcomes_df.empty:
                logger.warning(f'Нет исходов матчей на {date.strftime("%Y-%m-%d")}')
                return self._create_empty_quality_outcomes_report(date)
            
            # Фильтруем по критериям качества
            quality_outcomes = self.filter_forecasts_by_criteria(
                outcomes_df, 
                min_probability=0.5,
                min_confidence=0.8,
                max_uncertainty=0.3
            )
            
            if quality_outcomes.empty:
                logger.warning(f'Нет качественных исходов на {date.strftime("%Y-%m-%d")}')
                return self._create_no_quality_outcomes_report(date, len(outcomes_df))
            
            # Группируем по матчам
            grouped_outcomes = self.group_forecasts_by_match(quality_outcomes)
            
            # Генерируем отчет
            report_content = self._generate_quality_outcomes_content(grouped_outcomes, date)
            
            # Сохраняем отчет
            file_path = self._save_quality_outcomes_report(report_content, date)
            
            logger.info(f'Отчет с итогами качественных прогнозов сохранен: {file_path}')
            return file_path
            
        except Exception as e:
            logger.error(f'Ошибка при генерации отчета с итогами качественных прогнозов: {e}')
            raise

    def generate_conformal_forecast_report(self, df: pd.DataFrame) -> str:
        """
        Генерирует отчет с конформными прогнозами.
        
        Args:
            df: DataFrame с конформными прогнозами
            
        Returns:
            Строка с отчетом
        """
        if df.empty:
            return "Нет конформных прогнозов на сегодня"
        
        report_lines = [
            f'\n{"="*60}\n',
            f'*** КОНФОРМНЫЕ ПРОГНОЗЫ НА СЕГОДНЯ: {today.strftime("%Y-%m-%d")} ***',
            f'Всего матчей: {len(df)}',
            f'{"="*60}\n'
        ]
        
        for _, match in df.iterrows():
            # Основная информация о матче
            match_info = (
                f"\n🏆 {match['sportName']} - {match['championshipName']}\n"
                f"⚽ {match['teamHome_name']} vs {match['teamAway_name']}\n"
                f"🆔 ID матча: {match['match_id']}\n"
                f"🕐 {match['gameData'].strftime('%H:%M')}\n"
            )
            
            # Добавляем комментарий матча, если он есть
            game_comment = match.get('gameComment', '')
            if game_comment and str(game_comment).strip():
                comment = str(game_comment).strip()
                if len(comment) > 100:
                    comment = comment[:97] + "..."
                match_info += f"💬 {comment}\n"
            
            # Прогнозы
            forecasts_info = []
            for forecast_type, forecast_data in match['forecasts'].items():
                if forecast_type == 'win_draw_loss':
                    outcome_text = {
                        'п1': 'П1 (Победа хозяев)',
                        'х': 'Х (Ничья)',
                        'п2': 'П2 (Победа гостей)'
                    }.get(forecast_data['outcome'], forecast_data['outcome'])
                elif forecast_type == 'oz':
                    outcome_text = 'ОЗД' if 'да' in forecast_data['outcome'].lower() else 'ОЗН'
                elif forecast_type in ['goal_home', 'goal_away']:
                    outcome_text = 'ГОЛ' if 'да' in forecast_data['outcome'].lower() else 'НЕТ'
                elif forecast_type in ['total', 'total_home', 'total_away']:
                    outcome_text = 'БОЛЬШЕ' if 'больше' in forecast_data['outcome'].lower() else 'МЕНЬШЕ'
                else:
                    outcome_text = forecast_data['outcome']
                
                confidence_text = f"🎯 {forecast_data['confidence']:.1%}"
                probability_text = f"📊 {forecast_data['probability']:.1%}"
                
                # Добавляем неопределенность и границы интервала, если доступны
                uncertainty_text = ""
                if 'uncertainty' in forecast_data and forecast_data['uncertainty'] is not None:
                    uncertainty_text = f" | 📈 {forecast_data['uncertainty']:.1%}"
                
                # Добавляем границы интервала
                bounds_text = ""
                if ('lower_bound' in forecast_data and 'upper_bound' in forecast_data and 
                    forecast_data['lower_bound'] is not None and forecast_data['upper_bound'] is not None):
                    bounds_text = f" | 📏 [{forecast_data['lower_bound']:.1%}-{forecast_data['upper_bound']:.1%}]"
                
                # Добавляем значение нейросети/регрессии для сумм тоталов
                nn_value_text = ""
                if forecast_type in ['total_amount', 'total_home_amount', 'total_away_amount']:
                    try:
                        nn_value = forecast_data.get('forecast', None)
                        if nn_value is not None:
                            nn_value_text = f" | NN={float(nn_value):.2f}"
                    except Exception:
                        nn_value_text = ""
                
                forecasts_info.append(
                    f"  • {forecast_type.upper()}: {outcome_text} | {confidence_text} | {probability_text}{uncertainty_text}{bounds_text}{nn_value_text}"
                )
            
            if forecasts_info:
                match_info += '\n'.join(forecasts_info)
            
            report_lines.append(match_info)
        
        return '\n'.join(report_lines)

    def generate_yesterday_outcomes_report(self, df: pd.DataFrame) -> str:
        """
        Генерирует отчет с итогами вчерашних матчей.
        
        Args:
            df: DataFrame с результатами вчерашних матчей
            
        Returns:
            Строка с отчетом по итогам
        """
        if df.empty:
            return "❌ Нет результатов вчерашних матчей"
        
        report_lines = [
            f'\n{"="*60}\n',
            f'📈 ИТОГИ ВЧЕРАШНИХ МАТЧЕЙ: {(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")}',
            f'📊 Всего матчей: {len(df)}',
            f'{"="*60}\n'
        ]
        
        for _, match in df.iterrows():
            # Основная информация о матче
            match_info = [
                f"\n🏆 {match['sportName']} - {match['championshipName']}",
                f"⚽ {match['teamHome_name']} vs {match['teamAway_name']}",
                f"🆔 ID матча: {match['match_id']}",
                f"📊 Счет: {match['numOfHeadsHome']}:{match['numOfHeadsAway']}",
                f"🎯 Результат: {self._get_match_result(match['numOfHeadsHome'], match['numOfHeadsAway'])}"
            ]
            
            # Добавляем комментарий матча, если он есть
            game_comment = match.get('gameComment', '')
            if game_comment and str(game_comment).strip():
                comment = str(game_comment).strip()
                if len(comment) > 100:
                    comment = comment[:97] + "..."
                match_info.append(f"💬 {comment}")
            
            # Анализируем точность прогнозов
            forecasts_info = []
            total_forecasts = 0
            correct_forecasts = 0
            
            for forecast_type, forecast_data in match['forecasts'].items():
                total_forecasts += 1
                
                # Добавляем forecast_type в forecast_data для правильной проверки
                forecast_data_with_type = forecast_data.copy()
                forecast_data_with_type['forecast_type'] = forecast_type
                
            # Определяем, был ли прогноз правильным
            is_correct = self.formatter.is_forecast_correct(forecast_data_with_type, match)
            if is_correct:
                correct_forecasts += 1
            
            outcome = self.formatter.format_outcome(forecast_data['outcome'], forecast_type)
            confidence = f"{forecast_data['confidence']:.1%}"
            probability = f"{forecast_data['probability']:.1%}"
            
            status_emoji = "✅" if is_correct else "❌"
            
            forecasts_info.append(
                f"  {status_emoji} {forecast_type.upper()}: {outcome} | "
                f"Уверенность: {confidence} | Вероятность: {probability}"
            )
            
            if forecasts_info:
                match_info.extend(forecasts_info)
                
                # Добавляем статистику по матчу
                accuracy = (correct_forecasts / total_forecasts) * 100 if total_forecasts > 0 else 0
                match_info.append(f"  📊 Точность по матчу: {correct_forecasts}/{total_forecasts} ({accuracy:.1f}%)")
            
            report_lines.extend(match_info)
            report_lines.append('-' * 60)
        
        return '\n'.join(report_lines)


    def select_best_forecasts(self, forecasts_df: pd.DataFrame, max_forecasts: int = 5) -> pd.DataFrame:
        """
        Выбирает наиболее вероятные прогнозы из общей кучи.
        
        Args:
            forecasts_df: DataFrame с прогнозами
            max_forecasts: Максимальное количество прогнозов для выбора
            
        Returns:
            DataFrame с отобранными прогнозами
        """
        if forecasts_df.empty:
            return forecasts_df
        
        logger.info(f"Выбор {max_forecasts} лучших прогнозов из {len(forecasts_df)} доступных")
        
        # Вычисляем рейтинг для каждого прогноза
        forecasts_df = self._calculate_forecast_rating(forecasts_df)
        
        # Сортируем по рейтингу (убывание)
        forecasts_df = forecasts_df.sort_values('rating', ascending=False)
        
        # Берем топ N прогнозов
        best_forecasts = forecasts_df.head(max_forecasts)
        
        logger.info(f"Выбрано {len(best_forecasts)} лучших прогнозов")
        
        return best_forecasts

    def _calculate_forecast_rating(self, forecasts_df: pd.DataFrame) -> pd.DataFrame:
        """
        Вычисляет рейтинг для каждого прогноза на основе множественных факторов.
        
        Рейтинг учитывает:
        - Вероятность прогноза (probability)
        - Уверенность в прогнозе (confidence)
        - Неопределенность интервала (uncertainty)
        - Историческую точность (если доступна)
        """
        # Создаем копию для безопасной работы
        df = forecasts_df.copy()
        
        # Нормализуем значения в диапазон 0-1
        df['prob_norm'] = df['probability'].clip(0, 1)
        df['conf_norm'] = df['confidence'].clip(0, 1)
        
        # Вычисляем неопределенность (если доступна)
        if 'uncertainty' in df.columns:
            # Нормализуем неопределенность (меньше = лучше)
            uncertainty_max = df['uncertainty'].max()
            if uncertainty_max > 0:
                df['uncertainty_norm'] = 1 - (df['uncertainty'] / uncertainty_max)
            else:
                df['uncertainty_norm'] = 1.0
        else:
            df['uncertainty_norm'] = 1.0
        
        # Весовые коэффициенты для разных факторов
        weights = {
            'probability': 0.4,    # 40% - вероятность прогноза
            'confidence': 0.3,     # 30% - уверенность в прогнозе
            'uncertainty': 0.2,    # 20% - низкая неопределенность
            'diversity': 0.1       # 10% - разнообразие типов прогнозов
        }
        
        # Базовый рейтинг на основе вероятности и уверенности
        df['base_rating'] = (
            weights['probability'] * df['prob_norm'] +
            weights['confidence'] * df['conf_norm'] +
            weights['uncertainty'] * df['uncertainty_norm']
        )
        
        # Добавляем бонус за разнообразие типов прогнозов
        df = self._add_diversity_bonus(df)
        
        # Финальный рейтинг
        df['rating'] = df['base_rating'] + df.get('diversity_bonus', 0)
        
        # Ограничиваем рейтинг диапазоном 0-1
        df['rating'] = df['rating'].clip(0, 1)
        
        return df

    def _add_diversity_bonus(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Добавляет бонус за разнообразие типов прогнозов.
        Поощряет выбор разных типов прогнозов (исходы, тоталы, голы и т.д.)
        """
        if 'forecast_type' not in df.columns:
            df['diversity_bonus'] = 0
            return df
        
        # Определяем категории прогнозов
        def get_forecast_category(forecast_type: str) -> str:
            if 'win_draw_loss' in forecast_type:
                return 'outcome'
            elif 'oz' in forecast_type:
                return 'both_score'
            elif 'goal' in forecast_type:
                return 'goals'
            elif 'total' in forecast_type:
                return 'totals'
            else:
                return 'other'
        
        df['category'] = df['forecast_type'].apply(get_forecast_category)
        
        # Подсчитываем количество прогнозов в каждой категории
        category_counts = df['category'].value_counts()
        
        # Вычисляем бонус за разнообразие
        # Чем меньше прогнозов в категории, тем больше бонус
        max_count = category_counts.max()
        df['diversity_bonus'] = df['category'].apply(
            lambda cat: 0.1 * (1 - category_counts[cat] / max_count) if max_count > 0 else 0
        )
        
        return df

    def filter_forecasts_by_criteria(self, forecasts_df: pd.DataFrame, 
                                   min_probability: float = 0.6,
                                   min_confidence: float = 0.7,
                                   max_uncertainty: float = 0.3) -> pd.DataFrame:
        """
        Фильтрует прогнозы по заданным критериям качества.
        
        Args:
            forecasts_df: DataFrame с прогнозами
            min_probability: Минимальная вероятность
            min_confidence: Минимальная уверенность
            max_uncertainty: Максимальная неопределенность
            
        Returns:
            Отфильтрованный DataFrame
        """
        if forecasts_df.empty:
            return forecasts_df
        
        original_count = len(forecasts_df)
        
        # Применяем фильтры
        filtered_df = forecasts_df[
            (forecasts_df['probability'] >= min_probability) &
            (forecasts_df['confidence'] >= min_confidence)
        ]
        
        # Фильтр по неопределенности (если доступна)
        if 'uncertainty' in filtered_df.columns:
            # Исключаем NULL значения из фильтрации
            uncertainty_mask = filtered_df['uncertainty'].isna() | (filtered_df['uncertainty'] <= max_uncertainty)
            filtered_df = filtered_df[uncertainty_mask]
        
        filtered_count = len(filtered_df)
        
        logger.info(f"Фильтрация: {original_count} -> {filtered_count} прогнозов "
                   f"(min_prob={min_probability}, min_conf={min_confidence})")
        
        return filtered_df

    def generate_forecast_ranking_report(self, forecasts_df: pd.DataFrame) -> str:
        """
        Генерирует отчет с рейтингами прогнозов.
        
        Args:
            forecasts_df: DataFrame с прогнозами и рейтингами
            
        Returns:
            Строка с отчетом
        """
        if forecasts_df.empty:
            return "❌ Нет прогнозов для ранжирования"
        
        report_lines = []
        report_lines.append("🏆 ТОП ПРОГНОЗОВ ПО РЕЙТИНГУ")
        report_lines.append("=" * 50)
        
        # Сортируем по рейтингу
        sorted_forecasts = forecasts_df.sort_values('rating', ascending=False)
        
        for idx, (_, forecast) in enumerate(sorted_forecasts.iterrows(), 1):
            # Получаем информацию о матче
            match_info = f"{forecast.get('teamHome_name', 'Unknown')} vs {forecast.get('teamAway_name', 'Unknown')}"
            championship = forecast.get('championshipName', 'Unknown')
            
            # Получаем информацию о прогнозе
            forecast_type = forecast.get('forecast_type', 'unknown')
            outcome = forecast.get('outcome', 'unknown')
            probability = forecast.get('probability', 0.0)
            confidence = forecast.get('confidence', 0.0)
            rating = forecast.get('rating', 0.0)
            uncertainty = forecast.get('uncertainty', 0.0)
            
            # Форматируем рейтинг
            rating_stars = "⭐" * int(rating * 5) + "☆" * (5 - int(rating * 5))
            
            report_lines.append(f"\n{idx}. {match_info}")
            report_lines.append(f"   🏆 {championship}")
            report_lines.append(f"   📊 Прогноз: {outcome} | Вероятность: {probability:.1%} | Уверенность: {confidence:.1%}")
            report_lines.append(f"   ⭐ Рейтинг: {rating:.3f} {rating_stars}")
            if uncertainty > 0:
                report_lines.append(f"   📈 Неопределенность: {uncertainty:.3f}")
        
        report_lines.append(f"\n📊 Всего прогнозов: {len(forecasts_df)}")
        report_lines.append(f"🎯 Средний рейтинг: {forecasts_df['rating'].mean():.3f}")
        
        return "\n".join(report_lines)

    def _get_match_result(self, home_goals: int, away_goals: int) -> str:
        """
        Определяет результат матча на основе счета.
        
        Args:
            home_goals: Голы хозяев
            away_goals: Голы гостей
            
        Returns:
            Строка с результатом: 'П1', 'Н', 'П2'
        """
        if home_goals is None or away_goals is None:
            return 'Неизвестно'
        
        if home_goals > away_goals:
            return 'П1'  # Победа хозяев
        elif home_goals < away_goals:
            return 'П2'  # Победа гостей
        else:
            return 'Н'   # Ничья
    
    def _generate_quality_outcomes_content(self, grouped_outcomes: pd.DataFrame, date: datetime) -> str:
        """Генерирует содержимое отчета по качественным итогам."""
        report_lines = []
        
        # Заголовок отчета
        report_lines.append("=" * 80)
        report_lines.append("🏆 ИТОГИ КАЧЕСТВЕННЫХ ПРОГНОЗОВ")
        report_lines.append(f"📅 Дата: {date.strftime('%Y-%m-%d')}")
        report_lines.append(f"⏰ Время генерации: {datetime.now().strftime('%H:%M:%S')}")
        report_lines.append("=" * 80)
        
        # Статистика
        total_matches = len(grouped_outcomes)
        total_forecasts = sum(len(match['forecasts']) for _, match in grouped_outcomes.iterrows())
        
        report_lines.append(f"\n📊 СТАТИСТИКА КАЧЕСТВЕННЫХ ИТОГОВ:")
        report_lines.append(f"   • Всего матчей: {total_matches}")
        report_lines.append(f"   • Всего прогнозов: {total_forecasts}")
        
        # Критерии качества
        report_lines.append(f"\n🎯 КРИТЕРИИ КАЧЕСТВА:")
        report_lines.append(f"   • Минимальная вероятность: 50%")
        report_lines.append(f"   • Минимальная уверенность: 80%")
        report_lines.append(f"   • Максимальная неопределенность: 30%")
        
        if total_matches == 0:
            report_lines.append("\n❌ Нет качественных прогнозов для анализа")
            return "\n".join(report_lines)
        
        # Анализируем каждый матч
        report_lines.append(f"\n🏆 АНАЛИЗ КАЧЕСТВЕННЫХ ПРОГНОЗОВ ({total_matches} матчей):")
        report_lines.append("-" * 80)
        
        total_correct = 0
        total_analyzed = 0
        
        for _, match in grouped_outcomes.iterrows():
            match_analysis = self._analyze_quality_match(match)
            report_lines.extend(match_analysis['lines'])
            total_correct += match_analysis['correct']
            total_analyzed += match_analysis['total']
        
        # Итоговая статистика
        accuracy = (total_correct / total_analyzed * 100) if total_analyzed > 0 else 0
        report_lines.append(f"\n📈 ИТОГОВАЯ СТАТИСТИКА:")
        report_lines.append(f"   • Обработано матчей: {total_matches}")
        report_lines.append(f"   • Проанализировано прогнозов: {total_analyzed}")
        report_lines.append(f"   • Правильных прогнозов: {total_correct}")
        report_lines.append(f"   • Точность: {accuracy:.1f}%")
        
        return "\n".join(report_lines)
    
    def _analyze_quality_match(self, match: dict) -> dict:
        """Анализирует качественные прогнозы для одного матча."""
        lines = []
        correct = 0
        total = 0
        
        # Информация о матче
        match_info = f"{match['teamHome_name']} vs {match['teamAway_name']}"
        championship = match['championshipName']
        sport = match['sportName']
        score = f"{match['numOfHeadsHome']}:{match['numOfHeadsAway']}"
        result = self._get_match_result(match['numOfHeadsHome'], match['numOfHeadsAway'])
        game_comment = match.get('gameComment', '')
        
        lines.append(f"\n⚽ {match_info}")
        lines.append(f"   🏆 {sport} - {championship}")
        lines.append(f"   📊 Счет: {score}")
        lines.append(f"   🎯 Результат: {result}")
        
        # Добавляем комментарий матча, если он есть
        if game_comment and str(game_comment).strip():
            comment = str(game_comment).strip()
            if len(comment) > 100:
                comment = comment[:97] + "..."
            lines.append(f"   💬 {comment}")
        
        # Анализируем прогнозы
        lines.append(f"   📋 КАЧЕСТВЕННЫЕ ПРОГНОЗЫ:")
        
        for forecast_type, forecast_data in match['forecasts'].items():
            total += 1
            
            # Определяем, был ли прогноз правильным
            is_correct = self.formatter.is_forecast_correct({
                'forecast_type': forecast_type,
                'outcome': forecast_data['outcome']
            }, match)
            
            if is_correct:
                correct += 1
                status = "✅"
            else:
                status = "❌"
            
            # Форматируем тип прогноза
            type_display = self.formatter.format_forecast_type(forecast_type)
            outcome = self.formatter.format_outcome(forecast_data['outcome'], forecast_type)
            confidence = f"{forecast_data['confidence']:.1%}"
            probability = f"{forecast_data['probability']:.1%}"
            
            # Показываем значение нейросети/регрессии для сумм тоталов
            nn_value_text = ""
            if forecast_type in ['total_amount', 'total_home_amount', 'total_away_amount']:
                try:
                    nn_value = match['forecasts'][forecast_type].get('forecast', None)
                    if nn_value is not None:
                        nn_value_text = f" | NN={float(nn_value):.2f}"
                except Exception:
                    nn_value_text = ""
            
            lines.append(f"      {status} {type_display}: {outcome} | Уверенность: {confidence} | Вероятность: {probability}{nn_value_text}")
        
        # Точность по матчу
        match_accuracy = (correct / total * 100) if total > 0 else 0
        lines.append(f"   📊 Точность по матчу: {correct}/{total} ({match_accuracy:.1f}%)")
        lines.append("-" * 80)
        
        return {
            'lines': lines,
            'correct': correct,
            'total': total
        }
    
    
    def _save_quality_outcomes_report(self, content: str, date: datetime) -> str:
        """Сохраняет отчет по качественным итогам в файл."""
        # Создаем поддиректорию по году и месяцу в папке outcome
        year_month_dir = Path('results') / 'outcome' / date.strftime('%Y') / date.strftime('%m')
        year_month_dir.mkdir(parents=True, exist_ok=True)
        
        # Имя файла
        filename = f"{date.strftime('%Y-%m-%d')}_quality_outcome.txt"
        file_path = year_month_dir / filename
        
        # Сохраняем файл
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return str(file_path)
    
    def _create_empty_quality_outcomes_report(self, date: datetime) -> str:
        """Создает отчет при отсутствии исходов."""
        content = f"""
{'=' * 80}
🏆 ИТОГИ КАЧЕСТВЕННЫХ ПРОГНОЗОВ
📅 Дата: {date.strftime('%Y-%m-%d')}
⏰ Время генерации: {datetime.now().strftime('%H:%M:%S')}
{'=' * 80}

❌ НЕТ ИСХОДОВ МАТЧЕЙ НА УКАЗАННУЮ ДАТУ

На {date.strftime('%Y-%m-%d')} не найдено исходов матчей в базе данных.
"""
        return self._save_quality_outcomes_report(content, date)
    
    def _create_no_quality_outcomes_report(self, date: datetime, total_outcomes: int) -> str:
        """Создает отчет при отсутствии качественных исходов."""
        content = f"""
{'=' * 80}
🏆 ИТОГИ КАЧЕСТВЕННЫХ ПРОГНОЗОВ
📅 Дата: {date.strftime('%Y-%m-%d')}
⏰ Время генерации: {datetime.now().strftime('%H:%M:%S')}
{'=' * 80}

📊 СТАТИСТИКА:
   • Всего исходов: {total_outcomes}
   • Качественных исходов: 0

❌ НЕТ КАЧЕСТВЕННЫХ ИСХОДОВ

Из {total_outcomes} исходов ни один не соответствует строгим критериям качества:
   • Минимальная вероятность: 50%
   • Минимальная уверенность: 80%
   • Максимальная неопределенность: 30%
"""
        return self._save_quality_outcomes_report(content, date)
    
    def _generate_quality_forecast_for_today(self, df_today: pd.DataFrame) -> str:
        """
        Генерирует качественные прогнозы на сегодня на основе загруженных данных.
        
        Args:
            df_today: DataFrame с прогнозами на сегодня
            
        Returns:
            Путь к созданному файлу с качественными прогнозами
        """
        from .quality_forecast_report import QualityForecastReporter
        
        # Создаем генератор качественных прогнозов
        quality_reporter = QualityForecastReporter(output_dir='results')
        
        # Генерируем отчет на сегодня
        report_path = quality_reporter.generate_quality_forecast_report(datetime.now())
        
        return report_path
    

    def generate_forecasts(self) -> Dict[str, str]:
        """
        Генерирует конформные прогнозы.
        
        Returns:
            Словарь с отчетами: {'today': str, 'yesterday': str}
        """
        logger.info('Генерация конформных прогнозов')
        
        try:
            # Загружаем прогнозы на сегодня
            df_today = self.load_today_forecasts()
            today_report = ""
            
            if not df_today.empty:
                # Группируем прогнозы по матчам
                df_grouped = self.group_forecasts_by_match(df_today)
                # Генерируем отчет
                today_report = self.generate_conformal_forecast_report(df_grouped)
            else:
                logger.warning('Нет конформных прогнозов на сегодня')
                today_report = "❌ Нет прогнозов на сегодня"
            
            # Загружаем итоги вчерашних матчей
            df_yesterday = self.load_yesterday_outcomes()
            yesterday_report = ""
            
            if not df_yesterday.empty:
                # Группируем результаты по матчам
                df_yesterday_grouped = self.group_forecasts_by_match(df_yesterday)
                # Генерируем отчет по итогам
                yesterday_report = self.generate_yesterday_outcomes_report(df_yesterday_grouped)
            else:
                logger.warning('Нет результатов вчерашних матчей')
                yesterday_report = "❌ Нет результатов вчерашних матчей"
            
            logger.info('Генерация конформных прогнозов завершена')
            
            return {
                'today': today_report,
                'yesterday': yesterday_report
            }
            
        except Exception as e:
            logger.error(f'Ошибка при генерации конформных прогнозов: {e}')
            raise


    def process_season_conformal_forecasts(self, year: str = None):
        """
        Обрабатывает конформные прогнозы за весь период чемпионата:
        - Получает список всех турниров за указанный год
        - Для каждого турнира получает список матчей за сезон
        - Обрабатывает прогнозы день за днем
        
        Args:
            year: Год турнира (например, "2025"). Если None, используется текущий сезон.
        """
        logger.info(f'Обработка конформных прогнозов за период чемпионата (год: {year or "текущий сезон"})')
        
        try:
            # 1. Получаем список всех турниров
            tournaments = self.get_all_tournaments(year)
            logger.info(f'Найдено {len(tournaments)} турниров для обработки')
            
            # 2. Обрабатываем каждый турнир
            for tournament_id in tournaments:
                logger.info(f'Обработка турнира {tournament_id}')
                self.process_tournament_season_forecasts(tournament_id)
            
            logger.info('Обработка конформных прогнозов за весь период завершена')
            
        except Exception as e:
            logger.error(f'Ошибка при обработке конформных прогнозов за весь период: {e}')
            raise

    def get_all_tournaments(self, year: str = None) -> List[int]:
        """
        Получает список турниров по году.
        
        Args:
            year: Год турнира (например, "2025"). Если None, используется текущий сезон.
        
        Returns:
            List[int]: Список ID турниров
        """
        from db.queries.forecast import get_all_tournaments
        
        return get_all_tournaments(year)

    def process_tournament_season_forecasts(self, tournament_id: int):
        """
        Обрабатывает прогнозы за весь сезон для конкретного чемпионата.
        
        Args:
            tournament_id: ID чемпионата
        """
        logger.info(f'Обработка сезона чемпионата {tournament_id}')
        
        try:
            # 1. Получаем список дат матчей, для которых есть прогнозы
            match_dates = self.get_tournament_match_dates(tournament_id)
            logger.info(f'Найдено {len(match_dates)} дат матчей с прогнозами в чемпионате {tournament_id}')
            
            if not match_dates:
                logger.info(f'Нет матчей с прогнозами в чемпионате {tournament_id} - пропускаем')
                return
            
            # 2. Обрабатываем каждую дату
            for match_date in match_dates:
                logger.info(f'Обработка даты {match_date} для чемпионата {tournament_id}')
                self.process_date_forecasts(tournament_id, match_date)
            
            logger.info(f'Обработка сезона чемпионата {tournament_id} завершена')
            
        except Exception as e:
            logger.error(f'Ошибка при обработке сезона чемпионата {tournament_id}: {e}')

    def get_tournament_match_dates(self, tournament_id: int) -> List[str]:
        """
        Получает список дат матчей в чемпионате, для которых есть прогнозы.
        
        Args:
            tournament_id: ID чемпионата (championship_id из matchs.tournament_id)
            
        Returns:
            List[str]: Список дат в формате YYYY-MM-DD
        """
        from db.queries.forecast import get_tournament_match_dates
        
        return get_tournament_match_dates(tournament_id)

    def process_date_forecasts(self, tournament_id: int, match_date: str):
        """
        Обрабатывает прогнозы для конкретной даты и чемпионата.
        
        Args:
            tournament_id: ID чемпионата
            match_date: Дата в формате YYYY-MM-DD
        """
        logger.info(f'Обработка прогнозов на {match_date} для чемпионата {tournament_id}')
        
        try:
            # 1. Загружаем прогнозы на указанную дату
            df_forecasts = self.load_date_forecasts(tournament_id, match_date)
            
            if df_forecasts.empty:
                logger.warning(f'Нет прогнозов на {match_date} для чемпионата {tournament_id}')
                return
            
            logger.info(f'Найдено {len(df_forecasts)} прогнозов на {match_date}')
            
            # 2. Группируем прогнозы по матчам
            df_grouped = self.group_forecasts_by_match(df_forecasts)
            
            # 3. Генерируем отчеты
            report = self.generate_conformal_forecast_report(df_grouped)
            outcomes_report = self.generate_yesterday_outcomes_report(df_grouped)

            # Дополнительно: ОБЯЗАТЕЛЬНО сформируем качественный прогноз для указанной даты
            quality_path = None
            try:
                date_obj = datetime.strptime(match_date, '%Y-%m-%d')
                from .quality_forecast_report import QualityForecastReporter
                quality_reporter = QualityForecastReporter(output_dir='results')
                quality_path = quality_reporter.generate_quality_forecast_report(date_obj)
                logger.info(f"Качественный прогноз за {match_date} сохранен: {quality_path}")
            except Exception as e:
                logger.warning(f"Не удалось сформировать качественный прогноз за {match_date}: {e}")
            
            # 4. Публикуем отчет
            for publisher in self.publishers:
                try:
                    if isinstance(publisher, ConformalDailyPublisher):
                        message = {
                            'date': match_date,
                            'tournament_id': tournament_id,
                            'forecasts': report,
                            'outcomes': outcomes_report,
                            'quality_path': quality_path
                        }
                        publisher.publish(message)
                    else:
                        # Обычный публикатор
                        publisher.publish(f"Чемпионат {tournament_id}, {match_date}:\n{report}")
                    
                    logger.info(f'Отчет за {match_date} опубликован через {type(publisher).__name__}')
                except Exception as e:
                    logger.error(f'Ошибка публикации отчета за {match_date}: {e}')
            
        except Exception as e:
            logger.error(f'Ошибка при обработке прогнозов на {match_date}: {e}')

    def load_date_forecasts(self, tournament_id: int, match_date: str) -> pd.DataFrame:
        """
        Загружает прогнозы на конкретную дату для конкретного чемпионата.
        
        Args:
            tournament_id: ID чемпионата (championship_id)
            match_date: Дата в формате YYYY-MM-DD
            
        Returns:
            pd.DataFrame: Данные прогнозов
        """
        with Session_pool() as db_session:
            # tournament_id уже является championship_id из matchs.tournament_id
            championship_id = tournament_id
            
            # Получаем год турнира для дополнительной фильтрации
            tournament_year_result = db_session.execute(text("""
                SELECT yearTournament FROM tournaments WHERE championship_id = :championship_id
                ORDER BY yearTournament DESC
                LIMIT 1
            """), {'championship_id': championship_id})
            
            tournament_year_row = tournament_year_result.fetchone()
            if not tournament_year_row:
                logger.warning(f'Не найден yearTournament для championship_id {championship_id}')
                return pd.DataFrame()
            
            tournament_year = tournament_year_row[0]
            
            query = text("""
                SELECT 
                    o.id,
                    o.match_id,
                    o.feature,
                    o.forecast,
                    o.outcome,
                    o.probability,
                    o.confidence,
                    o.lower_bound,
                    o.upper_bound,
                    o.uncertainty,
                    o.created_at,
                    m.gameData,
                    m.tournament_id,
                    m.teamHome_id,
                    m.teamAway_id,
                    m.numOfHeadsHome,
                    m.numOfHeadsAway,
                    m.typeOutcome,
                    m.gameComment,
                    th.teamName as teamHome_name,
                    ta.teamName as teamAway_name,
                    ch.championshipName,
                    s.sportName
                FROM outcomes o
                JOIN matchs m ON o.match_id = m.id
                INNER JOIN predictions p ON m.id = p.match_id
                LEFT JOIN teams th ON m.teamHome_id = th.id
                LEFT JOIN teams ta ON m.teamAway_id = ta.id
                LEFT JOIN tournaments t ON m.tournament_id = t.championship_id
                LEFT JOIN championships ch ON t.championship_id = ch.id
                LEFT JOIN sports s ON ch.sport_id = s.id
                WHERE m.tournament_id = :championship_id
                AND YEAR(m.gameData) = :tournament_year
                AND DATE(m.gameData) = :match_date
                ORDER BY m.gameData, o.feature
            """)
            
            # Обрабатываем yearTournament в формате XX/XX или YYYY
            if '/' in str(tournament_year):
                # Формат XX/XX - берем первую часть и добавляем 2000
                year_part = str(tournament_year).split('/')[0]
                tournament_year_int = int(year_part) + 2000
                # Для сезонов типа 24/25 или 25/26 ищем матчи в обоих годах
                next_year = tournament_year_int + 1
                year_filter = f"YEAR(m.gameData) IN ({tournament_year_int}, {next_year})"
            else:
                # Формат YYYY - используем как есть
                tournament_year_int = int(tournament_year)
                year_filter = f"YEAR(m.gameData) = {tournament_year_int}"
            
            # Заменяем фильтр по году в запросе
            query_str = str(query)
            query_str = query_str.replace("AND YEAR(m.gameData) = :tournament_year", f"AND {year_filter}")
            
            result = db_session.execute(text(query_str), {
                'championship_id': championship_id,
                'match_date': match_date
            })
            
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            
            if not df.empty:
                # Добавляем название типа прогноза
                df['forecast_type'] = df['feature'].map(self.feature_mapping)
                
                # Преобразуем дату
                df['gameData'] = pd.to_datetime(df['gameData'])
            
            return df


def main():
    """Основная функция для запуска генерации конформных прогнозов"""
    logger.info('Запуск генерации конформных прогнозов')
    
    # Создаем генератор
    generator = ConformalForecastGenerator()
    
    # Генерируем прогнозы
    reports = generator.generate_forecasts()
    
    logger.info('Генерация конформных прогнозов завершена')
    return reports


if __name__ == "__main__":
    main()
