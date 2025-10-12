"""
API для работы со статистикой прогнозов.
Предоставляет эндпоинты для фронтенда.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, date, timedelta
from sqlalchemy import text, func
from sqlalchemy.orm import Session

from config import Session_pool

logger = logging.getLogger(__name__)


class StatisticsAPI:
    """API для работы со статистикой прогнозов."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def get_statistics_summary(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        championship_id: Optional[int] = None,
        sport_id: Optional[int] = None,
        forecast_type: Optional[str] = None,
        model_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Получает сводную статистику прогнозов.
        
        Args:
            start_date: Начальная дата (по умолчанию - последние 30 дней)
            end_date: Конечная дата (по умолчанию - сегодня)
            championship_id: ID чемпионата (опционально)
            sport_id: ID вида спорта (опционально)
            forecast_type: Тип прогноза (опционально)
            model_name: Название модели (опционально)
        
        Returns:
            Dict с сводной статистикой
        """
        try:
            with Session_pool() as db_session:
                # Устанавливаем даты по умолчанию
                if not end_date:
                    end_date = date.today()
                if not start_date:
                    start_date = end_date - timedelta(days=30)
                
                # Базовый запрос
                base_query = """
                    SELECT 
                        COUNT(*) as total_predictions,
                        SUM(CASE WHEN prediction_correct = 1 THEN 1 ELSE 0 END) as correct_predictions,
                        AVG(prediction_accuracy) as avg_accuracy,
                        COUNT(DISTINCT match_id) as unique_matches,
                        COUNT(DISTINCT championship_id) as unique_championships,
                        COUNT(DISTINCT sport_id) as unique_sports
                    FROM statistics_optimized
                    WHERE match_date BETWEEN :start_date AND :end_date
                """
                
                params = {
                    'start_date': start_date,
                    'end_date': end_date
                }
                
                # Добавляем фильтры
                if championship_id:
                    base_query += " AND championship_id = :championship_id"
                    params['championship_id'] = championship_id
                
                if sport_id:
                    base_query += " AND sport_id = :sport_id"
                    params['sport_id'] = sport_id
                
                if forecast_type:
                    base_query += " AND forecast_type = :forecast_type"
                    params['forecast_type'] = forecast_type
                
                if model_name:
                    base_query += " AND model_name = :model_name"
                    params['model_name'] = model_name
                
                result = db_session.execute(text(base_query), params).fetchone()
                
                if not result or result[0] == 0:
                    return {
                        'total_predictions': 0,
                        'correct_predictions': 0,
                        'accuracy_percentage': 0.0,
                        'avg_accuracy': 0.0,
                        'unique_matches': 0,
                        'unique_championships': 0,
                        'unique_sports': 0
                    }
                
                total_predictions = result[0]
                correct_predictions = result[1] or 0
                avg_accuracy = float(result[2]) if result[2] else 0.0
                unique_matches = result[3]
                unique_championships = result[4]
                unique_sports = result[5]
                
                accuracy_percentage = (correct_predictions / total_predictions * 100) if total_predictions > 0 else 0.0
                
                return {
                    'total_predictions': total_predictions,
                    'correct_predictions': correct_predictions,
                    'accuracy_percentage': round(accuracy_percentage, 2),
                    'avg_accuracy': round(avg_accuracy, 4),
                    'unique_matches': unique_matches,
                    'unique_championships': unique_championships,
                    'unique_sports': unique_sports
                }
                
        except Exception as e:
            self.logger.error(f"Ошибка получения сводной статистики: {e}")
            return {}
    
    def get_statistics_by_type(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        championship_id: Optional[int] = None,
        sport_id: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Получает статистику по типам прогнозов.
        
        Args:
            start_date: Начальная дата
            end_date: Конечная дата
            championship_id: ID чемпионата
            sport_id: ID вида спорта
        
        Returns:
            List с статистикой по типам
        """
        try:
            with Session_pool() as db_session:
                # Устанавливаем даты по умолчанию
                if not end_date:
                    end_date = date.today()
                if not start_date:
                    start_date = end_date - timedelta(days=30)
                
                query = """
                    SELECT 
                        forecast_type,
                        COUNT(*) as total_predictions,
                        SUM(CASE WHEN prediction_correct = 1 THEN 1 ELSE 0 END) as correct_predictions,
                        AVG(prediction_accuracy) as avg_accuracy,
                        AVG(prediction_error) as avg_error,
                        AVG(prediction_residual) as avg_residual
                    FROM statistics_optimized
                    WHERE match_date BETWEEN :start_date AND :end_date
                """
                
                params = {
                    'start_date': start_date,
                    'end_date': end_date
                }
                
                if championship_id:
                    query += " AND championship_id = :championship_id"
                    params['championship_id'] = championship_id
                
                if sport_id:
                    query += " AND sport_id = :sport_id"
                    params['sport_id'] = sport_id
                
                query += " GROUP BY forecast_type ORDER BY total_predictions DESC"
                
                results = db_session.execute(text(query), params).fetchall()
                
                statistics = []
                for row in results:
                    total = row[1]
                    correct = row[2] or 0
                    accuracy_percentage = (correct / total * 100) if total > 0 else 0.0
                    
                    statistics.append({
                        'forecast_type': row[0],
                        'total_predictions': total,
                        'correct_predictions': correct,
                        'accuracy_percentage': round(accuracy_percentage, 2),
                        'avg_accuracy': round(float(row[3]) if row[3] else 0.0, 4),
                        'avg_error': round(float(row[4]) if row[4] else 0.0, 3),
                        'avg_residual': round(float(row[5]) if row[5] else 0.0, 3)
                    })
                
                return statistics
                
        except Exception as e:
            self.logger.error(f"Ошибка получения статистики по типам: {e}")
            return []
    
    def get_statistics_by_model(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        championship_id: Optional[int] = None,
        sport_id: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Получает статистику по моделям.
        
        Args:
            start_date: Начальная дата
            end_date: Конечная дата
            championship_id: ID чемпионата
            sport_id: ID вида спорта
        
        Returns:
            List с статистикой по моделям
        """
        try:
            with Session_pool() as db_session:
                # Устанавливаем даты по умолчанию
                if not end_date:
                    end_date = date.today()
                if not start_date:
                    start_date = end_date - timedelta(days=30)
                
                query = """
                    SELECT 
                        model_name,
                        model_version,
                        model_type,
                        COUNT(*) as total_predictions,
                        SUM(CASE WHEN prediction_correct = 1 THEN 1 ELSE 0 END) as correct_predictions,
                        AVG(prediction_accuracy) as avg_accuracy,
                        AVG(prediction_error) as avg_error
                    FROM statistics_optimized
                    WHERE match_date BETWEEN :start_date AND :end_date
                """
                
                params = {
                    'start_date': start_date,
                    'end_date': end_date
                }
                
                if championship_id:
                    query += " AND championship_id = :championship_id"
                    params['championship_id'] = championship_id
                
                if sport_id:
                    query += " AND sport_id = :sport_id"
                    params['sport_id'] = sport_id
                
                query += " GROUP BY model_name, model_version, model_type ORDER BY total_predictions DESC"
                
                results = db_session.execute(text(query), params).fetchall()
                
                statistics = []
                for row in results:
                    total = row[3]
                    correct = row[4] or 0
                    accuracy_percentage = (correct / total * 100) if total > 0 else 0.0
                    
                    statistics.append({
                        'model_name': row[0],
                        'model_version': row[1],
                        'model_type': row[2],
                        'total_predictions': total,
                        'correct_predictions': correct,
                        'accuracy_percentage': round(accuracy_percentage, 2),
                        'avg_accuracy': round(float(row[5]) if row[5] else 0.0, 4),
                        'avg_error': round(float(row[6]) if row[6] else 0.0, 3)
                    })
                
                return statistics
                
        except Exception as e:
            self.logger.error(f"Ошибка получения статистики по моделям: {e}")
            return []
    
    def get_daily_statistics(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        championship_id: Optional[int] = None,
        sport_id: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Получает ежедневную статистику.
        
        Args:
            start_date: Начальная дата
            end_date: Конечная дата
            championship_id: ID чемпионата
            sport_id: ID вида спорта
        
        Returns:
            List с ежедневной статистикой
        """
        try:
            with Session_pool() as db_session:
                # Устанавливаем даты по умолчанию
                if not end_date:
                    end_date = date.today()
                if not start_date:
                    start_date = end_date - timedelta(days=30)
                
                query = """
                    SELECT 
                        match_date,
                        COUNT(*) as total_predictions,
                        SUM(CASE WHEN prediction_correct = 1 THEN 1 ELSE 0 END) as correct_predictions,
                        AVG(prediction_accuracy) as avg_accuracy,
                        COUNT(DISTINCT match_id) as unique_matches
                    FROM statistics_optimized
                    WHERE match_date BETWEEN :start_date AND :end_date
                """
                
                params = {
                    'start_date': start_date,
                    'end_date': end_date
                }
                
                if championship_id:
                    query += " AND championship_id = :championship_id"
                    params['championship_id'] = championship_id
                
                if sport_id:
                    query += " AND sport_id = :sport_id"
                    params['sport_id'] = sport_id
                
                query += " GROUP BY match_date ORDER BY match_date DESC"
                
                results = db_session.execute(text(query), params).fetchall()
                
                statistics = []
                for row in results:
                    total = row[1]
                    correct = row[2] or 0
                    accuracy_percentage = (correct / total * 100) if total > 0 else 0.0
                    
                    statistics.append({
                        'date': row[0].isoformat() if row[0] else None,
                        'total_predictions': total,
                        'correct_predictions': correct,
                        'accuracy_percentage': round(accuracy_percentage, 2),
                        'avg_accuracy': round(float(row[3]) if row[3] else 0.0, 4),
                        'unique_matches': row[4]
                    })
                
                return statistics
                
        except Exception as e:
            self.logger.error(f"Ошибка получения ежедневной статистики: {e}")
            return []
    
    def get_match_details(
        self,
        match_id: int
    ) -> Dict[str, Any]:
        """
        Получает детали прогнозов для конкретного матча.
        
        Args:
            match_id: ID матча
        
        Returns:
            Dict с деталями матча
        """
        try:
            with Session_pool() as db_session:
                # Получаем основную информацию о матче
                match_query = """
                    SELECT 
                        s.match_id,
                        s.match_date,
                        s.championship_id,
                        s.sport_id,
                        ch.championshipName,
                        sp.sportName,
                        m.numOfHeadsHome,
                        m.numOfHeadsAway,
                        th.teamName as team_home_name,
                        ta.teamName as team_away_name
                    FROM statistics_optimized s
                    LEFT JOIN matchs m ON s.match_id = m.id
                    LEFT JOIN championships ch ON s.championship_id = ch.id
                    LEFT JOIN sports sp ON s.sport_id = sp.id
                    LEFT JOIN teams th ON m.teamHome_id = th.id
                    LEFT JOIN teams ta ON m.teamAway_id = ta.id
                    WHERE s.match_id = :match_id
                    LIMIT 1
                """
                
                match_result = db_session.execute(text(match_query), {'match_id': match_id}).fetchone()
                
                if not match_result:
                    return {}
                
                # Получаем все прогнозы для матча
                predictions_query = """
                    SELECT 
                        s.id,
                        s.forecast_type,
                        s.forecast_subtype,
                        s.model_name,
                        s.prediction_correct,
                        s.prediction_accuracy,
                        s.prediction_error,
                        s.prediction_residual,
                        o.probability,
                        o.confidence,
                        o.uncertainty,
                        o.lower_bound,
                        o.upper_bound
                    FROM statistics_optimized s
                    LEFT JOIN outcomes o ON s.outcome_id = o.id
                    WHERE s.match_id = :match_id
                    ORDER BY s.forecast_type, s.id
                """
                
                predictions_results = db_session.execute(text(predictions_query), {'match_id': match_id}).fetchall()
                
                predictions = []
                for row in predictions_results:
                    predictions.append({
                        'id': row[0],
                        'forecast_type': row[1],
                        'forecast_subtype': row[2],
                        'model_name': row[3],
                        'prediction_correct': row[4],
                        'prediction_accuracy': float(row[5]) if row[5] else None,
                        'prediction_error': float(row[6]) if row[6] else None,
                        'prediction_residual': float(row[7]) if row[7] else None,
                        'probability': float(row[8]) if row[8] else None,
                        'confidence': float(row[9]) if row[9] else None,
                        'uncertainty': float(row[10]) if row[10] else None,
                        'lower_bound': float(row[11]) if row[11] else None,
                        'upper_bound': float(row[12]) if row[12] else None
                    })
                
                return {
                    'match_id': match_result[0],
                    'match_date': match_result[1].isoformat() if match_result[1] else None,
                    'championship_id': match_result[2],
                    'sport_id': match_result[3],
                    'championship_name': match_result[4],
                    'sport_name': match_result[5],
                    'goal_home': match_result[6],
                    'goal_away': match_result[7],
                    'team_home_name': match_result[8],
                    'team_away_name': match_result[9],
                    'predictions': predictions
                }
                
        except Exception as e:
            self.logger.error(f"Ошибка получения деталей матча {match_id}: {e}")
            return {}
    
    def get_championship_list(self) -> List[Dict[str, Any]]:
        """Получает список чемпионатов с количеством прогнозов."""
        try:
            with Session_pool() as db_session:
                query = """
                    SELECT 
                        s.championship_id,
                        ch.championshipName,
                        sp.sportName,
                        COUNT(*) as total_predictions,
                        SUM(CASE WHEN prediction_correct = 1 THEN 1 ELSE 0 END) as correct_predictions
                    FROM statistics_optimized s
                    LEFT JOIN championships ch ON s.championship_id = ch.id
                    LEFT JOIN sports sp ON s.sport_id = sp.id
                    GROUP BY s.championship_id, ch.championshipName, sp.sportName
                    ORDER BY total_predictions DESC
                """
                
                results = db_session.execute(text(query)).fetchall()
                
                championships = []
                for row in results:
                    total = row[3]
                    correct = row[4] or 0
                    accuracy_percentage = (correct / total * 100) if total > 0 else 0.0
                    
                    championships.append({
                        'championship_id': row[0],
                        'championship_name': row[1],
                        'sport_name': row[2],
                        'total_predictions': total,
                        'correct_predictions': correct,
                        'accuracy_percentage': round(accuracy_percentage, 2)
                    })
                
                return championships
                
        except Exception as e:
            self.logger.error(f"Ошибка получения списка чемпионатов: {e}")
            return []
    
    def get_sport_list(self) -> List[Dict[str, Any]]:
        """Получает список видов спорта с количеством прогнозов."""
        try:
            with Session_pool() as db_session:
                query = """
                    SELECT 
                        s.sport_id,
                        sp.sportName,
                        COUNT(*) as total_predictions,
                        SUM(CASE WHEN prediction_correct = 1 THEN 1 ELSE 0 END) as correct_predictions
                    FROM statistics_optimized s
                    LEFT JOIN sports sp ON s.sport_id = sp.id
                    GROUP BY s.sport_id, sp.sportName
                    ORDER BY total_predictions DESC
                """
                
                results = db_session.execute(text(query)).fetchall()
                
                sports = []
                for row in results:
                    total = row[2]
                    correct = row[3] or 0
                    accuracy_percentage = (correct / total * 100) if total > 0 else 0.0
                    
                    sports.append({
                        'sport_id': row[0],
                        'sport_name': row[1],
                        'total_predictions': total,
                        'correct_predictions': correct,
                        'accuracy_percentage': round(accuracy_percentage, 2)
                    })
                
                return sports
                
        except Exception as e:
            self.logger.error(f"Ошибка получения списка видов спорта: {e}")
            return []


# Создаем экземпляр API для использования
statistics_api = StatisticsAPI()
