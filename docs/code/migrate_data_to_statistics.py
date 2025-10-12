#!/usr/bin/env python3
"""
Скрипт для массовой интеграции существующих данных в statistics_optimized.
Интегрирует все outcomes и predictions в новую таблицу статистики.
"""

import logging
import sys
from datetime import datetime
from typing import Dict, Any, List, Tuple
from tqdm import tqdm

from config import Session_pool
from db.models.outcome import Outcome
from db.models.prediction import Prediction
from db.models.match import Match
from db.models.championship import ChampionShip
from db.models.sport import Sport
# from db.models.statistics_optimized import StatisticsOptimized

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration_statistics.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class StatisticsDataMigrator:
    """Класс для миграции данных в statistics_optimized."""
    
    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
        self.logger = logging.getLogger(__name__)
        
        # Маппинг типов прогнозов
        self.feature_mapping = {
            1: ('win_draw_loss', 'classification'),
            2: ('oz', 'classification'),
            3: ('goal_home', 'classification'),
            4: ('goal_away', 'classification'),
            5: ('total', 'classification'),
            6: ('total_home', 'classification'),
            7: ('total_away', 'classification'),
            8: ('total_amount', 'regression'),
            9: ('total_home_amount', 'regression'),
            10: ('total_away_amount', 'regression')
        }
    
    def migrate_outcomes(self) -> Dict[str, int]:
        """Мигрирует все outcomes в statistics_optimized."""
        self.logger.info("🔄 Начинаем миграцию outcomes...")
        
        with Session_pool() as db_session:
            # Получаем общее количество outcomes
            total_outcomes = db_session.query(Outcome).count()
            self.logger.info(f"📊 Найдено {total_outcomes} outcomes для миграции")
            
            # Получаем все outcomes с связанными данными
            outcomes = (
                db_session.query(Outcome)
                .join(Match, Outcome.match_id == Match.id)
                .join(ChampionShip, Match.tournament_id == ChampionShip.id)
                .join(Sport, ChampionShip.sport_id == Sport.id)
                .order_by(Outcome.id)
                .all()
            )
            
            migrated_count = 0
            error_count = 0
            skipped_count = 0
            
            # Обрабатываем батчами
            for i in tqdm(range(0, len(outcomes), self.batch_size), desc="Миграция outcomes"):
                batch = outcomes[i:i + self.batch_size]
                
                for outcome in batch:
                    try:
                        # Проверяем, не мигрирован ли уже
                        existing = (
                            db_session.query(StatisticsOptimized)
                            .filter(StatisticsOptimized.outcome_id == outcome.id)
                            .first()
                        )
                        
                        if existing:
                            skipped_count += 1
                            continue
                        
                        # Создаем запись статистики
                        statistics = self._create_statistics_from_outcome(outcome, db_session)
                        if statistics:
                            db_session.add(statistics)
                            migrated_count += 1
                        
                    except Exception as e:
                        self.logger.error(f"Ошибка миграции outcome {outcome.id}: {e}")
                        error_count += 1
                        continue
                
                # Коммитим батч
                try:
                    db_session.commit()
                except Exception as e:
                    self.logger.error(f"Ошибка коммита батча {i//self.batch_size + 1}: {e}")
                    db_session.rollback()
                    error_count += len(batch)
            
            result = {
                'total': total_outcomes,
                'migrated': migrated_count,
                'errors': error_count,
                'skipped': skipped_count
            }
            
            self.logger.info(f"✅ Миграция outcomes завершена: {result}")
            return result
    
    def migrate_predictions(self) -> Dict[str, int]:
        """Мигрирует все predictions в statistics_optimized."""
        self.logger.info("🔄 Начинаем миграцию predictions...")
        
        with Session_pool() as db_session:
            # Получаем общее количество predictions
            total_predictions = db_session.query(Prediction).count()
            self.logger.info(f"📊 Найдено {total_predictions} predictions для миграции")
            
            # Получаем все predictions с связанными данными
            predictions = (
                db_session.query(Prediction)
                .join(Match, Prediction.match_id == Match.id)
                .join(ChampionShip, Match.tournament_id == ChampionShip.id)
                .join(Sport, ChampionShip.sport_id == Sport.id)
                .order_by(Prediction.id)
                .all()
            )
            
            migrated_count = 0
            error_count = 0
            skipped_count = 0
            
            # Обрабатываем батчами
            for i in tqdm(range(0, len(predictions), self.batch_size), desc="Миграция predictions"):
                batch = predictions[i:i + self.batch_size]
                
                for prediction in batch:
                    try:
                        # Проверяем, не мигрирован ли уже
                        existing = (
                            db_session.query(StatisticsOptimized)
                            .filter(StatisticsOptimized.prediction_id == prediction.id)
                            .first()
                        )
                        
                        if existing:
                            skipped_count += 1
                            continue
                        
                        # Создаем запись статистики
                        statistics = self._create_statistics_from_prediction(prediction, db_session)
                        if statistics:
                            db_session.add(statistics)
                            migrated_count += 1
                        
                    except Exception as e:
                        self.logger.error(f"Ошибка миграции prediction {prediction.id}: {e}")
                        error_count += 1
                        continue
                
                # Коммитим батч
                try:
                    db_session.commit()
                except Exception as e:
                    self.logger.error(f"Ошибка коммита батча {i//self.batch_size + 1}: {e}")
                    db_session.rollback()
                    error_count += len(batch)
            
            result = {
                'total': total_predictions,
                'migrated': migrated_count,
                'errors': error_count,
                'skipped': skipped_count
            }
            
            self.logger.info(f"✅ Миграция predictions завершена: {result}")
            return result
    
    def _create_statistics_from_outcome(self, outcome: Outcome, db_session) -> StatisticsOptimized:
        """Создает запись StatisticsOptimized из Outcome."""
        try:
            # Получаем связанные данные
            match = db_session.query(Match).filter(Match.id == outcome.match_id).first()
            if not match:
                return None
            
            championship = db_session.query(ChampionShip).filter(ChampionShip.id == match.tournament_id).first()
            if not championship:
                return None
            
            sport = db_session.query(Sport).filter(Sport.id == championship.sport_id).first()
            if not sport:
                return None
            
            # Определяем тип прогноза
            forecast_type, model_type = self.feature_mapping.get(outcome.feature, ('unknown', 'unknown'))
            forecast_subtype = outcome.outcome or 'unknown'
            
            # Вычисляем результат матча
            actual_result, actual_value = self._calculate_actual_result(match)
            
            # Создаем запись статистики
            statistics = StatisticsOptimized(
                outcome_id=outcome.id,
                prediction_id=None,
                match_id=outcome.match_id,
                championship_id=match.tournament_id,
                sport_id=championship.sport_id,
                match_date=match.gameData.date() if match.gameData else datetime.now().date(),
                match_round=getattr(match, 'tour', None),
                match_stage=getattr(match, 'stage', None),
                forecast_type=forecast_type,
                forecast_subtype=forecast_subtype,
                model_name='conformal_predictor',
                model_version='1.0',
                model_type=model_type,
                actual_result=actual_result,
                actual_value=actual_value,
                prediction_correct=None,  # Будет вычислено позже
                prediction_accuracy=None,  # Будет вычислено позже
                prediction_error=None,  # Будет вычислено позже
                prediction_residual=None,  # Будет вычислено позже
                coefficient=None,
                potential_profit=None,
                actual_profit=None
            )
            
            return statistics
            
        except Exception as e:
            self.logger.error(f"Ошибка создания статистики из outcome {outcome.id}: {e}")
            return None
    
    def _create_statistics_from_prediction(self, prediction: Prediction, db_session) -> StatisticsOptimized:
        """Создает запись StatisticsOptimized из Prediction."""
        try:
            # Получаем связанные данные
            match = db_session.query(Match).filter(Match.id == prediction.match_id).first()
            if not match:
                return None
            
            championship = db_session.query(ChampionShip).filter(ChampionShip.id == match.tournament_id).first()
            if not championship:
                return None
            
            sport = db_session.query(Sport).filter(Sport.id == championship.sport_id).first()
            if not sport:
                return None
            
            # Для predictions сложнее определить тип, используем общий
            forecast_type = 'prediction'
            forecast_subtype = 'general'
            model_type = 'classification'
            
            # Вычисляем результат матча
            actual_result, actual_value = self._calculate_actual_result(match)
            
            # Создаем запись статистики
            statistics = StatisticsOptimized(
                outcome_id=None,
                prediction_id=prediction.id,
                match_id=prediction.match_id,
                championship_id=match.tournament_id,
                sport_id=championship.sport_id,
                match_date=match.gameData.date() if match.gameData else datetime.now().date(),
                match_round=getattr(match, 'tour', None),
                match_stage=getattr(match, 'stage', None),
                forecast_type=forecast_type,
                forecast_subtype=forecast_subtype,
                model_name=prediction.model_name or 'keras_model',
                model_version='1.0',
                model_type=model_type,
                actual_result=actual_result,
                actual_value=actual_value,
                prediction_correct=None,  # Будет вычислено позже
                prediction_accuracy=None,  # Будет вычислено позже
                prediction_error=None,  # Будет вычислено позже
                prediction_residual=None,  # Будет вычислено позже
                coefficient=None,
                potential_profit=None,
                actual_profit=None
            )
            
            return statistics
            
        except Exception as e:
            self.logger.error(f"Ошибка создания статистики из prediction {prediction.id}: {e}")
            return None
    
    def _calculate_actual_result(self, match: Match) -> Tuple[str, float]:
        """Вычисляет фактический результат матча."""
        if not match.numOfHeadsHome or not match.numOfHeadsAway:
            return None, None
        
        goal_home = int(match.numOfHeadsHome)
        goal_away = int(match.numOfHeadsAway)
        
        # Определяем результат матча
        if goal_home > goal_away:
            actual_result = 'home_win'
        elif goal_home < goal_away:
            actual_result = 'away_win'
        else:
            actual_result = 'draw'
        
        # Для регрессии (total_amount) возвращаем сумму голов
        actual_value = float(goal_home + goal_away)
        
        return actual_result, actual_value
    
    def verify_migration(self) -> Dict[str, Any]:
        """Проверяет результаты миграции."""
        self.logger.info("🔍 Проверяем результаты миграции...")
        
        with Session_pool() as db_session:
            # Подсчитываем записи
            total_outcomes = db_session.query(Outcome).count()
            total_predictions = db_session.query(Prediction).count()
            total_statistics = db_session.query(StatisticsOptimized).count()
            
            # Проверяем связи
            statistics_with_outcomes = (
                db_session.query(StatisticsOptimized)
                .filter(StatisticsOptimized.outcome_id.isnot(None))
                .count()
            )
            
            statistics_with_predictions = (
                db_session.query(StatisticsOptimized)
                .filter(StatisticsOptimized.prediction_id.isnot(None))
                .count()
            )
            
            # Проверяем дубликаты
            duplicate_outcomes = (
                db_session.query(StatisticsOptimized.outcome_id)
                .filter(StatisticsOptimized.outcome_id.isnot(None))
                .group_by(StatisticsOptimized.outcome_id)
                .having(db_session.func.count() > 1)
                .count()
            )
            
            duplicate_predictions = (
                db_session.query(StatisticsOptimized.prediction_id)
                .filter(StatisticsOptimized.prediction_id.isnot(None))
                .group_by(StatisticsOptimized.prediction_id)
                .having(db_session.func.count() > 1)
                .count()
            )
            
            result = {
                'total_outcomes': total_outcomes,
                'total_predictions': total_predictions,
                'total_statistics': total_statistics,
                'statistics_with_outcomes': statistics_with_outcomes,
                'statistics_with_predictions': statistics_with_predictions,
                'duplicate_outcomes': duplicate_outcomes,
                'duplicate_predictions': duplicate_predictions,
                'coverage_outcomes': (statistics_with_outcomes / total_outcomes * 100) if total_outcomes > 0 else 0,
                'coverage_predictions': (statistics_with_predictions / total_predictions * 100) if total_predictions > 0 else 0
            }
            
            self.logger.info(f"📊 Результаты проверки: {result}")
            return result


def main():
    """Основная функция миграции."""
    print("🚀 МИГРАЦИЯ ДАННЫХ В STATISTICS_OPTIMIZED")
    print("=" * 50)
    
    migrator = StatisticsDataMigrator(batch_size=1000)
    
    try:
        # 1. Мигрируем outcomes
        print("\\n📊 Этап 1: Миграция outcomes...")
        outcomes_result = migrator.migrate_outcomes()
        
        # 2. Мигрируем predictions
        print("\\n📊 Этап 2: Миграция predictions...")
        predictions_result = migrator.migrate_predictions()
        
        # 3. Проверяем результаты
        print("\\n🔍 Этап 3: Проверка результатов...")
        verification_result = migrator.verify_migration()
        
        # 4. Выводим итоговый отчет
        print("\\n" + "=" * 50)
        print("📈 ИТОГОВЫЙ ОТЧЕТ МИГРАЦИИ")
        print("=" * 50)
        
        print(f"\\n📊 OUTCOMES:")
        print(f"   Всего: {outcomes_result['total']}")
        print(f"   Мигрировано: {outcomes_result['migrated']}")
        print(f"   Ошибок: {outcomes_result['errors']}")
        print(f"   Пропущено: {outcomes_result['skipped']}")
        
        print(f"\\n📊 PREDICTIONS:")
        print(f"   Всего: {predictions_result['total']}")
        print(f"   Мигрировано: {predictions_result['migrated']}")
        print(f"   Ошибок: {predictions_result['errors']}")
        print(f"   Пропущено: {predictions_result['skipped']}")
        
        print(f"\\n📊 STATISTICS_OPTIMIZED:")
        print(f"   Всего записей: {verification_result['total_statistics']}")
        print(f"   С outcomes: {verification_result['statistics_with_outcomes']}")
        print(f"   С predictions: {verification_result['statistics_with_predictions']}")
        print(f"   Покрытие outcomes: {verification_result['coverage_outcomes']:.1f}%")
        print(f"   Покрытие predictions: {verification_result['coverage_predictions']:.1f}%")
        
        if verification_result['duplicate_outcomes'] > 0:
            print(f"   ⚠️ Дубликаты outcomes: {verification_result['duplicate_outcomes']}")
        
        if verification_result['duplicate_predictions'] > 0:
            print(f"   ⚠️ Дубликаты predictions: {verification_result['duplicate_predictions']}")
        
        print("\\n✅ Миграция завершена успешно!")
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка миграции: {e}")
        print(f"\\n❌ Критическая ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
