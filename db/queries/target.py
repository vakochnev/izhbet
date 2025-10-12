"""
Запросы для работы с целевыми переменными (targets).
"""

import logging
from typing import List, Dict, Any, Optional
import pandas as pd
from sqlalchemy import text

from config import Session_pool, DBSession
from db.models import Target


logger = logging.getLogger(__name__)


class TargetQueries:
    """Класс для работы с запросами target данных."""
    
    def __init__(self):
        pass

    def get_targets_for_training(self, match_ids: Optional[List[int]] = None) -> pd.DataFrame:
        """
        Получает target данные для обучения моделей.
        
        Args:
            match_ids: Список ID матчей для фильтрации (опционально)
            
        Returns:
            pd.DataFrame: DataFrame с target данными
        """
        with Session_pool() as session:
            query = session.query(Target)
            if match_ids:
                query = query.filter(Target.match_id.in_(match_ids))
            
            targets = query.all()
            
            if not targets:
                logger.warning("Не найдено target данных для обучения.")
                return pd.DataFrame()
            
            df = pd.DataFrame([t.as_dict() for t in targets])
            return df

    def get_target_by_match_id(self, match_id: int) -> Optional[Target]:
        """
        Получает target по match_id.
        
        Args:
            match_id: ID матча
            
        Returns:
            Target: Объект Target или None если не найден
        """
        with Session_pool() as session:
            return session.query(Target).filter_by(match_id=match_id).first()


def get_target_match_ids(
        db_session: DBSession,
        match_ids: list[int]
) -> list[Target]:
    return (
        db_session.query(Target).filter(
            Target.match_id.in_(match_ids)
        ).all()
    )
