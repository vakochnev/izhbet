"""
Хранилище для работы с целевыми переменными (targets).
"""

import logging
from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from db.models.target import Target
from config import Session_pool

logger = logging.getLogger(__name__)


class TargetStorage:
    """Класс для работы с целевыми переменными в БД."""

    def save_target(self, target_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Сохраняет или обновляет целевую переменную.
        
        Args:
            target_data: Словарь с данными целевой переменной
            
        Returns:
            Dict[str, Any]: Словарь с данными сохраненной целевой переменной или None при ошибке
        """
        try:
            logger.debug(f"Сохранение target данных для матча {target_data.get('match_id')}")
            with Session_pool() as db_session:
                # Проверяем, существует ли уже запись для этого матча
                existing_target = db_session.query(Target).filter(
                    Target.match_id == target_data['match_id']
                ).first()
                
                if existing_target:
                    # Обновляем существующую запись
                    for key, value in target_data.items():
                        if hasattr(existing_target, key):
                            setattr(existing_target, key, value)
                    db_session.commit()
                    db_session.refresh(existing_target)  # Обновляем объект с актуальными данными
                    # Получаем словарь до закрытия сессии и исключаем relationship
                    result = {
                        key: value
                        for key, value in existing_target.__dict__.items()
                        if not key.startswith('_') and key not in ['query', 'metadata', 'matchs']
                    }
                    logger.debug(f"Target обновлен для матча {target_data['match_id']}")
                    return result
                else:
                    # Создаем новую запись
                    target = Target(**target_data)
                    db_session.add(target)
                    db_session.commit()
                    db_session.refresh(target)  # Обновляем объект с ID
                    # Получаем словарь до закрытия сессии и исключаем relationship
                    result = {
                        key: value
                        for key, value in target.__dict__.items()
                        if not key.startswith('_') and key not in ['query', 'metadata', 'matchs']
                    }
                    logger.debug(f"Target создан для матча {target_data['match_id']} с ID {result.get('id')}")
                    return result
                    
        except IntegrityError as e:
            logger.error(f"Ошибка целостности при сохранении целевой переменной: {e}")
            db_session.rollback()
            return None
        except Exception as e:
            logger.error(f"Ошибка при сохранении целевой переменной: {e}")
            try:
                db_session.rollback()
            except:
                pass
            return None

    def save_targets_batch(self, targets_data: List[Dict[str, Any]]) -> int:
        """
        Сохраняет множество целевых переменных пакетом.
        
        Args:
            targets_data: Список словарей с данными целевых переменных
            
        Returns:
            int: Количество успешно сохраненных записей
        """
        saved_count = 0
        
        try:
            with Session_pool() as db_session:
                for target_data in targets_data:
                    try:
                        # Проверяем существование записи
                        existing_target = db_session.query(Target).filter(
                            Target.match_id == target_data['match_id']
                        ).first()
                        
                        if existing_target:
                            # Обновляем
                            for key, value in target_data.items():
                                if hasattr(existing_target, key):
                                    setattr(existing_target, key, value)
                        else:
                            # Создаем новую
                            target = Target(**target_data)
                            db_session.add(target)
                        
                        saved_count += 1
                        
                    except Exception as e:
                        logger.warning(f"Ошибка при сохранении целевой переменной для матча {target_data.get('match_id', 'unknown')}: {e}")
                        continue
                
                db_session.commit()
                logger.info(f"Сохранено {saved_count} целевых переменных из {len(targets_data)}")
                
        except Exception as e:
            logger.error(f"Ошибка при пакетном сохранении целевых переменных: {e}")
            db_session.rollback()
            
        return saved_count

    def get_target_by_match_id(self, match_id: int) -> Optional[Target]:
        """
        Получает целевую переменную по ID матча.
        
        Args:
            match_id: ID матча
            
        Returns:
            Target: Объект целевой переменной или None
        """
        try:
            with Session_pool() as db_session:
                return db_session.query(Target).filter(
                    Target.match_id == match_id
                ).first()
        except Exception as e:
            logger.error(f"Ошибка при получении целевой переменной для матча {match_id}: {e}")
            return None

    def get_targets_by_match_ids(self, match_ids: List[int]) -> List[Target]:
        """
        Получает целевые переменные по списку ID матчей.
        
        Args:
            match_ids: Список ID матчей
            
        Returns:
            List[Target]: Список объектов целевых переменных
        """
        try:
            with Session_pool() as db_session:
                return db_session.query(Target).filter(
                    Target.match_id.in_(match_ids)
                ).all()
        except Exception as e:
            logger.error(f"Ошибка при получении целевых переменных для матчей {match_ids}: {e}")
            return []

    def delete_target(self, match_id: int) -> bool:
        """
        Удаляет целевую переменную по ID матча.
        
        Args:
            match_id: ID матча
            
        Returns:
            bool: True если удаление успешно, False иначе
        """
        try:
            with Session_pool() as db_session:
                target = db_session.query(Target).filter(
                    Target.match_id == match_id
                ).first()
                
                if target:
                    db_session.delete(target)
                    db_session.commit()
                    logger.info(f"Удалена целевая переменная для матча {match_id}")
                    return True
                else:
                    logger.warning(f"Целевая переменная для матча {match_id} не найдена")
                    return False
                    
        except Exception as e:
            logger.error(f"Ошибка при удалении целевой переменной для матча {match_id}: {e}")
            db_session.rollback()
            return False

    def get_all_targets(self, limit: Optional[int] = None) -> List[Target]:
        """
        Получает все целевые переменные.
        
        Args:
            limit: Максимальное количество записей
            
        Returns:
            List[Target]: Список всех целевых переменных
        """
        try:
            with Session_pool() as db_session:
                query = db_session.query(Target)
                if limit:
                    query = query.limit(limit)
                return query.all()
        except Exception as e:
            logger.error(f"Ошибка при получении всех целевых переменных: {e}")
            return []
