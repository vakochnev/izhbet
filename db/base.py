from logging import getLogger
from sqlalchemy.exc import IntegrityError, DataError, SQLAlchemyError
from sqlalchemy.orm import Session
#from db.models.base import BaseModel


logger = getLogger(__name__)


class DBSession:
    """Класс для работы с БД через сессию с поддержкой контекстного менеджера."""
    def __init__(self, session: Session):
        self._session = session
        self._is_managed = False

    def __enter__(self):
        """Поддержка контекстного менеджера."""
        self._is_managed = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Автоматический commit/rollback при выходе из with."""
        if exc_type is not None:
            logger.error(
                msg=f'[DBSession] Ошибка в транзакции: {exc_val}',
                exc_info=True
            )
            self.rollback()
            return False  # propagate exception

        try:
            self.commit()
            logger.debug('[DBSession] Транзакция успешно завершена')

        except SQLAlchemyError as e:
            logger.error(
                msg=f'[DBSession] Ошибка коммита: {e}',
                exc_info=True
            )
            self.rollback()
            return False

        finally:
            self.close()

        return True

    def query(self, *entities, **kwargs):
        return self._session.query(*entities, **kwargs)

    def add_model(self, model, need_flush: bool = False):
        self._session.add(model)
        #self._session.merge(model)
        if need_flush:
            self._session.flush([model])

    def update_model(self, model, need_flush: bool = False):
        self._session.merge(model)
        if need_flush:
            self._session.flush([model])

    def delete_model(self, model):
        if model is None:
            logger.warning('Попытка удаления None модели')
            return
        try:
            self._session.delete(model)
        except (IntegrityError, DataError) as e:
            logger.error(f'Ошибка удаления модели: {e}')

    def commit(self):
        self._session.commit()

    def rollback(self):
        self._session.rollback()

    def close(self):
        try:
            self._session.close()
        except Exception as e:
            logger.warning(f'Ошибка закрытия сессии: {e}')
