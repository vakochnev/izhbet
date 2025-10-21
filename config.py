import sys
import logging
from datetime import datetime
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool, QueuePool

from db.base import DBSession


LOG_FILE = f"logs/{datetime.now().strftime('%d-%m-%Y')}.log"
LOG_STREAM = sys.stdout


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] %(filename)s->%(funcName)s()'
           ':%(lineno)s - %(message)s',
    handlers=[logging.StreamHandler(LOG_STREAM),
              logging.FileHandler(LOG_FILE)],
    encoding='utf-8'
)


def get_db_session():
    engine = create_engine(
        settings.DATABASE_URL_mysql,
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=3600,
        # Убрали AUTOCOMMIT для управления транзакциями вручную
        # isolation_level='AUTOCOMMIT',
    )
    # Отключаем autoflush для избежания блокировок при запросах
    # session = sessionmaker(bind=engine)()
    session = sessionmaker(bind=engine, autoflush=False, autocommit=False)()
    return DBSession(session)


class Settings(BaseSettings):
    DATABASE_USER: str
    DATABASE_PASSWORD: str
    DATABASE_HOST: str
    DATABASE_PORT: int
    DATABASE_NAME: str
    DATABASE_CHARSET: str
    DATABSE_USE_UNICODE: str
    CONTAINER_NAME: str

    @property
    def DATABASE_URL_mysql(self) -> str:
        return (
            f'mysql+pymysql://{self.DATABASE_USER}:'
            f'{self.DATABASE_PASSWORD}@{self.DATABASE_HOST}:'
            f'{self.DATABASE_PORT}/{self.DATABASE_NAME}?'
            f'charset={self.DATABASE_CHARSET}'
        )

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8'
    )

settings = Settings()

engine: Engine = create_engine(
    url=settings.DATABASE_URL_mysql,
    echo=False,
    # pool_size=10,
    # max_overflow=10,
    # pool_timeout=30,
    poolclass=NullPool,
    pool_pre_ping=True,
    pool_recycle=3600,
    pool_reset_on_return=None #@, isolation_level='AUTOCOMMIT'
)

Session_pool = scoped_session(
    sessionmaker(bind=engine, autoflush=False, autocommit=False)
)
db_session_pool = DBSession(Session_pool())

Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
db_session = DBSession(Session())
