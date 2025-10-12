from db.models.table import Table
from config import Session_pool


def get_table_id(db_session, table_id: int) -> Table:
    return (
        db_session.query(Table).filter(
            Table.id == table_id
        ).first()
    )


def get_table(table_id: int) -> Table:
    with Session_pool() as session:
        return (
            session.query(Table).filter(
                Table.id == table_id
            ).one()
        )


def get_table_all() -> list:
    with Session_pool() as session:
        return session.query(Table).all()
