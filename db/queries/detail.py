from db.models import Detail
from config import Session_pool


def get_detail_id(detail_id: int) -> Detail:
    with Session_pool() as session:
        return (
            session.query(Detail).filter(
                Detail.id == detail_id
            ).first()
        )
