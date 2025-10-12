from config import Session_pool
from db.models import Statistic


def get_team_typeoutcome_id(
    db_session,
    team_id: int,
    type_outcome: str,
    vid_outcome: str,
    outcome: str
) -> Statistic:
    return (
        db_session.query(Statistic).filter(
            Statistic.team_id == team_id,
            Statistic.forecast_type == type_outcome,
            Statistic.forecast_vid == vid_outcome,
            Statistic.forecast == outcome
        ).first()
    )


def get_statistic_team_id(
        db_session,
        team_id: int,
        forecast_vid: str,
        forecast_type: str,
        forecast: str
) -> Statistic | None:
    # Проверяем на NaN значения
    import pandas as pd
    if pd.isna(team_id):
        return None
    
    return (
        db_session.query(Statistic).filter(
            Statistic.team_id == team_id,
            Statistic.forecast_type == forecast_type,
            Statistic.forecast_vid == forecast_vid,
            Statistic.forecast == forecast
        ).first()
    )


def get_team_in_statistics_all(
    teams_id: list[int]
) -> list[int]:
    # добавил
    with Session_pool() as session:
        return (
            session.query(Statistic).filter(
                Statistic.team_id.in_(teams_id)
            ).all()
        )
