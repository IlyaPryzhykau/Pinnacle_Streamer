from sqlalchemy import (Column, BigInteger, String, Float, TIMESTAMP,
                        Index, Integer, desc)
from sqlalchemy.orm import declarative_base


Base = declarative_base()


class LiveOddsParsed(Base):
    """
    Модель для хранения live-коэффициентов от Pinnacle.

    Поля:
    - match_id: ID матча
    - home_team, away_team: Названия команд
    - home_score, away_score: Счёт матча
    - sport_name: Вид спорта
    - source: Источник (например, "Pinnacle")
    - league_name: Название лиги
    - period: Период (например, "Match", "H1", "Set1")
    - market: Название маркета (например, Totals, Handicap)
    - outcome: Исход (например, Win1, WinMore)
    - line: Линия, если применимо (например, 2.5)
    - value: Коэффициент
    - created_at: Время получения данных
    - key_hash: Уникальный хеш записи (используется для поиска)
    """
    __tablename__ = 'live_odds_parsed'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    match_id = Column(BigInteger, nullable=False)
    home_team = Column(String, nullable=False)
    away_team = Column(String, nullable=False)
    home_score = Column(Integer, nullable=True)
    away_score = Column(Integer, nullable=True)
    sport_name = Column(String, nullable=False)
    source = Column(String, nullable=True)
    league_name = Column(String, nullable=True)
    period = Column(String, nullable=True)
    market = Column(String, nullable=False)
    outcome = Column(String, nullable=False)
    line = Column(String, nullable=True)
    value = Column(Float, nullable=True)
    created_at = Column(TIMESTAMP, nullable=False)
    key_hash = Column(String(64), nullable=False)

    __table_args__ = (
        Index('ix_liveodds_keyhash_created', 'key_hash', 'created_at'),
        Index('ix_liveodds_match_created', 'match_id', desc('created_at')),
    )


class AnalyzerOddsParsed(Base):
    """
    Модель для хранения парсинга сравнительных коэффициентов между Pinnacle и Lobbet.

    Поля:
    - match_id_pinnacle, match_id_lobbet: ID матчей в соответствующих БК
    - home_team, away_team: Названия команд
    - home_score, away_score: Счёт
    - sport_name: Вид спорта
    - league_pinnacle, league_lobbet: Названия лиг
    - market_type: Тип маркета (int, используется внутри проекта)
    - outcome: Название исхода
    - value_pinnacle, value_lobbet: Коэффициенты от БК
    - roi: Потенциальный ROI между букмекерами
    - margin: Маржа
    - created_at: Время получения данных
    - raw_created_at: Оригинальная строка времени с наносекундами
    - key_hash: Уникальный хеш для идентификации строки
    """
    __tablename__ = 'analyzer_odds_parsed'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    match_id_pinnacle = Column(BigInteger, nullable=False)
    match_id_lobbet = Column(BigInteger, nullable=False)
    home_team = Column(String, nullable=False)
    away_team = Column(String, nullable=False)
    home_score = Column(Integer, nullable=True)
    away_score = Column(Integer, nullable=True)
    sport_name = Column(String, nullable=False)
    league_pinnacle = Column(String, nullable=True)
    league_lobbet = Column(String, nullable=True)
    market_type = Column(Integer, nullable=False)
    outcome = Column(String, nullable=False)
    value_pinnacle = Column(Float, nullable=True)
    value_lobbet = Column(Float, nullable=True)
    roi = Column(Float, nullable=True)
    margin = Column(Float, nullable=True)
    created_at = Column(TIMESTAMP, nullable=False)
    raw_created_at = Column(String, nullable=False)
    key_hash = Column(String(64), nullable=False)

    __table_args__ = (
        Index('ix_analyzer_keyhash_created', 'key_hash', 'created_at'),
        Index('ix_analyzer_match_created', 'match_id_pinnacle', desc('created_at')),
    )
