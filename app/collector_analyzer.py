import asyncio
import csv
import logging
import os
from datetime import datetime, timedelta

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.constants.settings import OUTDATED_THRESHOLD, EXPORT_INTERVAL_SECONDS
from app.db import SessionLocal
from app.models import AnalyzerOddsParsed
from app.constants.csv_columns import CSV_ANALYZER_COLUMNS
from app.constants.paths import EXPORT_ANALYZER_DIR
from app.utils import format_filename


logger = logging.getLogger(__name__)


async def collect_and_export_old_analyzer_data():
    """
    Находит и экспортирует устаревшие матчи анализатора, затем удаляет их из базы.
    """
    now = datetime.utcnow()
    outdated_time = now - timedelta(hours=OUTDATED_THRESHOLD)

    async with SessionLocal() as session:
        match_keys = await find_stale_analyzer_matches(session, outdated_time)

        for match_id, outcome in match_keys:
            await export_and_delete_analyzer_match(session, match_id, outcome)


async def find_stale_analyzer_matches(
        session: AsyncSession,
        outdated_time: datetime
) -> list[tuple[int, str]]:
    """
    Возвращает список пар (match_id_pinnacle, outcome), устаревших по времени.
    """
    subquery = (
        select(
            AnalyzerOddsParsed.match_id_pinnacle,
            AnalyzerOddsParsed.outcome,
            func.max(AnalyzerOddsParsed.created_at).label('max_created_at')
        )
        .group_by(AnalyzerOddsParsed.match_id_pinnacle, AnalyzerOddsParsed.outcome)
        .subquery()
    )

    result = await session.execute(
        select(subquery.c.match_id_pinnacle, subquery.c.outcome)
        .where(subquery.c.max_created_at < outdated_time)
    )

    match_keys = result.all()
    logger.info(f'🔍 Найдено {len(match_keys)} устаревших матчей-анализов')
    return match_keys


async def export_and_delete_analyzer_match(
        session: AsyncSession,
        match_id: int,
        outcome: str
):
    """
    Экспортирует данные по паре (match_id, outcome) в CSV и удаляет их из базы.
    """
    logger.info(f'📦 Экспорт анализатора: match={match_id}, outcome={outcome}')

    result = await session.stream(
        select(AnalyzerOddsParsed).where(
            AnalyzerOddsParsed.match_id_pinnacle == match_id,
            AnalyzerOddsParsed.outcome == outcome
        )
    )

    rows = []
    async for row in result.scalars():
        rows.append(row)

    if not rows:
        logger.warning(f'⚠️ Нет данных для match={match_id}, outcome={outcome}')
        return

    created_at_sample = rows[0].created_at
    home = rows[0].home_team
    away = rows[0].away_team
    sport = rows[0].sport_name
    file_name = format_filename(match_id, created_at_sample, home, away, sport, outcome)
    file_path = os.path.join(EXPORT_ANALYZER_DIR, file_name)

    with open(file_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_ANALYZER_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                'createdAt': row.raw_created_at,
                'sportName': row.sport_name,
                'matchId_pinnacle': row.match_id_pinnacle,
                'matchId_lobbet': row.match_id_lobbet,
                'homeName': row.home_team,
                'awayName': row.away_team,
                'homeScore': row.home_score,
                'awayScore': row.away_score,
                'league_pinnacle': row.league_pinnacle,
                'league_lobbet': row.league_lobbet,
                'bookmaker_1': 'Pinnacle',
                'bookmaker_2': 'Lobbet',
                'market': row.market_type,
                'outcome': row.outcome,
                'value_pinnacle': row.value_pinnacle,
                'value_lobbet': row.value_lobbet,
                'roi': row.roi,
                'margin': row.margin,
                'marketType': row.market_type,
            })

    await session.execute(
        AnalyzerOddsParsed.__table__.delete().where(
            AnalyzerOddsParsed.match_id_pinnacle == match_id,
            AnalyzerOddsParsed.outcome == outcome
        )
    )
    await session.commit()
    logger.info(f'✅ Экспортировано и удалено: match={match_id}, outcome={outcome}')


async def run_analyzer_collector_loop():
    """
    Циклично запускает сбор устаревших матчей с интервалом EXPORT_INTERVAL_SECONDS.
    """
    while True:
        await collect_and_export_old_analyzer_data()
        await asyncio.sleep(EXPORT_INTERVAL_SECONDS)
