import asyncio
import csv
import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.constants.csv_columns import CSV_PINNACLE_COLUMNS
from app.constants.paths import EXPORT_PINNACLE_DIR
from app.constants.settings import OUTDATED_THRESHOLD, EXPORT_INTERVAL_SECONDS
from app.db import SessionLocal
from app.models import LiveOddsParsed
from app.utils import format_filename

logger = logging.getLogger(__name__)


def expand_market_map(rows: list[LiveOddsParsed]) -> dict[str, dict[str, float]]:
    """
    Группирует данные по created_at и period, и преобразует их в формат для экспорта в CSV,
    включая Totals, Handicap, First/Second Team Totals и Games (всё по слотам).
    """
    snapshot_dict = defaultdict(dict)

    slot_maps = {
        'Totals': [],
        'Handicap': [],
        'FirstTeamTotals': [],
        'SecondTeamTotals': [],
        'Games': [],
    }

    for row in rows:
        timestamp = row.created_at.replace(microsecond=0).isoformat()
        period_type = row.period
        key = f'{timestamp}|{period_type}'

        snap = snapshot_dict[key]
        snap['CreatedAt'] = timestamp
        snap['PeriodType'] = period_type
        snap['homeName'] = row.home_team
        snap['awayName'] = row.away_team
        snap['HomeScore'] = row.home_score or 0
        snap['AwayScore'] = row.away_score or 0

        market = row.market
        outcome = row.outcome
        value = row.value
        line = row.line

        try:
            line_value = float(line) if line else 0.0
        except ValueError:
            line_value = 0.0

        col = None

        if market == 'Totals' and outcome in {'WinMore', 'WinLess'}:
            col = _slot_column('Totals', line_value, outcome, slot_maps, max_slots=3)
        elif market == 'Handicap' and outcome in {'Win1', 'Win2'}:
            col = _slot_column('Handicap', line_value, outcome, slot_maps, max_slots=3)
        elif market == 'FirstTeamTotals' and outcome in {'WinMore', 'WinLess'}:
            col = _slot_column('FirstTeamTotals', line_value, outcome, slot_maps, max_slots=2)
        elif market == 'SecondTeamTotals' and outcome in {'WinMore', 'WinLess'}:
            col = _slot_column('SecondTeamTotals', line_value, outcome, slot_maps, max_slots=2)
        elif market == 'Games' and outcome in {'WinMore', 'WinLess'}:
            col = _slot_column('Games', line_value, outcome, slot_maps, max_slots=3)
        elif market == 'Win1x2':
            col = outcome

        if col in CSV_PINNACLE_COLUMNS:
            snap[col] = value

    return snapshot_dict


def _slot_column(prefix: str, line_value: float, outcome: str, slot_maps: dict, max_slots: int) -> str:
    """
    Возвращает имя колонки с номером слота на основе значения линии.
    Привязывает линию к одному из max_slots слотов.
    """
    slots = slot_maps[prefix]
    if line_value not in slots and len(slots) < max_slots:
        slots.append(line_value)
    try:
        slot = slots.index(line_value) + 1
        return f'{prefix}_{slot}_{outcome}'
    except ValueError:
        return ''


async def collect_and_export_old_data():
    """
    Находит устаревшие матчи по данным от Pinnacle и экспортирует их в CSV.
    После экспорта удаляет данные матчи из базы.
    """
    now = datetime.utcnow()
    outdated_time = now - timedelta(hours=OUTDATED_THRESHOLD)

    async with SessionLocal() as session:
        match_ids = await find_stale_matches(session, outdated_time)

        for match_id in match_ids:
            await export_and_delete_match(session, match_id)


async def find_stale_matches(session: AsyncSession, outdated_time: datetime) -> list[int]:
    """
    Возвращает список матчей, которые не обновлялись дольше указанного времени.
    """
    subquery = (
        select(
            LiveOddsParsed.match_id,
            func.max(LiveOddsParsed.created_at).label('max_created_at')
        )
        .group_by(LiveOddsParsed.match_id)
        .subquery()
    )

    result = await session.execute(
        select(subquery.c.match_id)
        .where(subquery.c.max_created_at < outdated_time)
    )

    match_ids = [row[0] for row in result.all()]
    logger.info(f'🔍 Найдено {len(match_ids)} устаревших матчей для выгрузки')
    return match_ids


async def export_and_delete_match(session: AsyncSession, match_id: int):
    """
    Экспортирует данные по заданному матчу Pinnacle в CSV и удаляет их из базы.
    """
    logger.info(f'Обрабатываем матч {match_id}')
    result = await session.stream(
        select(LiveOddsParsed).where(LiveOddsParsed.match_id == match_id)
    )
    rows = []
    async for row in result.scalars():
        rows.append(row)

    if not rows:
        logger.warning(f'⚠️ Нет данных для матча {match_id}')
        return

    snapshot_dict = expand_market_map(rows)

    created_at_sample = rows[0].created_at
    home = rows[0].home_team or 'home'
    away = rows[0].away_team or 'away'
    sport = rows[0].sport_name or 'sport'
    file_name = format_filename(match_id, created_at_sample, home, away, sport)
    file_path = os.path.join(EXPORT_PINNACLE_DIR, file_name)

    with open(file_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_PINNACLE_COLUMNS)
        writer.writeheader()

        for ts, row_data in sorted(snapshot_dict.items()):
            row = {col: 'null' for col in CSV_PINNACLE_COLUMNS}
            row['CreatedAt'] = ts
            row['homeName'] = home
            row['awayName'] = away
            row['HomeScore'] = rows[0].home_score or 0
            row['AwayScore'] = rows[0].away_score or 0

            for k, v in row_data.items():
                if k in row:
                    row[k] = 'null' if v is None else v

            writer.writerow(row)

    await session.execute(
        LiveOddsParsed.__table__.delete().where(LiveOddsParsed.match_id == match_id)
    )
    await session.commit()
    logger.info(f'✅ Матч {match_id} экспортирован и удалён')


async def run_pinnacle_collector_loop():
    """
    Цикл экспорта и удаления устаревших матчей Pinnacle.
    Выполняется с интервалом EXPORT_INTERVAL_SECONDS.
    """
    while True:
        await collect_and_export_old_data()
        await asyncio.sleep(EXPORT_INTERVAL_SECONDS)
