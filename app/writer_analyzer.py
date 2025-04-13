import logging
from typing import Any

from app.db import SessionLocal
from app.models import AnalyzerOddsParsed
from app.utils import generate_analyzer_key_hash, safe_parse_iso
from sqlalchemy.ext.asyncio import AsyncSession


logger = logging.getLogger(__name__)


async def write_analyzer_to_storage(messages: list[dict[str, Any]]):
    """
    Обрабатывает список сообщений от анализатора, преобразует их в объекты AnalyzerOddsParsed
    и сохраняет в базу данных.

    :param messages: Список словарей с данными от анализатора
    """
    if not messages:
        return

    parsed_rows = []
    seen_keys = set()

    for msg in messages:
        try:
            sport_name = msg.get('sportName')
            first = msg.get('first', {})
            second = msg.get('second', {})
            outcome_list = msg.get('outcome', [])

            match_id_pinnacle = int(first['matchId'])
            match_id_lobbet = int(second['matchId'])

            created_at_str = msg['createdAt']
            created_at_dt = safe_parse_iso(created_at_str)

            home_team = first['homeName']
            away_team = first['awayName']
            home_score = first.get('homeScore')
            away_score = first.get('awayScore')
            league_pinnacle = first.get('leagueName')
            league_lobbet = second.get('leagueName')

            for outcome_data in outcome_list:
                outcome = outcome_data['outcome']
                market_type = int(outcome_data.get('marketType', -999))
                value_pinnacle = _safe_float(outcome_data.get('score1', {}).get('value'))
                value_lobbet = _safe_float(outcome_data.get('score2', {}).get('value'))
                roi = _safe_float(outcome_data.get('roi'))
                margin = _safe_float(outcome_data.get('margin'))

                key_hash = generate_analyzer_key_hash(
                    match_id_pinnacle, match_id_lobbet, market_type, outcome)

                if (created_at_str, outcome) in seen_keys:
                    continue
                seen_keys.add((created_at_str, outcome))

                parsed_rows.append(AnalyzerOddsParsed(
                    match_id_pinnacle=match_id_pinnacle,
                    match_id_lobbet=match_id_lobbet,
                    home_team=home_team,
                    away_team=away_team,
                    home_score=home_score,
                    away_score=away_score,
                    sport_name=sport_name,
                    league_pinnacle=league_pinnacle,
                    league_lobbet=league_lobbet,
                    market_type=market_type,
                    outcome=outcome,
                    value_pinnacle=value_pinnacle,
                    value_lobbet=value_lobbet,
                    roi=roi,
                    margin=margin,
                    created_at=created_at_dt,
                    raw_created_at=created_at_str,
                    key_hash=key_hash
                ))

        except Exception as e:
            logger.warning(f'❌ Ошибка при обработке analyzer-сообщения: {e}\n📦 Сообщение: {msg}')

    if parsed_rows:
        async with SessionLocal() as session:
            await save_analyzer_rows(session, parsed_rows)


async def save_analyzer_rows(session: AsyncSession, rows: list[AnalyzerOddsParsed]):
    """
    Сохраняет список объектов AnalyzerOddsParsed в базу данных через переданную сессию.

    :param session: Асинхронная сессия SQLAlchemy
    :param rows: Список строк для записи
    """
    session.add_all(rows)
    await session.commit()
    logger.info(f'✅ Сохранили {len(rows)} строк от анализатора')


def _safe_float(val: Any) -> float | None:
    """
    Преобразует значение в float, если возможно. Иначе возвращает None.

    :param val: Значение для преобразования
    :return: Число или None
    """
    try:
        return float(val) if val is not None else None
    except Exception:
        return None
