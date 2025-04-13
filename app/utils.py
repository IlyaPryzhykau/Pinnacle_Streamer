import logging
import hashlib
import re
from datetime import datetime


def setup_logging():
    """
    Настраивает базовую конфигурацию логгирования:
    - уровень INFO,
    - формат с меткой времени, уровнем и именем логгера.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )


def generate_pinnacle_key_hash(
    match_id: int,
    period: str,
    market: str,
    outcome: str
) -> str:
    """
    Генерирует уникальный ключ (MD5-хеш) для строки исхода Pinnacle
    на основе match_id, периода, типа маркета и исхода.
    """
    base_string = f'{match_id}-{period}-{market}-{outcome}'
    return hashlib.md5(base_string.encode('utf-8')).hexdigest()


def generate_analyzer_key_hash(
    match_id_pinnacle: int,
    match_id_lobbet: int,
    market_type: int,
    outcome: str
) -> str:
    """
    Генерирует уникальный ключ (MD5-хеш) для строки исхода анализатора
    на основе идентификаторов матчей, типа маркета и исхода.
    """
    base_string = f'{match_id_pinnacle}-{match_id_lobbet}-{market_type}-{outcome}'
    return hashlib.md5(base_string.encode('utf-8')).hexdigest()


def sanitize_filename_part(name: str) -> str:
    """
    Очищает строку для безопасного использования в имени файла.
    Заменяет < > на over/less, удаляет нежелательные символы,
    но сохраняет точку для чисел (например, 10.5).

    Пример:
    "P1 > 10.5" → "p1_over_10.5"
    """
    replacements = {
        '<': '_less',
        '>': '_over',
    }

    for old, new in replacements.items():
        name = name.replace(old, new)

    # Разрешаем только a-z, 0-9, _, и .
    return re.sub(r'[^a-z0-9_.]+', '_', name.lower().strip())


def format_filename(
    match_id: int,
    created_at: datetime,
    home: str,
    away: str,
    sport: str,
    outcome: str | None = None,
) -> str:
    """
    Формирует имя CSV-файла на основе даты, команд, вида спорта и идентификатора матча.
    Пример: 123456_2025-04-07_team1_vs_team2_soccer.csv
    """
    date_str = created_at.strftime('%Y-%m-%d')
    home = sanitize_filename_part(home)
    away = sanitize_filename_part(away)
    sport = sanitize_filename_part(sport)

    parts = [str(match_id), date_str, f'{home}_vs_{away}', sport]

    if outcome:
        parts.append(sanitize_filename_part(outcome))

    return '_'.join(parts) + '.csv'



def safe_parse_iso(dt_str: str) -> datetime:
    """
    Безопасно парсит ISO-строку даты с поддержкой наносекунд.
    Обрезает дробную часть до микросекунд, чтобы избежать ошибки от datetime.fromisoformat.
    """
    if dt_str.endswith('Z'):
        dt_str = dt_str[:-1]
    if '.' in dt_str:
        date_part, micro_part = dt_str.split('.')
        micro_part = (micro_part + '000000')[:6]  # дополняем и обрезаем
        dt_str = f'{date_part}.{micro_part}'
    return datetime.fromisoformat(dt_str)
