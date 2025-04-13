# Список колонок для экспорта данных Pinnacle в CSV.
# Каждая строка — это момент времени (CreatedAt) и коэффициенты по основным маркетам.
CSV_PINNACLE_COLUMNS = [
    'CreatedAt',  # Время записи коэффициентов
    'PeriodType',

    # Основная информация о матче
    'homeName', 'awayName', 'HomeScore', 'AwayScore',

    # Основные исходы на матч
    'Win1', 'WinNone', 'Win2',

    # Тоталы (пороговые значения + over/under)
    'Totals_1_WinMore', 'Totals_1_WinLess',
    'Totals_2_WinMore', 'Totals_2_WinLess',
    'Totals_3_WinMore', 'Totals_3_WinLess',

    # Фора (Handicap), 3 линии по 2 исхода каждая
    'Handicap_1_Win1', 'Handicap_1_Win2',
    'Handicap_2_Win1', 'Handicap_2_Win2',
    'Handicap_3_Win1', 'Handicap_3_Win2',

    # Индивидуальные тоталы 1-й команды
    'FirstTeamTotals_1_WinMore', 'FirstTeamTotals_1_WinLess',
    'FirstTeamTotals_2_WinMore', 'FirstTeamTotals_2_WinLess',

    # Индивидуальные тоталы 2-й команды
    'SecondTeamTotals_1_WinMore', 'SecondTeamTotals_1_WinLess',
    'SecondTeamTotals_2_WinMore', 'SecondTeamTotals_2_WinLess',

    # Победа по геймам (для тенниса)
    'Games_1_WinMore', 'Games_1_WinLess',
    'Games_2_WinMore', 'Games_2_WinLess',
    'Games_3_WinMore', 'Games_3_WinLess',
]


# Список колонок для экспорта данных анализатора в CSV.
# Включает в себя два источника (Pinnacle и Lobbet), маркет, ROI и маржу.
CSV_ANALYZER_COLUMNS = [
    'createdAt',      # Время формирования исходов
    'sportName',      # Вид спорта (Soccer, Tennis)

    'matchId_pinnacle', 'matchId_lobbet',  # Идентификаторы матчей

    'homeName', 'awayName',
    'homeScore', 'awayScore',

    'league_pinnacle', 'league_lobbet',

    'bookmaker_1', 'bookmaker_2',  # Названия источников

    'market',      # Название рынка
    'outcome',     # Конкретный исход в маркете

    'value_pinnacle', 'value_lobbet',  # Коэффициенты в обеих БК

    'roi',    # Return on Investment (потенциальная прибыль)
    'margin', # Маржа между букмекерами

    'marketType',  # Тип маркета (обычно используется как int-код)
]
