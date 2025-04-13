# Интервал сбора данных перед записью в базу (в секундах)
WRITE_INTERVAL = 60

# Порог "старости" CSV-файлов (в часах), старше которых они архивируются
ARCHIVE_AGE_THRESHOLD = 24  # часы

# Период архивирования старых CSV-файлов (в секундах)
ARCHIVE_INTERVAL = 9000  # 2,5 часа

# Порог "старости" матчей для выгрузки из базы (в часах)
OUTDATED_THRESHOLD = 3  # часы

# Интервал проверки и выгрузки завершённых матчей (в секундах)
EXPORT_INTERVAL_SECONDS = 7200  # 2 часа

# Интервал отправки архивов на Mega (в секундах)
MEGA_UPLOAD_INTERVAL_SECONDS = 9000  # 2,5 часа

# Допустимые виды спорта
ALLOWED_SPORTS = ('Soccer', 'Tennis')

PERIOD_MAP_FOOTBALL = {
    0: 'Match',
    1: '1H',
    2: '2H'
}
PERIOD_MAP_TENNIS = {
    0: 'Match',
    1: 'Set1',
    2: 'Set2',
    3: 'Set3',
    4: 'Set4',
    5: 'Set5'
}
