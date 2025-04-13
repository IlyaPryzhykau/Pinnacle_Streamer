from pathlib import Path

# Базовая директория для экспорта данных и архивов
EXPORT_BASE_DIR = Path('exports')

# Поддиректории по источникам
EXPORT_PINNACLE_DIR = EXPORT_BASE_DIR / 'pinnacle'   # Данные от Pinnacle
EXPORT_ANALYZER_DIR = EXPORT_BASE_DIR / 'analyzer'   # Данные от анализатора
ARCHIVE_DIR = EXPORT_BASE_DIR / 'archives'           # ZIP-архивы выгрузок

# Создание директорий, если их ещё нет
EXPORT_BASE_DIR.mkdir(parents=True, exist_ok=True)
EXPORT_PINNACLE_DIR.mkdir(parents=True, exist_ok=True)
EXPORT_ANALYZER_DIR.mkdir(parents=True, exist_ok=True)
ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)