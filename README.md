# 📡 Pinnacle Streamer

> 🛠️ Этот проект изначально разрабатывался как часть коммерческого заказа.  
> Однако после завершения и передачи исходников, заказчик отказался от оплаты  
> и прервал связь. Проект опубликован в открытый доступ как часть портфолио  
> и демонстрация архитектурного подхода к live-обработке данных.

---

**Pinnacle Streamer** — это микросервис для сбора и агрегации live-коэффициентов из WebSocket-источников (Pinnacle, Analyzer), с последующей выгрузкой в CSV, архивированием и загрузкой в облако.

Он реализует полный цикл:
- Подключение к источникам в реальном времени
- Буферизация и сохранение в PostgreSQL
- Структурированный экспорт матчей
- Архивация и загрузка на Mega.nz

---

## 📁 Структура проекта
```
pinnacle_streamer/ 
│ ├── app/ # Основная логика микросервиса 
│ ├── constants/ # Константы путей, настроек, шаблонов CSV 
│ ├── aggregator.py # Буферизация входящих сообщений 
│ ├── archiver.py # Архивация старых CSV-файлов 
│ ├── collector_analyzer.py # Выгрузка данных анализатора 
│ ├── collector_pinnacle.py # Выгрузка данных Pinnacle 
│ ├── config.py # Загрузка конфигурации из .env 
│ ├── db.py # Подключение к базе данных 
│ ├── models.py # SQLAlchemy модели 
│ ├── uploader_to_mega.py # Загрузка архивов на Mega 
│ ├── utils.py # Утилиты (хэши, парсинг дат, логирование) 
│ ├── websocket_client.py # WebSocket-клиент 
│ ├── writer_analyzer.py # Парсинг и запись данных анализатора 
│ └── writer_pinnacle.py # Парсинг и запись данных Pinnacle 
│ ├── scripts/ 
│ └── run_pinnacle_streamer.py # Точка входа 
│ ├── exports/ # Папка для выгрузок 
│ ├── analyzer/ # CSV-файлы анализатора 
│ ├── pinnacle/ # CSV-файлы Pinnacle 
│ └── archives/ # Архивы .zip 
│ ├── alembic/ # Миграции базы данных 
├── .env # Конфигурация окружения (не в репозитории) 
├── Dockerfile # Образ для запуска микросервиса 
├── docker-compose.yml # Конфигурация docker-compose 
├── requirements.txt # Зависимости проекта 
└── README.md # Документация (этот файл)
```
---

## 🐳 Запуск проекта через Docker

### 🔧 1. Собери контейнеры
```bash
   docker compose up --build  
```

### 🚀 2. Запусти сервис
```bash
   docker compose run --rm app python -m scripts.run_pinnacle_streamer
```
Контейнер запустит WebSocket-клиент, сбор данных, экспорт CSV и загрузку архивов на Mega.

## 🔐 Переменные окружения
Создай файл .env в корне и добавь в него:

```
WS_PINNACLE_URL=ws://...
WS_ANALYZER_URL=ws://...
FILTER_NAME=main
SPORTS=Soccer,Tennis
DATABASE_URL=postgresql+asyncpg://user:pass@host:port/dbname
MEGA_EMAIL=your_email@example.com
MEGA_PASSWORD=your_password
```

🧠 Примечания
Все входящие сообщения буферизуются и группируются по времени перед записью.

Коэффициенты записываются построчно с key_hash, чтобы избежать дублирования.

Экспорт по шаблону CSV_PINNACLE_COLUMNS или CSV_ANALYZER_COLUMNS.

Архивы создаются раз в 2 часа и заливаются на Mega.


## 📄 Disclaimer

В рамках проекта данные поступали через WebSocket-интерфейс от сервера заказчика,  
который самостоятельно реализовал парсинг и сбор данных из внешних источников.  
Автор микросервиса не подключался напрямую к сторонним API или сервисам и  
не несёт ответственности за способ получения данных заказчиком.

Данный проект публикуется исключительно как демонстрация архитектуры  
live-аналитики и потоковой обработки данных.
