from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    Конфигурация приложения, загружается из файла `.env`.

    Параметры:
    - ws_pinnacle_url: WebSocket-адрес для получения данных от Pinnacle
    - ws_analyzer_url: WebSocket-адрес для получения данных от анализатора
    - filter_name: Имя фильтра для отправки при подключении к WebSocket
    - sports: Список видов спорта для обработки
    - database_url: URL подключения к базе данных
    - mega_email: Email для входа в облачное хранилище Mega
    - mega_password: Пароль для Mega

    Примечание: чувствительность к регистру отключена, лишние переменные игнорируются.
    """

    ws_pinnacle_url: str
    ws_analyzer_url: str
    filter_name: str
    sports: list[str]
    database_url: str
    mega_email: str
    mega_password: str

    class Config:
        env_file = str(Path(__file__).resolve().parent.parent / '.env')
        env_file_encoding = 'utf-8'
        extra = 'ignore'
        case_sensitive = False


settings = Settings()
