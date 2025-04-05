import asyncio
import logging
import sys
import urllib.parse
import aiohttp
import json
import html as html_escape
from datetime import datetime
from abc import ABC, abstractmethod # Для абстрактных классов (интерфейса)
from typing import List, Dict, Optional, Any # Для типизации

from aiogram import Bot, Dispatcher, html
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo, BotCommand, BotCommandScopeDefault, CallbackQuery

# --- Конфигурация ---
BOT_TOKEN = "7647669248:AAFxNs-rHgTJAbMxhS3-eWECJZ2sd5Rzusw"
BASE_GAME_URL = "https://upione.github.io/e/"

# --- Настройки Провайдера Данных ---
# Измените это, чтобы выбрать базу данных
DB_PROVIDER_TYPE = "supabase"  # Варианты: "firebase", "supabase"

# --- Настройки для Firebase ---
FIREBASE_DB_URL = "https://test-cd618-default-rtdb.europe-west1.firebasedatabase.app" # Если DB_PROVIDER_TYPE = "firebase"

# --- Настройки для Supabase ---
# Заполните, если DB_PROVIDER_TYPE = "supabase"
SUPABASE_URL = "https://lneycjyyoadboccmapam.supabase.co" # Например: https://xyz.supabase.co
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImxuZXljanl5b2FkYm9jY21hcGFtIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDM4NTgzMDksImV4cCI6MjA1OTQzNDMwOX0.yF_tiTrDszpFmyD-XeUb2YcY9RDWtgr3FNyW6QGk-mI"
SUPABASE_TABLE_NAME = "leaderboard" # Название вашей таблицы в Supabase

# --- Настройки Кеширования и Логирования ---
LEADERBOARD_CACHE_TTL = 15 # Секунды
logging.basicConfig(level=logging.INFO, stream=sys.stdout, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Глобальные переменные ---
http_session: aiohttp.ClientSession = None
db_provider: 'IDatabaseProvider' = None # Экземпляр выбранного провайдера
leaderboard_cache = {
    "data": None,
    "last_updated": 0
}

dp = Dispatcher()

# --- Интерфейс Провайдера Данных (Абстракция) ---

class IDatabaseProvider(ABC):
    """Абстрактный базовый класс для взаимодействия с базой данных."""

    @abstractmethod
    async def initialize(self, session: aiohttp.ClientSession):
        """Инициализация провайдера."""
        pass

    @abstractmethod
    async def close(self):
        """Закрытие соединений или очистка ресурсов (если нужно)."""
        pass

    @abstractmethod
    async def get_top_players(self, limit: int) -> List[Dict[str, Any]]:
        """
        Получает топ N игроков.
        Возвращает список словарей, каждый из которых содержит как минимум:
        {'id': str, 'name': str, 'score': int}
        Список должен быть отсортирован по score по убыванию.
        В случае ошибки должен выбросить исключение или вернуть пустой список.
        """
        pass

    @abstractmethod
    async def get_player_profile(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        Получает профиль игрока по ID.
        Возвращает словарь с данными игрока (включая 'id', 'name', 'score', 'last_played_timestamp')
        или None, если игрок не найден.
        В случае ошибки должен выбросить исключение или вернуть None.
        """
        pass

    @abstractmethod
    async def get_player_rank(self, user_id: str, current_score: int) -> int:
        """
        Получает ранг игрока (1-based).
        Возвращает ранг (int > 0) или -1, если ранг не может быть определен или игрок не найден.
        В случае ошибки должен выбросить исключение или вернуть -1.
        """
        pass

    @abstractmethod
    async def get_debug_info(self) -> str:
        """Возвращает отладочную информацию о базе данных."""
        pass


# --- Реализация Провайдера для Firebase RTDB ---

class FirebaseRTDBProvider(IDatabaseProvider):
    def __init__(self, db_url: str):
        if not db_url or not db_url.startswith("https"):
            raise ValueError("Некорректный Firebase DB URL")
        self._base_url = db_url.rstrip('/')
        self._session: Optional[aiohttp.ClientSession] = None
        logging.info(f"Инициализирован FirebaseRTDBProvider с URL: {self._base_url}")

    async def initialize(self, session: aiohttp.ClientSession):
        self._session = session
        logging.info("FirebaseRTDBProvider: Сессия HTTP установлена.")

    async def close(self):
        logging.info("FirebaseRTDBProvider: Закрытие не требуется.")
        pass # Для HTTP сессии закрытие происходит централизованно

    async def _request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        if not self._session:
            raise RuntimeError("HTTP сессия не инициализирована")
        url = f"{self._base_url}/{endpoint}.json"
        try:
            logging.debug(f"Firebase Запрос: GET {url} | Параметры: {params}")
            async with self._session.get(url, params=params, timeout=10) as response:
                logging.debug(f"Firebase Ответ Статус: {response.status}")
                if response.status == 200:
                    data = await response.json()
                    logging.debug(f"Firebase Ответ Данные: {data}")
                    return data
                elif response.status == 404:
                     logging.warning(f"Firebase: Ресурс не найден (404) по {url}")
                     return None # Или пустой словарь? Зависит от ожиданий
                else:
                    logging.error(f"Ошибка Firebase запроса к {url}: Статус {response.status}, Ответ: {await response.text()}")
                    response.raise_for_status() # Выбросит исключение для других ошибок
                    return None # На всякий случай, если raise_for_status не сработает
        except aiohttp.ClientError as e:
            logging.error(f"Ошибка сети при запросе к Firebase {url}: {e}", exc_info=True)
            raise # Перевыбросить для обработки выше
        except Exception as e:
            logging.error(f"Неожиданная ошибка при запросе к Firebase {url}: {e}", exc_info=True)
            raise

    async def get_top_players(self, limit: int) -> List[Dict[str, Any]]:
        params = {
            "orderBy": '"maxScore"', # Обратите внимание на кавычки внутри строки
            "limitToLast": str(limit) # Firebase ожидает строку для лимита
        }
        data = await self._request("scores", params=params)

        if not data:
            return []

        top_players = []
        for player_id, player_data in data.items():
            if isinstance(player_data, dict) and 'maxScore' in player_data:
                entry = {
                    'id': player_id,
                    'name': player_data.get('name', f'Player {player_id}'), # Имя по умолчанию
                    'score': player_data.get('maxScore', 0)
                    # Доп. поля можно добавить сюда, если нужно
                }
                top_players.append(entry)

        # Firebase limitToLast возвращает в порядке возрастания, сортируем по убыванию
        top_players.sort(key=lambda x: x['score'], reverse=True)
        return top_players

    async def get_player_profile(self, user_id: str) -> Optional[Dict[str, Any]]:
        data = await self._request(f"scores/{user_id}")
        if not data:
            return None

        # Стандартизируем вывод
        profile = {
            'id': user_id,
            'name': data.get('name', f'Player {user_id}'),
            'score': data.get('maxScore', 0),
            'last_played_timestamp': data.get('lastUpdate', 0) # Firebase хранит как lastUpdate
        }
        return profile

    async def get_player_rank(self, user_id: str, current_score: int) -> int:
        if current_score <= 0: # Нет смысла искать ранг для нулевого счета
            return -1

        # Запрос для подсчета игроков с большим счетом
        params = {
            "orderBy": '"maxScore"',
             # Firebase startAt включает указанное значение, поэтому +1
            "startAt": current_score + 1,
             # shallow=true возвращает только ключи, экономя трафик
            "shallow": "true"
        }
        data = await self._request("scores", params=params)

        better_count = len(data) if data else 0
        # Ранг = количество игроков лучше + 1
        return better_count + 1

    async def get_debug_info(self) -> str:
        data = await self._request("") # Запросить корень
        if data:
             # Преобразуем в строку JSON для отображения
             # Используем indent для красивого вывода, но ограничим длину
             try:
                 debug_str = json.dumps(data, indent=2, ensure_ascii=False)
                 return f"Firebase Root Data (ограничено):\n```json\n{debug_str[:1000]}...\n```"
             except Exception as e:
                 logging.error(f"Ошибка сериализации Firebase debug info: {e}")
                 return "Не удалось сериализовать debug info из Firebase."
        else:
            return "Не удалось получить debug info из Firebase."

# --- Реализация Провайдера для Supabase ---

class SupabaseProvider(IDatabaseProvider):
    def __init__(self, db_url: str, anon_key: str, table_name: str):
        if not db_url or not db_url.startswith("https"):
            raise ValueError("Некорректный Supabase URL")
        if not anon_key:
            raise ValueError("Не указан Supabase Anon Key")
        if not table_name:
             raise ValueError("Не указано имя таблицы Supabase")

        self._base_url = db_url.rstrip('/')
        self._anon_key = anon_key
        self._table_name = table_name
        self._session: Optional[aiohttp.ClientSession] = None
        self._headers = {
            'apikey': self._anon_key,
            'Authorization': f'Bearer {self._anon_key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=representation' # Для POST/PATCH, чтобы видеть результат
        }
        logging.info(f"Инициализирован SupabaseProvider с URL: {self._base_url}, Таблица: {self._table_name}")

    async def initialize(self, session: aiohttp.ClientSession):
        self._session = session
        logging.info("SupabaseProvider: Сессия HTTP установлена.")

    async def close(self):
        logging.info("SupabaseProvider: Закрытие не требуется.")
        pass

    async def _request(self, method: str, params: Optional[Dict] = None, data: Optional[Dict] = None) -> Optional[Any]:
        if not self._session:
            raise RuntimeError("HTTP сессия не инициализирована")

        url = f"{self._base_url}/rest/v1/{self._table_name}"
        try:
            logging.debug(f"Supabase Запрос: {method} {url} | Параметры: {params} | Данные: {data}")
            async with self._session.request(method, url, params=params, json=data, headers=self._headers, timeout=15) as response:
                logging.debug(f"Supabase Ответ Статус: {response.status}")
                # Supabase часто возвращает 200 или 204 при успехе, или 201 при создании
                if 200 <= response.status < 300:
                    try:
                        # Может вернуть пустой ответ (204 No Content) или JSON
                        if response.status == 204:
                            return None
                        resp_data = await response.json()
                        logging.debug(f"Supabase Ответ Данные: {resp_data}")
                        return resp_data
                    except aiohttp.ContentTypeError: # Если ответ не JSON
                         logging.warning(f"Supabase: Ответ не JSON при статусе {response.status} от {url}")
                         return None
                    except json.JSONDecodeError:
                         logging.warning(f"Supabase: Не удалось декодировать JSON при статусе {response.status} от {url}")
                         return None
                elif response.status == 404:
                     logging.warning(f"Supabase: Ресурс не найден (404) по {url}")
                     return None
                elif response.status == 406: # Not Acceptable - часто из-за RLS или неверного запроса
                     error_details = await response.text()
                     logging.error(f"Ошибка Supabase (Not Acceptable 406) к {url}: {error_details}")
                     return None # Возвращаем None, чтобы не падать, но сигнализировать об ошибке
                else:
                    error_text = await response.text()
                    logging.error(f"Ошибка Supabase запроса к {url}: Статус {response.status}, Ответ: {error_text}")
                    # Не будем возбуждать исключение для простоты, вернем None
                    # response.raise_for_status()
                    return None
        except aiohttp.ClientError as e:
            logging.error(f"Ошибка сети при запросе к Supabase {url}: {e}", exc_info=True)
            raise
        except Exception as e:
            logging.error(f"Неожиданная ошибка при запросе к Supabase {url}: {e}", exc_info=True)
            raise

    async def get_top_players(self, limit: int) -> List[Dict[str, Any]]:
        params = {
            "select": "user_id,user_name,score", # Запросить нужные поля (замените на ваши имена столбцов!)
            "order": "score.desc",         # Сортировка по полю 'score'
            "limit": str(limit)
        }
        # Supabase возвращает массив
        data = await self._request("GET", params=params)

        if data is None or not isinstance(data, list):
            return []

        # Стандартизируем вывод
        top_players = []
        for item in data:
             # Убедимся, что все поля есть и тип правильный
             if isinstance(item, dict) and 'score' in item and isinstance(item['score'], int):
                 top_players.append({
                     'id': str(item.get('user_id', '')), # Используйте ваши имена столбцов
                     'name': item.get('user_name', 'Unknown'), # Используйте ваши имена столбцов
                     'score': item['score']
                 })
             else:
                 logging.warning(f"Supabase: Пропуск некорректной записи в топ игроках: {item}")


        # Supabase уже сортирует по order=score.desc
        return top_players

    async def get_player_profile(self, user_id: str) -> Optional[Dict[str, Any]]:
        params = {
            "select": "user_id,user_name,score,created_at", # Добавьте нужные поля
            "user_id": f"eq.{user_id}", # Фильтр по user_id (предполагая, что у вас есть такой столбец)
            "limit": "1"
        }
        data = await self._request("GET", params=params)

        if not data or not isinstance(data, list) or len(data) == 0:
            return None

        item = data[0]
        # Преобразуем timestamp Supabase (строка ISO 8601) в timestamp Unix ms для совместимости
        last_played_ts = 0
        created_at_str = item.get('created_at') # Или updated_at, если есть
        if created_at_str:
            try:
                # Парсим строку ISO 8601 с учетом временной зоны
                # Формат может быть '2023-10-27T10:30:00+00:00' или '2023-10-27T10:30:00.123456+00:00'
                # Используем fromisoformat, который гибок
                dt_obj = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                last_played_ts = int(dt_obj.timestamp() * 1000)
            except ValueError:
                logging.warning(f"Supabase: Не удалось распарсить дату: {created_at_str}")


        profile = {
            'id': str(item.get('user_id', user_id)),
            'name': item.get('user_name', f'Player {user_id}'),
            'score': item.get('score', 0),
            'last_played_timestamp': last_played_ts
        }
        return profile

    async def get_player_rank(self, user_id: str, current_score: int) -> int:
        if current_score < 0: # Не имеет смысла для отрицательного счета
             return -1
        # Считаем количество игроков со счетом > current_score
        params = {
            "select": "count", # Запросить только количество
            "score": f"gt.{current_score}" # Фильтр score > current_score
        }
        data = await self._request("GET", params=params)

        # Supabase с select=count возвращает массив с одним объектом {'count': N}
        if data and isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict) and 'count' in data[0]:
            better_count = data[0]['count']
            return better_count + 1
        elif data == []: # Если запрос вернул пустой массив - значит таких игроков 0
             return 1 # Если он единственный с таким или лучшим счетом
        else:
            logging.warning(f"Supabase: Не удалось получить count для ранга user_id={user_id}, score={current_score}. Ответ: {data}")
            return -1 # Ошибка или игрок не найден

    async def get_debug_info(self) -> str:
         # Попробуем получить список таблиц (требует прав, может не сработать с anon ключом)
         # Или просто вернем конфигурацию
        # return f"Supabase Provider\nURL: {self._base_url}\nTable: {self._table_name}\nAnon Key: {self._anon_key[:5]}..."
        # Попробуем получить 1 запись из таблицы
        params = {"select": "*", "limit": "1"}
        data = await self._request("GET", params=params)
        if data and isinstance(data, list):
            try:
                 sample_str = json.dumps(data[0] if data else {}, indent=2, ensure_ascii=False)
                 return f"Supabase Provider (Таблица: {self._table_name})\nПример записи:\n```json\n{sample_str}\n```"
            except Exception as e:
                 logging.error(f"Ошибка сериализации Supabase debug info: {e}")
                 return f"Supabase Provider (Таблица: {self._table_name})\nНе удалось сериализовать пример записи."
        else:
             return f"Supabase Provider (Таблица: {self._table_name})\nНе удалось получить пример записи (проверьте RLS или имя таблицы)."


# --- Функции бота (теперь используют db_provider) ---

async def update_leaderboard_cache_task():
    """Фоновая задача для обновления кеша лидерборда через db_provider."""
    global leaderboard_cache, db_provider
    await asyncio.sleep(5) # Небольшая задержка на старте
    while True:
        try:
            if not db_provider:
                logging.warning("db_provider еще не инициализирован, пропуск обновления кеша.")
                await asyncio.sleep(LEADERBOARD_CACHE_TTL)
                continue

            logging.info("Обновление кеша лидерборда...")
            # Запрашиваем топ-10 через провайдер
            top_players_list = await db_provider.get_top_players(limit=10)

            # Кешируем результат (список словарей)
            leaderboard_cache["data"] = top_players_list # Теперь храним список
            leaderboard_cache["last_updated"] = asyncio.get_event_loop().time()
            logging.info(f"Кеш лидерборда успешно обновлён ({len(top_players_list)} записей)")

        except Exception as e:
            logging.error(f"Ошибка обновления кеша лидерборда: {e}", exc_info=True)
            # Можно очистить кеш при ошибке
            # leaderboard_cache["data"] = None

        await asyncio.sleep(LEADERBOARD_CACHE_TTL)


# --- Обработчики команд ---

@dp.message(CommandStart())
@dp.message(Command("play"))
async def send_game_button(message: Message):
    user = message.from_user
    user_id = str(user.id)
    user_name = user.full_name
    encoded_user_name = urllib.parse.quote(user_name)

    logging.info(f"Команда /start или /play от пользователя {user_id} ({user_name})")

    if not BASE_GAME_URL:
         logging.warning("BASE_GAME_URL не установлен!")
         await message.answer("Извините, URL игры не настроен.")
         return

    try:
        game_url_with_params = f"{BASE_GAME_URL}?userId={user_id}&userName={encoded_user_name}"
        logging.info(f"Сгенерирован URL для пользователя {user_id}: {game_url_with_params}")

        web_app_info = WebAppInfo(url=game_url_with_params)
        play_button = InlineKeyboardButton(text="🚀 Запустить Игру!", web_app=web_app_info)
        leaderboard_button = InlineKeyboardButton(text="🏆 Таблица лидеров", callback_data="show_leaderboard")
        inline_keyboard = InlineKeyboardMarkup(inline_keyboard=[[play_button], [leaderboard_button]])

        await message.answer(
            f"Привет, {html.bold(html.quote(user_name))}!\n\n"
            "Готов уворачиваться? 😉\n"
            "Нажми кнопку ниже, чтобы начать!",
            reply_markup=inline_keyboard
        )
        logging.info(f"Кнопка для запуска игры отправлена пользователю {user_id}")
    except Exception as e:
        logging.error(f"Ошибка при отправке кнопки игры пользователю {user_id}: {e}", exc_info=True)
        await message.answer("Ой! Что-то пошло не так. Попробуйте позже.")

@dp.message(Command("leaderboard"))
async def show_leaderboard_command(message: Message):
    user_id = str(message.from_user.id)
    await fetch_and_show_leaderboard(message, user_id)

@dp.message(Command("help"))
async def show_help(message: Message):
    help_text = (
        "🎮 <b>Игровой бот - Справка</b>\n\n"
        "Доступные команды:\n"
        "/play - Запустить игру\n"
        "/leaderboard - Показать таблицу лидеров\n"
        "/profile - Посмотреть свой профиль\n"
        "/help - Показать эту справку\n"
        # "/debug_db - Отладочная информация о БД\n" # Раскомментировать если нужна команда
        "\nУдачной игры! 🍀"
    )
    await message.answer(help_text)


@dp.message(Command("profile"))
async def show_profile(message: Message):
    user = message.from_user
    user_id = str(user.id)
    user_name_from_tg = user.full_name # Имя из телеграма

    if not db_provider:
        await message.answer("Ошибка: Провайдер базы данных не инициализирован.")
        return

    try:
        # Получаем профиль через провайдер
        player_profile = await db_provider.get_player_profile(user_id)

        if not player_profile:
            profile_text = (
                f"👤 <b>Профиль игрока</b>\n\n"
                f"Имя: {html.quote(user_name_from_tg)}\n"
                f"ID: {user_id}\n\n"
                f"Вы еще не играли или ваш профиль не найден. Нажмите /play чтобы начать!"
            )
        else:
            # Используем данные из профиля БД
            profile_name = html.quote(player_profile.get('name', user_name_from_tg))
            max_score = player_profile.get('score', 0)
            last_update_ts = player_profile.get('last_played_timestamp', 0)

            last_played = "Никогда"
            if last_update_ts and last_update_ts > 0:
                try:
                    # Преобразуем timestamp ms в datetime
                    date_obj = datetime.fromtimestamp(last_update_ts / 1000)
                    last_played = date_obj.strftime('%d.%m.%Y %H:%M')
                except Exception as e:
                     logging.warning(f"Не удалось распарсить timestamp {last_update_ts}: {e}")
                     last_played = "Неизвестно"


            profile_text = (
                f"👤 <b>Профиль игрока</b>\n\n"
                f"Имя: {profile_name}\n"
                f"ID: {user_id}\n\n"
                f"📊 <b>Статистика:</b>\n"
                f"Лучший результат: {max_score} очков\n"
                f"Последняя игра: {last_played}\n\n"
                f"Нажмите /play, чтобы начать новую игру!"
            )
        await message.answer(profile_text)

    except Exception as e:
        logging.error(f"Ошибка при получении профиля user_id={user_id}: {e}", exc_info=True)
        await message.answer("Ой! Не удалось получить данные профиля. Попробуйте позже.")


@dp.callback_query(lambda c: c.data == "show_leaderboard")
async def show_leaderboard_callback(callback_query: CallbackQuery):
    user_id = str(callback_query.from_user.id)
    # Отвечаем на колбэк, чтобы убрать "часики" на кнопке
    await callback_query.answer()
    # Отправляем лидерборд как новое сообщение или редактируем старое
    # В данном случае проще отправить новое
    await fetch_and_show_leaderboard(callback_query.message, user_id)


async def fetch_and_show_leaderboard(message: Message, user_id: str):
    """Получает данные (из кеша или БД через провайдер) и отображает лидерборд."""
    global leaderboard_cache, db_provider

    if not db_provider:
        await message.answer("Ошибка: Провайдер базы данных не инициализирован.")
        return

    try:
        top_players = None
        # Проверяем кеш
        current_time = asyncio.get_event_loop().time()
        if leaderboard_cache["data"] is not None and (current_time - leaderboard_cache["last_updated"]) < LEADERBOARD_CACHE_TTL:
            top_players = leaderboard_cache["data"]
            logging.info("Используем кеш лидерборда")
        else:
            logging.info("Кеш лидерборда устарел или пуст, запрашиваем из БД...")
            top_players = await db_provider.get_top_players(limit=10)
            # Обновляем кеш, даже если пришла ошибка и список пуст
            leaderboard_cache["data"] = top_players
            leaderboard_cache["last_updated"] = current_time

        if top_players is None: # Если провайдер вернул None из-за ошибки
             await message.answer("Не удалось получить данные таблицы лидеров (провайдер вернул ошибку). Попробуйте позже.")
             return
        if not top_players: # Если список пуст
            await message.answer("Таблица лидеров пуста! Будь первым, кто установит рекорд! 🏆")
            return

        # top_players уже должен быть отсортированным списком словарей
        # {'id': str, 'name': str, 'score': int}

        # Ищем текущего игрока в топ-10
        player_in_top = None
        player_rank_in_top = -1
        for i, player in enumerate(top_players):
            if player.get('id') == user_id:
                player_in_top = player
                player_rank_in_top = i + 1
                break

        player_info_full = None
        player_rank = -1

        if player_in_top:
            player_info_full = player_in_top
            player_rank = player_rank_in_top
        else:
            # Если игрока нет в топе, получаем его профиль и ранг отдельно
            logging.info(f"Игрок {user_id} не в топ-{len(top_players)}, получаем профиль и ранг...")
            player_info_full = await db_provider.get_player_profile(user_id)
            if player_info_full and player_info_full.get('score', 0) > 0:
                 # Получаем ранг, передавая текущий счет
                 player_rank = await db_provider.get_player_rank(user_id, player_info_full['score'])
                 logging.info(f"Рассчитанный ранг для {user_id}: {player_rank}")
            else:
                 logging.info(f"Профиль для {user_id} не найден или счет 0, ранг не определяется.")


        # Формирование текста таблицы лидеров
        message_text = "🏆 <b>ТАБЛИЦА ЛИДЕРОВ</b> 🏆\n\n"
        for i, entry in enumerate(top_players):
            rank_num = i + 1
            name = html.quote(entry.get('name', 'Unknown'))
            score = entry.get('score', 0)
            is_current_user = entry.get('id') == user_id

            prefix = f"{rank_num}. "
            if is_current_user:
                prefix += "👉 " # Указатель на текущего пользователя

            line = f"{prefix}{html.bold(name) if is_current_user else name}: {score} очков\n"
            message_text += line

        # Добавляем информацию о текущем пользователе, если он не в топе
        if player_info_full and player_rank > len(top_players):
            name = html.quote(player_info_full.get('name', 'Вы'))
            score = player_info_full.get('score', 0)
            message_text += f"\n...\nВаш результат:\n{player_rank}. 👉 <b>{name}</b>: {score} очков"
        elif not player_info_full and not player_in_top: # Если профиль вообще не найден
            message_text += "\nУ вас пока нет результатов в таблице. Сыграйте, чтобы получить место в рейтинге!"

        # Используем edit_message_text если сообщение пришло из callback-кнопки, иначе answer
        # Но проще всегда использовать answer для надежности
        await message.answer(message_text)

    except Exception as e:
        logging.error(f"Ошибка при получении/отображении лидерборда для user_id={user_id}: {e}", exc_info=True)
        await message.answer("Ой! Не удалось получить данные таблицы лидеров. Попробуйте позже.")


# --- Отладочная команда ---
@dp.message(Command("debug_db"))
async def debug_db_command(message: Message):
    if not db_provider:
        await message.answer("Ошибка: Провайдер базы данных не инициализирован.")
        return
    try:
        debug_info = await db_provider.get_debug_info()
        await message.answer(f"🔧 <b>Debug Info ({db_provider.__class__.__name__})</b>:\n\n{debug_info}",
                             parse_mode=ParseMode.HTML) # Используем HTML парсер, если debug_info содержит разметку
    except Exception as e:
        logging.error(f"Ошибка при выполнении debug_db: {e}", exc_info=True)
        await message.answer(f"Ошибка при получении debug info: {str(e)}")


# --- Основная функция запуска бота ---
async def main() -> None:
    global http_session, db_provider

    # --- Выбор и инициализация Провайдера Данных ---
    try:
        if DB_PROVIDER_TYPE == "firebase":
            if not FIREBASE_DB_URL: raise ValueError("FIREBASE_DB_URL не установлен")
            db_provider = FirebaseRTDBProvider(FIREBASE_DB_URL)
        elif DB_PROVIDER_TYPE == "supabase":
            if not SUPABASE_URL: raise ValueError("SUPABASE_URL не установлен")
            if not SUPABASE_ANON_KEY: raise ValueError("SUPABASE_ANON_KEY не установлен")
            db_provider = SupabaseProvider(SUPABASE_URL, SUPABASE_ANON_KEY, SUPABASE_TABLE_NAME)
        else:
            raise ValueError(f"Неизвестный DB_PROVIDER_TYPE: {DB_PROVIDER_TYPE}")

        logging.info(f"Выбран провайдер данных: {db_provider.__class__.__name__}")

    except ValueError as e:
        logging.critical(f"Ошибка конфигурации провайдера данных: {e}")
        return # Завершаем работу, если конфиг некорректен


    # Установка uvloop (опционально, для производительности)
    try:
        import uvloop
        uvloop.install()
        logging.info("uvloop установлен и используется.")
    except ImportError:
        logging.warning("uvloop не найден. Используется стандартный asyncio loop.")

    if not BOT_TOKEN or BOT_TOKEN == "ВАШ_СУПЕР_СЕКРЕТНЫЙ_БОТ_ТОКЕН":
        logging.critical("!!! ОШИБКА: Бот-токен не указан!")
        return

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    commands = [
        BotCommand(command="play", description="🎮 Запустить игру"),
        BotCommand(command="leaderboard", description="🏆 Таблица лидеров"),
        BotCommand(command="profile", description="👤 Мой профиль"),
        BotCommand(command="help", description="❓ Помощь"),
        BotCommand(command="debug_db", description="🔧 Отладка БД")
    ]
    try:
        await bot.set_my_commands(commands, scope=BotCommandScopeDefault())
        logging.info("Команды меню бота настроены")
    except Exception as e:
        logging.error(f"Не удалось установить команды бота: {e}")


    # Создаём глобальную сессию для HTTP запросов
    async with aiohttp.ClientSession() as session:
        http_session = session # Сохраняем для глобального доступа (хотя лучше передавать явно)

        # Инициализируем выбранный провайдер данных с сессией
        try:
            await db_provider.initialize(session)
        except Exception as e:
             logging.critical(f"Не удалось инициализировать провайдер данных: {e}", exc_info=True)
             return # Не запускаем бота, если провайдер не инициализирован

        # Запускаем фоновую задачу для обновления кеша лидерборда
        # Важно запускать после инициализации db_provider
        leaderboard_task = asyncio.create_task(update_leaderboard_cache_task())

        logging.info("Запуск бота...")
        try:
            await dp.start_polling(bot)
        finally:
            logging.info("Остановка бота...")
            leaderboard_task.cancel()
            try:
                await leaderboard_task # Дождаться завершения задачи (или отмены)
            except asyncio.CancelledError:
                logging.info("Задача обновления кеша успешно отменена.")
            if db_provider:
                 await db_provider.close() # Даем шанс провайдеру на очистку
            # Сессия http_session закроется автоматически благодаря 'async with'

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Бот остановлен вручную (KeyboardInterrupt).")
    except Exception as e:
        logging.critical(f"Критическая ошибка в main loop: {e}", exc_info=True)