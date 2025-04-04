import asyncio
import logging
import sys
import urllib.parse # Добавляем для кодирования URL
import aiohttp  # Добавляем для HTTP запросов к Firebase
import json     # Добавляем для работы с JSON
import html as html_escape  # Добавляем стандартный модуль html для экранирования

from aiogram import Bot, Dispatcher, html
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo, BotCommand, BotCommandScopeDefault

# --- Константы ---
BOT_TOKEN = "7647669248:AAFxNs-rHgTJAbMxhS3-eWECJZ2sd5Rzusw" # Твой токен
# Базовый URL игры БЕЗ параметров
BASE_GAME_URL = "https://upione.github.io/e/"

# --- Firebase Константы ---
# URL берется из файла FirebaseManager.cs -> FirebaseConfig -> databaseURL
# Для REST API к Firebase добавляем ".json" в конце URL запроса
FIREBASE_DB_URL = "https://test-cd618-default-rtdb.europe-west1.firebasedatabase.app"

# --- Настройка логирования и диспетчера ---
logging.basicConfig(level=logging.INFO, stream=sys.stdout, format='%(asctime)s - %(levelname)s - %(message)s')
dp = Dispatcher()

# --- Обработчик команд /start и /play ---
@dp.message(CommandStart())
@dp.message(Command("play"))
async def send_game_button(message: Message):
    """
    Отправляет кнопку для запуска игры с параметрами userId и userName.
    """
    user = message.from_user
    user_id = str(user.id) # Firebase ключи - строки
    user_name = user.full_name
    # Кодируем имя пользователя для безопасной передачи в URL
    encoded_user_name = urllib.parse.quote(user_name)

    logging.info(f"Команда /start или /play от пользователя {user_id} ({user_name})")

    if not BASE_GAME_URL or BASE_GAME_URL == "СЮДА_ВАШ_HTTPS_URL_ИГРЫ_С_GITHUB_PAGES":
         logging.warning("BASE_GAME_URL не установлен!")
         await message.answer("Извините, URL игры не настроен.")
         return

    try:
        # 1. Создаем URL с параметрами
        # ВАЖНО: Убедись, что твой хостинг правильно обрабатывает параметры запроса (?)
        # GitHub Pages и Netlify обычно справляются.
        game_url_with_params = f"{BASE_GAME_URL}?userId={user_id}&userName={encoded_user_name}"
        logging.info(f"Сгенерирован URL для пользователя {user_id}: {game_url_with_params}")

        # 2. Создаем объект WebAppInfo с УНИКАЛЬНЫМ URL
        web_app_info = WebAppInfo(url=game_url_with_params)

        # 3. Создаем кнопки
        play_button = InlineKeyboardButton(
            text="🚀 Запустить Игру!",
            web_app=web_app_info
        )
        
        leaderboard_button = InlineKeyboardButton(
            text="🏆 Таблица лидеров",
            callback_data="show_leaderboard"
        )

        # 4. Создаем клавиатуру с двумя кнопками
        inline_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [play_button],
            [leaderboard_button]
        ])

        # 5. Отправляем сообщение
        await message.answer(
            f"Привет, {html.bold(user_name)}!\n\n"
            "Готов уворачиваться? 😉\n"
            "Нажми кнопку ниже, чтобы начать!",
            reply_markup=inline_keyboard
        )
        logging.info(f"Кнопка для запуска игры отправлена пользователю {user_id}")

    except Exception as e:
        logging.error(f"Ошибка при отправке кнопки игры пользователю {user_id}: {e}", exc_info=True)
        await message.answer("Ой! Что-то пошло не так. Попробуйте позже.")

# --- Обработчик команды /leaderboard ---
@dp.message(Command("leaderboard"))
async def show_leaderboard_command(message: Message):
    """
    Отображает таблицу лидеров по команде /leaderboard
    """
    user_id = str(message.from_user.id)
    await fetch_and_show_leaderboard(message, user_id)

# --- Обработчик команды /help ---
@dp.message(Command("help"))
async def show_help(message: Message):
    """
    Показывает справку по командам бота
    """
    help_text = (
        "🎮 <b>Игровой бот - Справка</b>\n\n"
        "Доступные команды:\n"
        "/play - Запустить игру\n"
        "/leaderboard - Показать таблицу лидеров\n"
        "/profile - Посмотреть свой профиль\n"
        "/help - Показать эту справку\n\n"
        "Удачной игры! 🍀"
    )
    await message.answer(help_text, parse_mode=ParseMode.HTML)

# --- Обработчик команды /profile ---
@dp.message(Command("profile"))
async def show_profile(message: Message):
    """
    Показывает профиль игрока с его статистикой
    """
    user = message.from_user
    user_id = str(user.id)
    user_name = user.full_name
    
    try:
        # Запрашиваем данные игрока из Firebase
        url = f"{FIREBASE_DB_URL}/scores/{user_id}.json"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    await message.answer("Не удалось получить данные вашего профиля. Попробуйте позже.")
                    return
                
                player_data = await response.json()
                
                if not player_data:
                    # Игрок еще не играл
                    profile_text = (
                        f"👤 <b>Профиль игрока</b>\n\n"
                        f"Имя: {html_escape.escape(user_name)}\n"
                        f"ID: {user_id}\n\n"
                        f"Вы еще не играли. Нажмите /play чтобы начать!"
                    )
                else:
                    # Получаем данные и формируем профиль
                    max_score = player_data.get('maxScore', 0)
                    last_update = player_data.get('lastUpdate', 0)
                    
                    # Форматируем дату последней игры, если есть
                    last_played = "Никогда"
                    if last_update:
                        from datetime import datetime
                        date_obj = datetime.fromtimestamp(last_update / 1000)  # конвертируем миллисекунды в секунды
                        last_played = date_obj.strftime('%d.%m.%Y %H:%M')
                    
                    profile_text = (
                        f"👤 <b>Профиль игрока</b>\n\n"
                        f"Имя: {html_escape.escape(player_data.get('name', user_name))}\n"
                        f"ID: {user_id}\n\n"
                        f"📊 <b>Статистика:</b>\n"
                        f"Лучший результат: {max_score} очков\n"
                        f"Последняя игра: {last_played}\n\n"
                        f"Нажмите /play, чтобы начать новую игру!"
                    )
                
                await message.answer(profile_text, parse_mode=ParseMode.HTML)
                
    except Exception as e:
        logging.error(f"Ошибка при получении профиля: {e}", exc_info=True)
        await message.answer("Ой! Не удалось получить данные профиля. Попробуйте позже.")

# --- Обработчик нажатия на кнопку лидерборда ---
@dp.callback_query(lambda c: c.data == "show_leaderboard")
async def show_leaderboard_callback(callback_query):
    """
    Отображает таблицу лидеров при нажатии на кнопку
    """
    user_id = str(callback_query.from_user.id)
    await callback_query.answer()  # Необходимо ответить на callback
    await fetch_and_show_leaderboard(callback_query.message, user_id)

async def fetch_and_show_leaderboard(message, user_id):
    """
    Получает данные лидерборда из Firebase и отображает их
    Оптимизировано для работы с большими таблицами
    """
    try:
        # Проверка адреса Firebase
        if not FIREBASE_DB_URL or FIREBASE_DB_URL == "YOUR_DATABASE_URL":
            logging.error("Firebase URL не настроен!")
            await message.answer("Таблица лидеров временно недоступна. Firebase URL не настроен.")
            return
            
        logging.info(f"Запрос лидерборда для пользователя {user_id}")
        
        async with aiohttp.ClientSession() as session:
            # 1. Получаем только топ-10 игроков с сортировкой по maxScore (убыванию)
            top_players_url = f"{FIREBASE_DB_URL}/scores.json?orderBy=\"maxScore\"&limitToLast=10"
            logging.info(f"Запрос топ-10 игроков: {top_players_url}")
            
            async with session.get(top_players_url) as response:
                if response.status != 200:
                    logging.error(f"Ошибка при запросе к Firebase: {response.status}")
                    await message.answer("Не удалось получить данные таблицы лидеров. Попробуйте позже.")
                    return
                
                top_players_data = await response.json()
                
                if not top_players_data:
                    await message.answer("Таблица лидеров пуста! Будь первым, кто установит рекорд! 🏆")
                    return
                
                # Преобразуем данные в список для сортировки
                top_players = []
                for player_id, player_data in top_players_data.items():
                    if isinstance(player_data, dict) and 'maxScore' in player_data:
                        entry = {
                            'id': player_id,
                            'name': player_data.get('name', 'Unknown'),
                            'maxScore': player_data.get('maxScore', 0)
                        }
                        top_players.append(entry)
                
                # Сортируем по убыванию очков
                top_players.sort(key=lambda x: x['maxScore'], reverse=True)
                
                # 2. Получаем информацию о текущем игроке (если он есть)
                player_info = None
                player_rank = -1
                
                # Проверяем сначала, есть ли игрок среди топ-10
                for i, player in enumerate(top_players):
                    if player['id'] == user_id:
                        player_info = player
                        player_rank = i + 1
                        break
                
                # Если игрока нет в топ-10, получаем его данные отдельным запросом
                if not player_info:
                    player_url = f"{FIREBASE_DB_URL}/scores/{user_id}.json"
                    logging.info(f"Запрос данных игрока: {player_url}")
                    
                    async with session.get(player_url) as player_response:
                        if player_response.status == 200:
                            player_data = await player_response.json()
                            if player_data and 'maxScore' in player_data:
                                player_info = {
                                    'id': user_id,
                                    'name': player_data.get('name', 'Unknown'),
                                    'maxScore': player_data.get('maxScore', 0)
                                }
                                
                                # 3. Определяем ранг игрока
                                # Для этого запрашиваем количество игроков с бо́льшим счетом
                                rank_url = f"{FIREBASE_DB_URL}/scores.json?orderBy=\"maxScore\"&startAt={player_info['maxScore'] + 1}&shallow=true"
                                logging.info(f"Запрос для определения ранга: {rank_url}")
                                
                                async with session.get(rank_url) as rank_response:
                                    if rank_response.status == 200:
                                        better_players_data = await rank_response.json()
                                        # Количество игроков с бо́льшим счетом + 1 = ранг игрока
                                        better_count = len(better_players_data) if better_players_data else 0
                                        player_rank = better_count + 1
            
            # Формируем сообщение
            message_text = "🏆 <b>ТАБЛИЦА ЛИДЕРОВ</b> 🏆\n\n"
            
            # Добавляем топ-10 игроков
            for i, entry in enumerate(top_players):
                rank = i + 1
                name = html_escape.escape(entry.get('name', 'Unknown'))
                score = entry.get('maxScore', 0)
                
                # Выделяем текущего игрока в списке
                if entry.get('id') == user_id:
                    message_text += f"{rank}. 👉 <b>{name}</b>: {score} очков\n"
                else:
                    message_text += f"{rank}. {name}: {score} очков\n"
            
            # Добавляем информацию о текущем игроке, если его нет в топ-10
            if player_info and player_rank > 10:
                message_text += f"\nВаш результат:\n{player_rank}. <b>{html_escape.escape(player_info.get('name', 'Unknown'))}</b>: {player_info.get('maxScore', 0)} очков"
            elif not player_info:
                message_text += "\nУ вас пока нет результатов в таблице. Сыграйте, чтобы получить место в рейтинге!"
            
            # Отправляем сообщение
            await message.answer(message_text, parse_mode=ParseMode.HTML)
                
    except Exception as e:
        logging.error(f"Ошибка при получении лидерборда: {e}", exc_info=True)
        await message.answer("Ой! Не удалось получить данные таблицы лидеров. Попробуйте позже.")

# --- Дополнительная команда для проверки структуры Firebase ---
@dp.message(Command("debug_firebase"))
async def debug_firebase(message: Message):
    """
    Отладочная команда для проверки структуры данных в Firebase
    """
    try:
        url = f"{FIREBASE_DB_URL}.json"
        logging.info(f"Запрос структуры базы: {url}")
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    await message.answer(f"Ошибка при запросе к Firebase: {response.status}")
                    return
                
                data = await response.json()
                
                # Анализируем структуру данных
                result = "📊 <b>Структура базы данных:</b>\n\n"
                
                if not data:
                    result += "База данных пуста или нет доступа."
                else:
                    # Проходим по корневым узлам
                    for key, value in data.items():
                        if isinstance(value, dict):
                            count = len(value)
                            result += f"• <b>{key}</b>: {count} записей\n"
                            
                            # Показываем пример данных из первой записи
                            if count > 0:
                                sample_key = next(iter(value))
                                sample_value = value[sample_key]
                                if isinstance(sample_value, dict):
                                    result += f"  └ Пример ({sample_key}): {json.dumps(sample_value, ensure_ascii=False)[:100]}...\n"
                        else:
                            result += f"• <b>{key}</b>: {type(value).__name__}\n"
                
                await message.answer(result, parse_mode=ParseMode.HTML)
                
    except Exception as e:
        logging.error(f"Ошибка при отладке Firebase: {e}", exc_info=True)
        await message.answer(f"Ошибка: {str(e)}")

# --- Основная функция запуска бота ---
async def main() -> None:
    if BOT_TOKEN == "ВАШ_СУПЕР_СЕКРЕТНЫЙ_БОТ_ТОКЕН": # Заменил на твой, но проверка полезна
        logging.critical("!!! ОШИБКА: Бот-токен не был указан!")
        return

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    
    # Настраиваем команды меню бота
    commands = [
        BotCommand(command="play", description="🎮 Запустить игру"),
        BotCommand(command="leaderboard", description="🏆 Таблица лидеров"),
        BotCommand(command="profile", description="👤 Мой профиль"),
        BotCommand(command="help", description="❓ Помощь")
    ]
    
    await bot.set_my_commands(commands, scope=BotCommandScopeDefault())
    logging.info("Команды меню бота настроены")

    logging.info("Запуск бота...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Бот остановлен вручную.")
        logging.info("Бот остановлен вручную.")
    except Exception as e:
        logging.critical(f"Критическая ошибка при запуске или работе бота: {e}", exc_info=True) 