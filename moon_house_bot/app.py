import logging
from datetime import date, datetime, timedelta
from distutils.util import strtobool

from aiogram import types, Bot, Dispatcher
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import Text
from aiogram.dispatcher.filters.state import StatesGroup, State
from aiogram.utils.callback_data import CallbackData
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pony.orm import db_session, commit, desc, count, select, rollback

from aiogramcalendar import create_calendar, calendar_callback, process_calendar_selection
from config import settings
from moon_house_bot.database.models import Users, Parties, Notifications

bot = Bot(token=settings.token)
dp = Dispatcher(bot, storage=MemoryStorage())
scheduler = AsyncIOScheduler()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def chat_checker(login_required: bool = True):
    def decorator(func):
        @db_session
        async def wrapper(message):
            if message.chat.type == 'private' or message.chat.id == settings.target_chat_id:
                user = Users.select(lambda u: u.chat_id == message.from_user.id and u.deleted is None).first()
                if not login_required:
                    return await func(message, user)
                elif user and message.chat.type == 'private':
                    return await func(message)
        return wrapper
    return decorator


class PlanParty(StatesGroup):
    party_date = State()
    guests_amount = State()
    using_sofa = State()


class EditPartyDate(StatesGroup):
    edit_party_date = State()


class EditPartyGuestsAmount(StatesGroup):
    edit_party_guests_amount = State()


sofa_dict = {True: 'да', False: 'нет'}
new_user_data = CallbackData('new_user', 'accept', 'id')
party_manage_data = CallbackData('party_manage', 'id', 'using_sofa')
party_edit_data = CallbackData('party_edit', 'edit_type', 'id')
sofa_using_edit_data = CallbackData('sofa_using_edit', 'using', 'id')


@dp.message_handler(commands=['start', 'home'])
@chat_checker(login_required=False)
async def main_menu_handler(message: types.Message, user: Users):
    if message.chat.type == 'group':
        message_for_all_users = 'Используй команды /home или /start в личной переписке со мной, ' \
                                'чтобы при совершении каких-то действий не захламлять чат сообщениями. ' \
                                'А все важные оповещения будут показываться здесь для всех.'
        if not user:
            deleted_user = Users.select(lambda u: u.chat_id == message.from_user.id and u.deleted is not None).first()
            message_for_deleted = 'снова ' if deleted_user else ''
            if deleted_user:
                deleted_user.set(deleted=None)
                commit()
            else:
                Users(
                    chat_id=message.from_user.id,
                    firstname=message.from_user.first_name,
                    lastname=message.from_user.last_name,
                    nickname=message.from_user.username if message.from_user.username else None
                )
                commit()
            return await message.answer(f'Привет, {message.from_user.full_name}! '
                                        f'Теперь ты {message_for_deleted}часть бытовухи квартиры на улице Радио!\n'
                                        f'{message_for_all_users}')
        return await message.answer(message_for_all_users)
    if user:
        keyboard_markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        all_buttons = ('Посудомойка 🍴', 'Выкинуть мусор 🗑', 'Тусовки 🍻', 'Статистика 📊')
        keyboard_markup.add(*all_buttons)
        return await message.reply('Что делаем?', reply_markup=keyboard_markup)

    keyboard_markup = types.InlineKeyboardMarkup()
    new_user_buttons = [
        types.InlineKeyboardButton('Принимаем ✅', callback_data=new_user_data.new(
            accept=True, id=message.from_user.id
        )),
        types.InlineKeyboardButton('Отклоняем ❌', callback_data=new_user_data.new(
            accept=False, id='-'
        )),
    ]
    keyboard_markup.add(*new_user_buttons)
    admins = Users.select(lambda u: u.is_admin is True and u.deleted is None)
    nickname = f'@{message.from_user.username.lower()} ' if message.from_user.username else ''
    for admin in admins:
        await bot.send_message(
            admin.chat_id,
            f'{message.from_user.full_name} {nickname}хочет присоединиться '
            f'к бытовухе на Радио. Что будем делать?',
            reply_markup=keyboard_markup
        )


@dp.callback_query_handler(new_user_data.filter())
@db_session
async def resolve_new_user(call: types.CallbackQuery, callback_data: dict):
    accepted = strtobool(callback_data['accept'])
    message_words_list = call.message.text.split(maxsplit=3)

    message_addon = '' if accepted else 'не '
    message_for_new_user = f'Ты {message_addon}принят в бытовуху на Радио'
    await bot.send_message(callback_data['id'], message_for_new_user)

    if accepted:
        deleted_user = Users.select(lambda u: u.chat_id == callback_data['id'] and u.deleted is not None).first()
        if deleted_user:
            deleted_user.set(deleted=None)
            commit()
        else:
            Users(
                chat_id=callback_data['id'],
                firstname=message_words_list[0],
                lastname=message_words_list[1],
                nickname=message_words_list[2].lstrip('@') if message_words_list[2].startswith('@') else None
            )
            commit()

        await call.message.answer(f'Ты принял {message_words_list[0]} {message_words_list[1]} в бытовуху на Радио')
        return await call.message.delete_reply_markup()

    await call.message.answer(f'Ты отклонил заявку {message_words_list[0]} {message_words_list[1]}')
    await call.message.delete_reply_markup()


async def check_user_honesty(message: types.Message, notification_type: str):
    today_trash_notifications = Notifications.select(
        lambda n: n.notification_type == notification_type and n.created.date() == date.today() and n.deleted is None
    ).count()
    if today_trash_notifications >= 3:
        notification_type_triple = {
            'trash': 'Сегодня мусор уже трижды выбрасывали',
            'dishwasher_load': 'Сегодня посудомойку уже трижды загружали',
            'dishwasher_unload': 'Сегодня посудомойку уже трижды разгружали'
        }
        dishonest_message = 'Кажется, ты просто пытаешься себе поднять рейтинг'
        await message.answer(f'{notification_type_triple.get(notification_type)}. {dishonest_message}')
        return False
    return True


async def send_dishwasher_unload_notify():
    await bot.send_message(settings.target_chat_id, 'Посудомойку можно разгружать!')


@dp.message_handler(Text(equals='Посудомойка 🍴'))
@chat_checker()
async def dishwasher_handler(message: types.Message):
    keyboard_markup = types.InlineKeyboardMarkup()
    dishwasher_buttons = [
        types.InlineKeyboardButton('Загрузить ⬇', callback_data='dishwasher_load'),
        types.InlineKeyboardButton('Разгрузить ⬆', callback_data='dishwasher_unload'),
    ]
    keyboard_markup.add(*dishwasher_buttons)
    await message.answer('Что сделать с посудомойкой?', reply_markup=keyboard_markup)


@dp.callback_query_handler(Text(startswith='dishwasher'))
@db_session
async def dishwasher_callback(call: types.CallbackQuery):
    honesty = await check_user_honesty(call.message, call.data)

    if honesty:
        action_prefixes = {
            True: 'раз',
            False: 'за'
        }
        unload = call.data.endswith('unload')
        dishwasher_last_notification = Notifications.select(
            lambda n: n.notification_type in ('dishwasher_load', 'dishwasher_unload') and n.deleted is None
        ).order_by(desc(Notifications.created)).first()
        if (not dishwasher_last_notification and unload) or \
                (dishwasher_last_notification and dishwasher_last_notification.notification_type == call.data):
            last_time_loaded = f'\nПоследний раз {action_prefixes.get(unload)}гружалась: ' \
                               f'{dishwasher_last_notification.created.strftime("%d.%m.%y в %H:%M")}' \
                if dishwasher_last_notification else ''
            return await call.message.answer(
                f'Прежде чем {action_prefixes.get(unload)}грузить посудомойку, '
                f'ее надо {action_prefixes.get(not unload)}грузить{last_time_loaded}'
            )
        now = datetime.now()
        dishwasher_working_minutes = timedelta(minutes=2)
        if unload:
            if dishwasher_last_notification.created + dishwasher_working_minutes > now:
                return await call.message.answer('Посудомойка еще моет, нельзя разгрузить')

        else:
            datetime_unload = now + dishwasher_working_minutes
            scheduler.add_job(send_dishwasher_unload_notify, 'date', run_date=datetime_unload)
        Notifications(
            user=call.from_user.id,
            notification_type=call.data
        )
        commit()

        await bot.send_message(
            settings.target_chat_id,
            f'{call.from_user.full_name} {action_prefixes.get(unload)}грузил(а) посудомойку'
        )
        await call.message.delete_reply_markup()


@dp.message_handler(Text(equals='Выкинуть мусор 🗑'))
@chat_checker()
async def trash_handler(message: types.Message):
    honesty = await check_user_honesty(message, 'trash')
    if honesty:
        Notifications(
            user=message.from_user.id,
            notification_type='trash'
        )
        commit()

        trash_message = 'выкинул(а) мусор'
        await bot.send_message(settings.target_chat_id, f'{message.from_user.full_name} {trash_message}')


@dp.message_handler(Text(equals='Тусовки 🍻'))
@chat_checker()
async def parties_handler(message: types.Message):
    keyboard_markup = types.InlineKeyboardMarkup(row_width=1)
    parties_buttons = [
        types.InlineKeyboardButton('Забронировать новую 🎉', callback_data='party_new'),
        types.InlineKeyboardButton('Посмотреть ближайшие 📆', callback_data='party_closest'),
        types.InlineKeyboardButton('Управлять 🏄‍', callback_data='party_manage'),
    ]
    keyboard_markup.add(*parties_buttons)
    await message.answer('Что по тусовкам?', reply_markup=keyboard_markup)


@dp.callback_query_handler(Text(equals='party_new'), state='*')
async def plan_party_date(call: types.CallbackQuery):
    await call.message.answer('Выбери дату', reply_markup=create_calendar())
    await call.message.delete_reply_markup()
    await PlanParty.party_date.set()


@dp.callback_query_handler(calendar_callback.filter(), state=PlanParty.party_date)
@db_session
async def choose_party_date(call: types.CallbackQuery, callback_data: dict, state: FSMContext):
    selected_date = await process_calendar_selection(call, callback_data)
    if selected_date:
        party = Parties.select(lambda p: p.party_date == selected_date and p.deleted is None).first()
        if party:
            return await call.message.reply(
                f'Выбери другую дату. На {selected_date.strftime("%d.%m.%y")} уже забронирована тусовка',
                reply_markup=create_calendar()
            )
        await state.update_data(party_date=selected_date)
        await call.message.answer(f'Выбрано {selected_date.strftime("%d.%m.%y")}')
        await call.message.reply('Введи количество гостей')
        await PlanParty.guests_amount.set()


async def validate_guests_amount(message: types.Message, amount_data: str):
    try:
        guests_amount = int(amount_data)
    except ValueError:
        return False, await message.reply('Ага, шутка классная, а теперь введи количество гостей в виде числа')
    if guests_amount < 1:
        return False, await message.reply(
            'Ого, тусовка обещает быть очень веселой. '
            'Но лучше все же ввести хотя бы одного гостя'
        )
    elif guests_amount > 50:
        return False, await message.reply('Кажется, столько гостей квартира на Радио не потянет')
    return True, guests_amount


@dp.message_handler(state=PlanParty.guests_amount)
async def plan_party_using_sofa(message: types.Message, state: FSMContext):
    validated, guests_amount = await validate_guests_amount(message, message.text)

    if validated:
        await state.update_data(guests_amount=guests_amount)
        keyboard_markup = types.InlineKeyboardMarkup()
        sofa_using_buttons = [
            types.InlineKeyboardButton('Да', callback_data='sofa_using_yes'),
            types.InlineKeyboardButton('Нет', callback_data='sofa_using_no'),
        ]
        keyboard_markup.add(*sofa_using_buttons)
        await message.reply('Будет ли использоваться диван?', reply_markup=keyboard_markup)
        await PlanParty.using_sofa.set()


@dp.callback_query_handler(Text(startswith='sofa_using'), state=PlanParty.using_sofa)
@db_session
async def plan_party_save(call: types.CallbackQuery, state: FSMContext):
    using_sofa = call.data.endswith('yes')
    await state.update_data(using_sofa=using_sofa)
    user_data = await state.get_data()
    Parties(user=call.from_user.id, **user_data)
    commit()
    await state.finish()
    await bot.send_message(
        settings.target_chat_id,
        f"{call.from_user.full_name} забронировал(а) тусовку {user_data.get('party_date').strftime('%d.%m.%y')}\n"
        f"Количество гостей: {user_data.get('guests_amount')}\n"
        f"Диван будет занят: {sofa_dict.get(user_data.get('using_sofa'))}"
    )
    await call.message.delete_reply_markup()


@dp.callback_query_handler(Text(startswith='party_closest'))
@db_session
async def show_closest_parties(call: types.CallbackQuery):
    parties = Parties.select(
        lambda p: p.party_date >= date.today() and p.deleted is None
    ).order_by(lambda p: p.party_date)[:3]
    if parties:
        parties_list = [
            f'{p.party_date}, людей: {p.guests_amount}, диван будет занят: {sofa_dict.get(p.using_sofa)}'
            for p in parties
        ]
        parties_answer = '\n'.join(parties_list)
    else:
        parties_answer = 'На ближайшее время никаких тусовок не запланировано'
    await call.message.answer(parties_answer)
    await call.message.delete_reply_markup()


@dp.callback_query_handler(Text(equals='party_manage'))
@db_session
async def show_user_parties(call: types.CallbackQuery):
    user_parties = Parties.select(
        lambda p: p.user.chat_id == call.from_user.id and p.party_date >= date.today() and p.deleted is None
    ).order_by(lambda p: p.party_date)
    if user_parties:
        keyboard_markup = types.InlineKeyboardMarkup(row_width=1)
        your_parties_buttons = [
            types.InlineKeyboardButton(
                f'{p.party_date.strftime("%d.%m.%y")}, '
                f'гостей: {p.guests_amount}, '
                f'занятость дивана: {sofa_dict.get(p.using_sofa)}',
                callback_data=party_manage_data.new(id=p.id, using_sofa=p.using_sofa)
            ) for p in user_parties
        ]
        keyboard_markup.add(*your_parties_buttons)
        await call.message.answer('Выбери тусовку', reply_markup=keyboard_markup)
    else:
        await call.message.answer('У тебя нет ни одной запланированной тусовки')
    await call.message.delete_reply_markup()


@dp.callback_query_handler(party_manage_data.filter())
@db_session
async def user_party_manage(call: types.CallbackQuery, callback_data: dict):
    change_using_sofa = not strtobool(callback_data["using_sofa"])
    keyboard_markup = types.InlineKeyboardMarkup(row_width=1)
    your_parties_buttons = [
        types.InlineKeyboardButton('Редактировать дату тусовки',
                                   callback_data=party_edit_data.new(
                                       edit_type='party_date', id=callback_data['id'])
                                   ),
        types.InlineKeyboardButton('Редактировать количество гостей',
                                   callback_data=party_edit_data.new(
                                       edit_type='guests_amount', id=callback_data['id'])
                                   ),
        types.InlineKeyboardButton(f'Изменить занятость дивана на «{sofa_dict.get(change_using_sofa)}»',
                                   callback_data=party_edit_data.new(
                                       edit_type='sofa_using', id=callback_data['id'])
                                   ),
        types.InlineKeyboardButton('Удалить тусовку',
                                   callback_data=party_edit_data.new(
                                       edit_type='delete_party', id=callback_data['id'])
                                   ),
    ]
    keyboard_markup.add(*your_parties_buttons)
    await call.message.answer('Выбери действие', reply_markup=keyboard_markup)
    await call.message.delete_reply_markup()


@dp.callback_query_handler(party_edit_data.filter(edit_type='party_date'), state='*')
async def edit_party_date(call: types.CallbackQuery, callback_data: dict, state: FSMContext):
    await state.update_data(party_id=callback_data['id'])
    await call.message.answer('Выбери новую дату', reply_markup=create_calendar())
    await call.message.delete_reply_markup()
    await EditPartyDate.edit_party_date.set()


@dp.callback_query_handler(calendar_callback.filter(), state=EditPartyDate.edit_party_date)
@db_session
async def choose_new_party_date(call: types.CallbackQuery, callback_data: dict, state: FSMContext):
    selected_date = await process_calendar_selection(call, callback_data)
    if selected_date:
        user_data = await state.get_data()
        party_with_the_same_date = Parties.select(
            lambda p: p.id != user_data.get('party_id') and p.party_date == selected_date and p.deleted is None
        ).first()
        if party_with_the_same_date:
            return await call.message.reply(
                f'Выбери другую дату. На {selected_date.strftime("%d.%m.%y")} уже забронирована тусовка',
                reply_markup=create_calendar()
            )
        party = Parties.select(lambda p: p.id == user_data.get('party_id')).first()
        await state.finish()
        old_date = party.party_date
        if selected_date == old_date:
            await call.message.answer('Дата не изменилась')
        else:
            party.set(party_date=selected_date)
            commit()
            await bot.send_message(
                settings.target_chat_id,
                f'{call.from_user.full_name} изменил дату тусовки '
                f'с {old_date.strftime("%d.%m.%y")} на {party.party_date.strftime("%d.%m.%y")}'
            )


@dp.callback_query_handler(party_edit_data.filter(edit_type='guests_amount'), state='*')
async def edit_party_guests_amount(call: types.CallbackQuery, callback_data: dict, state: FSMContext):
    await state.update_data(party_id=callback_data['id'])
    await call.message.reply('Сколько все-таки будет гостей?')
    await EditPartyGuestsAmount.edit_party_guests_amount.set()


@dp.message_handler(state=EditPartyGuestsAmount.edit_party_guests_amount)
@db_session
async def choose_party_guests_amount(message: types.Message, state: FSMContext):
    validated, guests_amount = await validate_guests_amount(message, message.text)

    if validated:
        user_data = await state.get_data()
        party = Parties.select(lambda p: p.id == user_data.get('party_id')).first()
        await state.finish()
        old_guests_amount = party.guests_amount
        if guests_amount == old_guests_amount:
            await message.answer('Количество гостей не изменилось')
        else:
            party.set(guests_amount=guests_amount)
            commit()
            await bot.send_message(
                settings.target_chat_id,
                f'{message.from_user.full_name} изменил количество гостей тусовки '
                f'{party.party_date.strftime("%d.%m.%y")} с {old_guests_amount} на {guests_amount}'
            )


@dp.callback_query_handler(party_edit_data.filter(edit_type='sofa_using'))
@db_session
async def reverse_party_sofa_using(call: types.CallbackQuery, callback_data: dict):
    party = Parties.select(lambda p: p.id == callback_data['id']).first()
    old_using_sofa = party.using_sofa
    party.set(using_sofa=not old_using_sofa)
    commit()

    await bot.send_message(
        settings.target_chat_id,
        f'{call.from_user.full_name} изменил использование дивана тусовки {party.party_date.strftime("%d.%m.%y")} '
        f'с «{sofa_dict.get(old_using_sofa)}» на «{sofa_dict.get(party.using_sofa)}»'
    )


@dp.callback_query_handler(party_edit_data.filter(edit_type='delete_party'))
@db_session
async def delete_party(call: types.CallbackQuery, callback_data: dict):
    party = Parties.select(lambda p: p.id == callback_data['id'] and p.deleted is None).first()
    party.set(deleted=datetime.now())
    commit()
    await bot.send_message(
        settings.target_chat_id,
        f'{call.from_user.full_name} не будет устраивать тусовку {party.party_date.strftime("%d.%m.%y")}'
    )


def prepare_statistics():
    users_with_notifications = select(
        (u.firstname, u.lastname, count(n)) for u in Users for n in u.notifications if u.deleted is None
    ).order_by(-3)
    users_without_notifications = select(
        (u.firstname, u.lastname, 0) for u in Users if not u.notifications and u.deleted is None
    )
    rollback()
    users = list(users_with_notifications) + list(users_without_notifications)
    users_list = [f'{i}. {u[0]} {u[1]} - {u[2]}' for i, u in enumerate(users, 1)]
    return '\n'.join(users_list)


@dp.message_handler(Text(equals='Статистика 📊'))
@chat_checker()
async def show_statistics(message: types.Message):
    statistics = prepare_statistics()
    await message.answer(statistics)


@db_session
async def show_cron_statistics():
    statistics = prepare_statistics()
    await bot.send_message(settings.target_chat_id, statistics)


def schedule_daily_notifications():
    scheduler.add_job(show_cron_statistics, 'cron', hour=4, minute=6)
