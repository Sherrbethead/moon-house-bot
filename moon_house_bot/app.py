import logging
from datetime import date, datetime, timedelta
from distutils.util import strtobool

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import Text
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.utils.callback_data import CallbackData
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import Date, and_, case, text

from aiogramcalendar import calendar_callback, create_calendar, process_calendar_selection
from config import settings
from moon_house_bot.database import Notification, Party, User, db

bot = Bot(token=settings.token)
dp = Dispatcher(bot, storage=MemoryStorage())
scheduler = AsyncIOScheduler()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def chat_checker(login_required: bool = True):
    def decorator(func):

        async def wrapper(message):
            if message.chat.type == 'private' or message.chat.id == settings.target_chat_id:
                user = await User.query.where(and_(
                    User.chat_id == message.from_user.id,
                    User.deleted.is_(None)
                )).gino.first()
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

DISHWASHER_TIME_LOADING = 4


@dp.message_handler(commands=['start', 'home'])
@chat_checker(login_required=False)
async def main_menu_handler(message: types.Message, user: User):
    if message.chat.type == 'group':
        message_for_all_users = 'Используй команды /home или /start в личной переписке со мной, ' \
                                'чтобы при совершении каких-то действий не захламлять чат сообщениями. ' \
                                'А все важные оповещения будут показываться здесь для всех.'
        if not user:
            deleted_user = await User.query.where(and_(
                User.chat_id == message.from_user.id,
                User.deleted.isnot(None)
            )).gino.first()
            message_for_deleted = 'снова ' if deleted_user else ''
            if deleted_user:
                async with db.transaction():
                    await deleted_user.update(deleted=None).apply()
            else:
                async with db.transaction():
                    await User.create(
                        chat_id=message.from_user.id,
                        firstname=message.from_user.first_name,
                        lastname=message.from_user.last_name,
                    )
            return await message.answer(f'Привет, {message.from_user.full_name}! '
                                        f'Теперь ты {message_for_deleted}часть бытовухи квартиры на улице Радио!\n'
                                        f'{message_for_all_users}')
        return await message.answer(message_for_all_users)
    if user:
        keyboard_markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        all_buttons = ('Посудомойка 🍴', 'Выкинуть мусор 🗑', 'Тусовки 🍻', 'Тише 🤫', 'Статистика 📊')
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
    admins = await User.query.where(and_(User.is_admin, User.deleted.is_(None))).gino.all()
    for admin in admins:
        await bot.send_message(
            admin.chat_id,
            f'[{message.from_user.full_name}](tg://user?id={message.from_user.id}) хочет присоединиться '
            f'к бытовухе на Радио. Что будем делать?',
            reply_markup=keyboard_markup,
            parse_mode='MarkdownV2'
        )


@dp.callback_query_handler(new_user_data.filter())
async def resolve_new_user(call: types.CallbackQuery, callback_data: dict):
    accepted = strtobool(callback_data['accept'])
    message_words_list = call.message.text.split(maxsplit=2)

    message_addon = '' if accepted else 'не '
    message_for_new_user = f'Ты {message_addon}принят в бытовуху на Радио'
    await bot.send_message(callback_data['id'], message_for_new_user)

    if accepted:
        deleted_user = await User.query.where(and_(
            User.chat_id == int(callback_data['id']),
            User.deleted.isnot(None)
        )).gino.first()
        if deleted_user:
            async with db.transaction():
                await deleted_user.update(deleted=None).apply()
        else:
            async with db.transaction():
                await User.create(
                    chat_id=callback_data['id'],
                    firstname=message_words_list[0],
                    lastname=message_words_list[1],
                )

        await call.message.answer(f'Ты принял {message_words_list[0]} {message_words_list[1]} в бытовуху на Радио')
        return await call.message.delete_reply_markup()

    await call.message.answer(f'Ты отклонил заявку {message_words_list[0]} {message_words_list[1]}')
    await call.message.delete_reply_markup()


async def check_user_honesty(message: types.Message, notification_type: str):
    today_trash_notifications = await db.select(
        [
            db.func.count()
        ]
    ).where(and_(
        Notification.notification_type == notification_type,
        Notification.created.cast(Date) == date.today(),
        Notification.deleted.is_(None)
    )).gino.scalar()
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
async def dishwasher_callback(call: types.CallbackQuery):
    honesty = await check_user_honesty(call.message, call.data)

    if honesty:
        action_prefixes = {
            True: 'раз',
            False: 'за'
        }
        unload = call.data.endswith('unload')
        notification_query = Notification.query.where(and_(
            Notification.notification_type.in_(['dishwasher_load', 'dishwasher_unload']),
            Notification.deleted.is_(None)
        ))
        dishwasher_last_notification = await notification_query.order_by(Notification.created.desc()).gino.first()

        if (not dishwasher_last_notification and unload) or \
                (dishwasher_last_notification and dishwasher_last_notification.notification_type == call.data):
            last_time_loaded = f'\nПоследний раз {action_prefixes.get(unload)}гружалась: ' \
                               f'{dishwasher_last_notification.created.astimezone().strftime("%d.%m.%y в %H:%M")}' \
                if dishwasher_last_notification else ''
            return await call.message.answer(
                f'Прежде чем {action_prefixes.get(unload)}грузить посудомойку, '
                f'ее надо {action_prefixes.get(not unload)}грузить{last_time_loaded}'
            )
        now = datetime.now().astimezone()
        dishwasher_working_minutes = timedelta(minutes=DISHWASHER_TIME_LOADING)
        if unload:
            time_to_unload = dishwasher_last_notification.created + dishwasher_working_minutes
            if time_to_unload > now:
                await call.message.delete_reply_markup()
                return await call.message.answer(
                    f'Посудомойка моет до {time_to_unload.astimezone().strftime("%H:%M")}, нельзя разгрузить'
                )

        else:
            datetime_unload = now + dishwasher_working_minutes
            scheduler.add_job(send_dishwasher_unload_notify, 'date', run_date=datetime_unload)
        async with db.transaction():
            new_notification = await Notification.create(
                user_id=call.from_user.id,
                notification_type=call.data
            )
            unload_time = '' if unload \
                else f', разгрузить можно будет в ' \
                     f'{(new_notification.created + dishwasher_working_minutes).astimezone().strftime("%H:%M")}'

        await bot.send_message(
            settings.target_chat_id,
            f'{call.from_user.full_name} {action_prefixes.get(unload)}грузил(а) посудомойку{unload_time}'
        )
        await call.message.delete_reply_markup()


@dp.message_handler(Text(equals='Выкинуть мусор 🗑'))
@chat_checker()
async def trash_handler(message: types.Message):
    honesty = await check_user_honesty(message, 'trash')
    if honesty:
        async with db.transaction():
            await Notification.create(
                user_id=message.from_user.id,
                notification_type='trash'
            )

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
async def choose_party_date(call: types.CallbackQuery, callback_data: dict, state: FSMContext):
    selected_date = await process_calendar_selection(call, callback_data)
    if isinstance(selected_date, str):
        await state.finish()
        return await call.message.answer('Бронирование тусовки отменено')
    elif selected_date:
        party = await Party.query.where(and_(
            Party.party_date == selected_date,
            Party.deleted.is_(None)
        )).gino.first()
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
async def plan_party_save(call: types.CallbackQuery, state: FSMContext):
    using_sofa = call.data.endswith('yes')
    await state.update_data(using_sofa=using_sofa)
    user_data = await state.get_data()
    async with db.transaction():
        await Party.create(user_id=call.from_user.id, **user_data)
    await state.finish()
    await bot.send_message(
        settings.target_chat_id,
        f"{call.from_user.full_name} забронировал(а) тусовку {user_data.get('party_date').strftime('%d.%m.%y')}\n"
        f"Количество гостей: {user_data.get('guests_amount')}\n"
        f"Диван будет занят: {sofa_dict.get(user_data.get('using_sofa'))}"
    )
    await call.message.delete_reply_markup()


@dp.callback_query_handler(Text(startswith='party_closest'))
async def show_closest_parties(call: types.CallbackQuery):
    parties_query = Party.join(User).select().where(and_(
        Party.party_date >= date.today(),
        Party.deleted.is_(None)
    ))
    closest_parties = await parties_query.order_by(Party.party_date).limit(3).gino.all()

    if closest_parties:
        parties_list = [f'Ближайшие {len(closest_parties)} тусовки:']
        parties_list.extend([
            f'{p.party_date.strftime("%d.%m.%y")}, людей: {p.guests_amount}, '
            f'диван будет занят: {sofa_dict.get(p.using_sofa)}, кем забронирована: {p.firstname} {p.lastname}'
            for p in closest_parties
        ])
        parties_answer = '\n'.join(parties_list)
    else:
        parties_answer = 'На ближайшее время никаких тусовок не запланировано'
    await call.message.answer(parties_answer)
    await call.message.delete_reply_markup()


@dp.callback_query_handler(Text(equals='party_manage'))
async def show_user_parties(call: types.CallbackQuery):
    parties_query = Party.query.where(and_(
        Party.user_id == call.from_user.id,
        Party.party_date >= date.today(),
        Party.deleted.is_(None)
    ))
    user_parties = await parties_query.order_by(Party.party_date).gino.all()
    if user_parties:
        keyboard_markup = types.InlineKeyboardMarkup(row_width=1)
        your_parties_buttons = [
            types.InlineKeyboardButton(
                f'{p.party_date.strftime("%d.%m.%y")}, '
                f'гостей: {p.guests_amount}, '
                f'диван будет занят: {sofa_dict.get(p.using_sofa)}',
                callback_data=party_manage_data.new(id=p.id, using_sofa=p.using_sofa)
            ) for p in user_parties
        ]
        keyboard_markup.add(*your_parties_buttons)
        await call.message.answer('Выбери тусовку', reply_markup=keyboard_markup)
    else:
        await call.message.answer('У тебя нет ни одной запланированной тусовки')
    await call.message.delete_reply_markup()


@dp.callback_query_handler(party_manage_data.filter())
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
    await state.update_data(party_id=int(callback_data['id']))
    await call.message.answer('Выбери новую дату', reply_markup=create_calendar())
    await call.message.delete_reply_markup()
    await EditPartyDate.edit_party_date.set()


@dp.callback_query_handler(calendar_callback.filter(), state=EditPartyDate.edit_party_date)
async def choose_new_party_date(call: types.CallbackQuery, callback_data: dict, state: FSMContext):
    selected_date = await process_calendar_selection(call, callback_data)
    if selected_date:
        user_data = await state.get_data()
        await state.finish()

        if isinstance(selected_date, str):
            return await call.message.answer('Изменение даты отменено')
        party_with_the_same_date = await Party.query.where(and_(
            Party.id != user_data.get('party_id'),
            Party.party_date == selected_date,
            Party.deleted.is_(None)
        )).gino.first()
        if party_with_the_same_date:
            return await call.message.reply(
                f'Выбери другую дату. На {selected_date.strftime("%d.%m.%y")} уже забронирована тусовка',
                reply_markup=create_calendar()
            )
        party = await Party.query.where(Party.id == user_data.get('party_id')).gino.first()
        old_date = party.party_date
        if selected_date == old_date:
            await call.message.answer('Дата не изменилась')
        else:
            async with db.transaction():
                await party.update(party_date=selected_date).apply()
            await bot.send_message(
                settings.target_chat_id,
                f'{call.from_user.full_name} изменил дату тусовки '
                f'с {old_date.strftime("%d.%m.%y")} на {party.party_date.strftime("%d.%m.%y")}'
            )


@dp.callback_query_handler(party_edit_data.filter(edit_type='guests_amount'), state='*')
async def edit_party_guests_amount(call: types.CallbackQuery, callback_data: dict, state: FSMContext):
    await state.update_data(party_id=int(callback_data['id']))
    await call.message.reply('Сколько все-таки будет гостей?')
    await EditPartyGuestsAmount.edit_party_guests_amount.set()


@dp.message_handler(state=EditPartyGuestsAmount.edit_party_guests_amount)
async def choose_party_guests_amount(message: types.Message, state: FSMContext):
    validated, guests_amount = await validate_guests_amount(message, message.text)

    if validated:
        user_data = await state.get_data()
        party = await Party.query.where(Party.id == user_data.get('party_id')).gino.first()
        await state.finish()
        old_guests_amount = party.guests_amount
        if guests_amount == old_guests_amount:
            await message.answer('Количество гостей не изменилось')
        else:
            async with db.transaction():
                await party.update(guests_amount=guests_amount).apply()
            await bot.send_message(
                settings.target_chat_id,
                f'{message.from_user.full_name} изменил количество гостей тусовки '
                f'{party.party_date.strftime("%d.%m.%y")} с {old_guests_amount} на {guests_amount}'
            )


@dp.callback_query_handler(party_edit_data.filter(edit_type='sofa_using'))
async def reverse_party_sofa_using(call: types.CallbackQuery, callback_data: dict):
    party = await Party.query.where(Party.id == int(callback_data['id'])).gino.first()
    old_using_sofa = party.using_sofa
    async with db.transaction():
        await party.update(using_sofa=not old_using_sofa).apply()

    await bot.send_message(
        settings.target_chat_id,
        f'{call.from_user.full_name} изменил использование дивана тусовки {party.party_date.strftime("%d.%m.%y")} '
        f'с «{sofa_dict.get(old_using_sofa)}» на «{sofa_dict.get(party.using_sofa)}»'
    )


@dp.callback_query_handler(party_edit_data.filter(edit_type='delete_party'))
async def delete_party(call: types.CallbackQuery, callback_data: dict):
    party = await Party.query.where(and_(
        Party.id == int(callback_data['id']),
        Party.deleted.is_(None)
    )).gino.first()
    async with db.transaction():
        await party.update(deleted=datetime.now().astimezone()).apply()
    await bot.send_message(
        settings.target_chat_id,
        f'{call.from_user.full_name} не будет устраивать тусовку {party.party_date.strftime("%d.%m.%y")}'
    )
    await call.message.delete_reply_markup()


async def prepare_rating(header: str, cron: bool = False):
    users_with_notifications = await db.select(
        [
            User.chat_id,
            User.firstname,
            User.lastname,
            db.func.count(Notification.id).label('total'),
            db.func.count(case(
                [((Notification.notification_type.startswith('dishwasher')), Notification.id)])).label('dishwasher'),
            db.func.count(case(
                [((Notification.notification_type == 'trash'), Notification.id)])).label('trash')
        ]
    ).select_from(
        User.join(Notification)
    ).where(and_(
        Notification.notification_type != 'silence',
        Notification.deleted.is_(None)
    )).group_by(
        User.chat_id
    ).order_by(text('total DESC')).gino.all()
    users_list = [f'{i}. {u.firstname} {u.lastname}   -   {u.dishwasher}🍴,   {u.trash}🗑'
                  for i, u in enumerate(users_with_notifications, 1)]
    if users_list:
        users_list.insert(0, f'{header}:')
    if cron:
        users_without_notifications = await User.query.where(User.chat_id.notin_([
            u.chat_id for u in users_with_notifications
        ])).gino.all()
        return '\n'.join(users_list), users_with_notifications[-1], users_without_notifications

    return '\n'.join(users_list)


@dp.message_handler(Text(equals='Тише 🤫'))
@chat_checker()
async def parties_handler(message: types.Message):
    last_silence_notifications = await db.select(
        [
            db.func.count()
        ]
    ).where(and_(
        Notification.notification_type == 'silence',
        Notification.created > datetime.now().astimezone() - timedelta(minutes=30),
        Notification.deleted.is_(None)
    )).gino.scalar()
    if last_silence_notifications:
        if last_silence_notifications == 3:
            return await message.answer('Просьбы не подействовали. Видимо стоит сходить поговорить без моей помощи')
        else:
            await bot.send_message(settings.target_chat_id, 'Попросили же сделать потише! Или нужно выйти разобраться?')
    else:
        await bot.send_message(settings.target_chat_id, 'Слишком громко! Можно потише?')

    async with db.transaction():
        await Notification.create(
            user_id=message.from_user.id,
            notification_type='silence'
        )


@dp.message_handler(Text(equals='Статистика 📊'))
@chat_checker()
async def statistics_handler(message: types.Message):
    keyboard_markup = types.InlineKeyboardMarkup()
    parties_buttons = [
        types.InlineKeyboardButton('Рейтинг', callback_data='statistics_usefulness'),
        types.InlineKeyboardButton('Жалобы на шум', callback_data='statistics_silence'),
    ]
    keyboard_markup.add(*parties_buttons)
    await message.answer('Какую статистику показать?', reply_markup=keyboard_markup)


@dp.callback_query_handler(Text(equals='statistics_usefulness'))
async def show_rating(call: types.CallbackQuery):
    rating = await prepare_rating(header='Рейтинг активности жильцов')
    if rating:
        await call.message.answer(rating)
    else:
        await call.message.answer('К сожалению, показать нечего. Никакой активности нет')
    await call.message.delete_reply_markup()


@dp.callback_query_handler(Text(equals='statistics_silence'))
async def show_silence_statistics(call: types.CallbackQuery):
    silence_notifications = await db.select(
        [
            db.func.count(case(
                [((Notification.user_id == call.from_user.id), Notification.id)])).label('yours'),
            db.func.count(case(
                [((Notification.user_id != call.from_user.id), Notification.id)])).label('others')
        ]
    ).where(and_(
        Notification.notification_type == 'silence',
        Notification.deleted.is_(None)
    )).gino.first()
    await call.message.answer(f'За все время ты пожаловался на шум {silence_notifications.yours} раз, '
                              f'а другие пожаловались {silence_notifications.others} раз')
    await call.message.delete_reply_markup()


async def show_cron_rating():
    rating, worst_user_with_notifications, users_without_notifications = await prepare_rating(
        header='Сводка самых активных жильцов', cron=True
    )
    if rating:
        await bot.send_message(settings.target_chat_id, rating)
        if users_without_notifications:
            tag_users = [f'[{u.firstname}](tg://user?id={u.chat_id})' for u in users_without_notifications]
            plurality_message = 'вас' if len(users_without_notifications) > 1 else 'тебя'
            return await bot.send_message(
                settings.target_chat_id,
                f'{" ,".join(tag_users)}, у {plurality_message} по нулям, пора сделать что\-то полезное в квартире',
                parse_mode='MarkdownV2'
            )
        return await bot.send_message(
            settings.target_chat_id,
            f'[{worst_user_with_notifications.firstname}](tg://user?id={worst_user_with_notifications.chat_id}), '
            f'пришла твоя очередь сделать что\-то полезное в квартире',
            parse_mode='MarkdownV2'
        )


async def show_cron_closest_parties():
    parties_query = Party.join(User).select().where(and_(
        Party.party_date >= date.today(),
        Party.party_date <= date.today() + timedelta(days=3),
        Party.deleted.is_(None)
    ))
    closest_parties = await parties_query.order_by(Party.party_date).gino.all()
    if closest_parties:
        closest_parties_dict = {
            0: 'Сегодня',
            1: 'Завтра',
            2: 'Послезавтра',
            3: 'Через 2 дня'
        }
        parties_list = ['Сводка ближайших тусовок:']
        parties_list.extend([f'{i}. {closest_parties_dict.get((p.party_date - date.today()).days)} - '
                             f'гостей: {p.guests_amount}, диван будет занят: {sofa_dict.get(p.using_sofa)}, '
                             f'кем забронирована: {p.firstname} {p.lastname}'
                             for i, p in enumerate(closest_parties, 1)])

        await bot.send_message(settings.target_chat_id, '\n'.join(parties_list))


async def check_dishwasher_loading():
    notification_query = Notification.query.where(and_(
        Notification.notification_type.in_(['dishwasher_load', 'dishwasher_unload']),
        Notification.deleted.is_(None)
    ))
    dishwasher_last_notification = await notification_query.order_by(Notification.created.desc()).gino.first()
    dishwasher_working_minutes = timedelta(minutes=DISHWASHER_TIME_LOADING)
    if dishwasher_last_notification and dishwasher_last_notification.notification_type == 'dishwasher_load' and \
            dishwasher_last_notification.created + dishwasher_working_minutes > datetime.now().astimezone():
        scheduler.add_job(
            send_dishwasher_unload_notify,
            'date',
            run_date=dishwasher_last_notification.created + dishwasher_working_minutes
        )


def schedule_daily_notifications():
    now_with_gap = datetime.now().astimezone() + timedelta(seconds=5)

    scheduler.add_job(show_cron_rating, 'cron', hour=0, minute=0)
    scheduler.add_job(show_cron_closest_parties, 'cron', hour=0, minute=0)
    scheduler.add_job(
        check_dishwasher_loading,
        'cron',
        hour=now_with_gap.hour,
        minute=now_with_gap.minute,
        second=now_with_gap.second
    )


async def on_startup(dp):
    logging.info('Starting app...')
    url = f'postgresql://{settings.database.user}:{settings.database.password}' \
          f'@{settings.database.host}:5432/{settings.database.name}'
    logging.info(f'Connecting to DB: {url}')
    await db.set_bind(url)
    await db.gino.create_all()
    schedule_daily_notifications()
    scheduler.start()
    await bot.set_webhook(f'{settings.webhook.host}{settings.webhook.path}')
    webhook = await bot.get_webhook_info()
    if webhook.url and webhook.url == f'{settings.webhook.host}{settings.webhook.path}':
        logging.info(f"Webhook configured. Pending updates count {webhook.pending_update_count}")
    else:
        logging.error("Configured wrong webhook URL {webhook}", webhook=webhook.url)


async def on_shutdown(dp):
    logging.warning('Shutting down..')

    await db.pop_bind().close()

    # Remove webhook (not acceptable in some cases)
    await bot.delete_webhook()

    # Close DB connection (if used)
    await dp.storage.close()
    await dp.storage.wait_closed()

    logging.warning('Bye!')
