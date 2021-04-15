from datetime import datetime, date

from pony.orm import PrimaryKey, Required, Set, Optional

from moon_house_bot.database import db


class Users(db.Entity):
    chat_id = PrimaryKey(int)
    firstname = Required(str)
    lastname = Optional(str)
    nickname = Optional(str)
    is_admin = Required(bool, default=False)
    created = Required(datetime, default=datetime.now)
    deleted = Optional(datetime)
    notifications = Set('Notifications')
    parties = Set('Parties')


class Notifications(db.Entity):
    id = PrimaryKey(int, auto=True)
    user = Required(Users, reverse='notifications')
    notification_type = Required(str)
    created = Required(datetime, default=datetime.now)
    deleted = Optional(datetime)


class Parties(db.Entity):
    id = PrimaryKey(int, auto=True)
    user = Required(Users, reverse='parties')
    party_date = Required(date)
    guests_amount = Optional(int)
    using_sofa = Required(bool)
    created = Required(datetime, default=datetime.now)
    deleted = Optional(datetime)


db.generate_mapping(create_tables=True)
