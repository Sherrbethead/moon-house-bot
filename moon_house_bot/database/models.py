from moon_house_bot.database.db import BaseModel, db


class User(BaseModel):
    __tablename__ = 'users'

    chat_id = db.Column(db.Integer(), primary_key=True, unique=True, autoincrement=False)
    firstname = db.Column(db.String(), nullable=False)
    lastname = db.Column(db.String())
    nickname = db.Column(db.String())
    is_admin = db.Column(db.Boolean(), nullable=False, default=False)


class Notification(BaseModel):
    __tablename__ = 'notifications'

    id = db.Column(db.Integer(), primary_key=True, unique=True)
    user_id = db.Column(db.ForeignKey(F'{User.__tablename__}.chat_id'), nullable=False)
    notification_type = db.Column(db.String(), nullable=False)


class Party(BaseModel):
    __tablename__ = 'parties'

    id = db.Column(db.Integer(), primary_key=True, unique=True)
    user_id = db.Column(db.ForeignKey(F'{User.__tablename__}.chat_id'), nullable=False)
    party_date = db.Column(db.Date(), nullable=False)
    guests_amount = db.Column(db.Integer(), nullable=False)
    using_sofa = db.Column(db.Boolean(), nullable=False)
