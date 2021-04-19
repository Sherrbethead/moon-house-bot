from datetime import datetime

from gino import Gino

db = Gino()


class BaseModel(db.Model):
    __abstract__ = True

    created = db.Column(db.DateTime(True), nullable=False, default=datetime.now)
    deleted = db.Column(db.DateTime(True))
