from datetime import datetime, timezone
from functools import partial

from gino import Gino

db = Gino()


class BaseModel(db.Model):
    __abstract__ = True

    created = db.Column(
        db.DateTime(True),
        nullable=False,
        default=partial(
            datetime.now,
            timezone.utc,
        )
    )
    deleted = db.Column(db.DateTime(True))
