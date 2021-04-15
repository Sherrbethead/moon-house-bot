from pony.orm import Database

from config import settings

db = Database()
db.bind(provider=settings.database.provider,
        user=settings.database.user,
        password=settings.database.password,
        host=settings.database.host,
        database=settings.database.name)
