# from pony import orm
#
#
# class Database:
#     def __init__(self, settings):
#         self.db = orm.Database()
#         self.Entity = self.db.Entity
#         self.provider = settings.provider
#         self.user = settings.user
#         self.password = settings.password
#         self.host = settings.host
#         self.database = settings.name
#
#     def connect(self):
#         self.db.bind(
#             proviver=self.provider,
#             user=self.user,
#             password=self.password,
#             host=self.host,
#             database=self.database
#         )
