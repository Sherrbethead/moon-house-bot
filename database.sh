# присваиваем значения для сервера, имени пользователя, паролю и названию БД из конфига сервиса
DB_HOST=$(python3 -c "from config import settings; print(settings.database.host)")
DB_USER=$(python3 -c "from config import settings; print(settings.database.user)")
DB_NAME=$(python3 -c "from config import settings; print(settings.database.name)")
export PGPASSWORD=$(python3 -c "from config import settings; print(settings.database.password)")

# проверяем наличие бд
DB=$(psql -h "${DB_HOST}" -U "${DB_USER}" -t -d postgres -c "SELECT datname FROM pg_database WHERE datname='${DB_NAME}'")
# если нет, то создаем новую
if [[ ! "$DB" ]]; then
        psql -h "${DB_HOST}" -U "${DB_USER}" -c "CREATE DATABASE ${DB_NAME};"
fi