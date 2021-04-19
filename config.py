from dynaconf import Dynaconf

settings = Dynaconf(
    envvar_prefix=False,
    settings_files=['settings.toml', '.secrets.toml'],
    load_dotenv=True,
)
settings.webhook.path = f'/{settings.token}'
