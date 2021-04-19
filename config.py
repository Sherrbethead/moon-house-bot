from dynaconf import Dynaconf

settings = Dynaconf(
    envvar_prefix=False,
    load_dotenv=True,
    settings_files=['settings.toml', '.secrets.toml'],
)
settings.webhook.path = f'/{settings.token}'
