class DbHydraException(Exception):
    """Raise for db_hydra specific exceptions such as 'pyodbc can't be used on macOS as it's not supported'."""
