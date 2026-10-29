"""What may be logged about an exception: its qualified class name and SQLSTATE, never str(exc).

PostgreSQL's error DETAIL, carried in the chained asyncpg exception, can quote a whole row,
payload included, and so can a traceback.
"""


def class_name(exc: BaseException) -> str:
    # Qualified, because the bare name is ambiguous: sqlalchemy.exc.TimeoutError is a full pool,
    # builtins.TimeoutError a database that stopped answering.
    return f"{type(exc).__module__}.{type(exc).__qualname__}"


def sqlstate(exc: BaseException) -> str | None:
    # SQLAlchemy's asyncpg adapter copies the code onto the driver error it wraps (exc.orig).
    code = getattr(getattr(exc, "orig", None), "sqlstate", None)
    return code if isinstance(code, str) else None
