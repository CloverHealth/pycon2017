import datetime

UTC_TZ = datetime.timezone.utc


def utc_now() -> datetime.datetime:
    """
    Returns the current datetime with the UTC timezone set.

    This is used to specify default model timestamps using the application's clock
    """
    return datetime.datetime.now(UTC_TZ)
