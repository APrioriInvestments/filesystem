import datetime
import functools
import pytz


NYC = pytz.timezone("America/New_York")
UTC = pytz.timezone("UTC")
LondonTz = pytz.timezone("Europe/London")


class Timestamp:
    DEFAULT_FORMAT = "%Y-%m-%d %H:%M:%S"

    @staticmethod
    def dateToDatetime(date, baseTime=None):
        """Convert a date object into a timezone-agnostic datetime object.

        If a datetime object is given, it is returned unchanged.

        This merits its own function because of the easy mistake made frequently
        which is to overlook that a datetime.datetime object is an instance of
        datetime.date.

        Args:
            date (OneOf(datetime.date, datetime.datetime)): the object to
                convert to a datetime object
            baseTime (OneOf(None, datetime.time())): the time to combine
                with the date object in order to produce the datetime object.
                If None is given, baseTime is set to datetime.datetime.min.time().
                Another common value to use is datetime.datetime.max.time()
        """
        if not isinstance(date, datetime.date):
            raise ValueError(f"dateToDatetime called with argument that is not a date: {date}")

        if isinstance(date, datetime.datetime):
            return date

        else:
            if baseTime is None:
                baseTime = datetime.datetime.min.time()  # == datetime.time(0, 0)
            return datetime.datetime.combine(date, baseTime)

    @staticmethod
    def toDatetime(timeObj, timezone):
        """convert a timeObject into a timezone-aware datetime.datetime

        Args:
            timeObj (OneOf(float, int, datetime.date, datetime.datetime)):
                the time object to convert into a datetime.datetime. It may
                be a timestamp (float or int), a date (datetime.date), or
                a datetime object

            timezone (OneOf(str, pytz.timezone)): the timezone for the resulting
                datetime object
        """
        if isinstance(timezone, str):
            timezone = pytz.timezone(timezone)

        if isinstance(timeObj, (int, float)):
            return datetime.datetime.fromtimestamp(timeObj, timezone)

        if isinstance(timeObj, datetime.date):
            timeObj = Timestamp.dateToDatetime(timeObj)

        if not isinstance(timeObj, datetime.datetime):
            raise ValueError(f"timeObj of unexpected type: '{type(timeObj)}'")

        if timeObj.tzinfo is None:
            return timezone.localize(timeObj)
        else:
            return timeObj.astimezone(timezone)

    @staticmethod
    def toDatetimeStr(timeObj, timezone, formatStr=None):
        if formatStr is None:
            formatStr = Timestamp.DEFAULT_FORMAT
        return Timestamp.toDatetime(timeObj, timezone).strftime(formatStr)

    @staticmethod
    def toNycDatetime(timeObj):
        return Timestamp.toDatetime(timeObj, NYC)

    @staticmethod
    def toNycDatetimeStr(timeObj, formatStr=None):
        return Timestamp.toDatetimeStr(timeObj, NYC, formatStr=formatStr)

    @staticmethod
    def toUtcDatetime(timeObj):
        return Timestamp.toDatetime(timeObj, UTC)

    @staticmethod
    def toUtcDatetimeStr(timeObj, formatStr=None):
        return Timestamp.toDatetimeStr(timeObj, UTC, formatStr=formatStr) + " UTC"

    @staticmethod
    def fromDatetimeStr(datetimeString, timezone):
        datetimeString = datetimeString.split(" ")
        date = datetimeString[0]
        if len(datetimeString) > 1:
            time = datetimeString[1]
        else:
            time = "00:00:00,000"
        date = date.split("-")
        year = int(date[0])
        month = int(date[1])
        day = int(date[2])

        time = time.split(",")
        clock = time[0]
        if len(time) > 1:
            micro = int(time[1])
        else:
            micro = 0
        clock = clock.split(":")
        hour = int(clock[0])
        if len(clock) > 2:
            minute = int(clock[1])
            second = int(clock[2])
        elif len(clock) == 2:
            minute = int(clock[1])
            second = 0
        else:
            minute = 0
            second = 0

        dt = datetime.datetime(year, month, day, hour, minute, second, micro)

        return timezone.localize(dt)

    @staticmethod
    def fromUtcDatetimeStr(datetimeString):
        return Timestamp.fromDatetimeStr(datetimeString, UTC)

    @staticmethod
    def fromNycDatetimeStr(datetimeString):
        return Timestamp.fromDatetimeStr(datetimeString, NYC)


toNycDatetime = Timestamp.toNycDatetime
toNycDatetimeStr = Timestamp.toNycDatetimeStr

toUtcDatetime = Timestamp.toUtcDatetime
toUtcDatetimeStr = Timestamp.toUtcDatetimeStr


class ExceededRetriesException(Exception):
    pass


def retry(
    func=None,
    *,
    retries=3,
    caughtExceptions=(Exception,),
    onException=lambda: None,
    onExceptionMember=None,
):
    """retry decorator with optional arguments.

    If the number of retries is exceeded, an ExceededRetriesException that
    wraps the underlying exception is raised. This is done to prevent
    unintended retries when multiple retry-decorated functions are active on
    the call-stack.

    Args:
        func (Function): function to execute and retry if it fails. If None,
            return a function whose only remaining argument is func (using
            functools.partial). This allows using this decorator in bare form
            `@retry` or with arguments `@retry(retries=10, ...)`
        retries (int): how many times to retry
        caughtExceptions (tuple of exception types): which exceptions to catch
        onException: a method to call when an exception happens (before retrying)
        onExceptionMember (str): name of a member function to call when an
            exception happens (before retrying). If None, this step is skipped.
    """
    if func is None:
        return functools.partial(
            retry,
            retries=retries,
            caughtExceptions=caughtExceptions,
            onException=onException,
            onExceptionMember=onExceptionMember,
        )

    @functools.wraps(func)
    def retryWrapper(*args, **kwargs):
        for ix in range(retries):
            try:
                return func(*args, **kwargs)
            except caughtExceptions as e:
                if isinstance(e, ExceededRetriesException):
                    raise
                if ix + 1 >= retries:
                    raise ExceededRetriesException(f"Exceeded {retries} retries") from e
                else:
                    onException()
                    if onExceptionMember is not None:
                        if len(args) == 0:
                            raise Exception("retry decorator could not find 'self'")
                        self = args[0]
                        getattr(self, onExceptionMember)()

    return retryWrapper


def chunkedByteStreamPipe(inStream, outStream, amount=-1, chunkSize=10 * 1024**2):
    """Write the contents of the input stream to the output stream

    Args:
        inStream: the byte stream to read from
        outStream: the byte stream to write to
        amount (int): the number of bytes to write, or -1 for "until EOF"
        chunkSize (int): the number of bytes to load into memory each time.
    """
    if amount < 0:
        readAmount = chunkSize
    else:
        readAmount = amount

    while readAmount > 0:
        chunk = inStream.read(readAmount)
        if not chunk:
            break
        outStream.write(chunk)

        if amount > 0:
            readAmount -= len(chunk)
