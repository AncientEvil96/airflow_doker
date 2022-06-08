from datetime import datetime


def convert_dict_args(arg_string):
    s = str(arg_string).replace('[', '').replace(']', '').replace("'", '').replace('(', '').replace(')', '').replace(
        '"', ''
    ).split(
        ',')
    return dict(zip(s[::2], s[1::2]))


def get_date(date_, for_ms=False, format_date='%Y%m%d'):
    executor_date = datetime.strptime(str(date_), format_date)
    return datetime(
        executor_date.year + (2000 if for_ms else 0),
        executor_date.month,
        executor_date.day,
        executor_date.hour,
        executor_date.minute,
        executor_date.second
    )


def get_datetime(datetime_, for_ms=False, format_date='%Y-%m-%d %H:%M:%S'):
    executor_date = datetime.strptime(str(datetime_), format_date)
    return datetime(
        executor_date.year + (2000 if for_ms else 0),
        executor_date.month,
        executor_date.day,
        executor_date.hour,
        executor_date.minute,
        executor_date.second
    )
