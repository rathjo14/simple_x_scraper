from datetime import datetime

class TimestampChecker():
    def __init__(self):
        pass
    def str_to_datetime(self, timestamp_string : str, timestamp_object : datetime) -> datetime:
        try:
            return datetime.strptime(timestamp_string, timestamp_object)
        except ValueError:
            print("time data %s does not match format %s" % (timestamp_string, timestamp_object))
            raise

    def datetime_to_str(self, timestamp_object : datetime, timestamp_string : str) -> str:
        return datetime.strftime(timestamp_object, timestamp_string)

    def check_cutoff(self, start_timestamp_object : datetime, days_cutoff : int) -> bool:
        delta_days = datetime.now() - start_timestamp_object
        if delta_days.days < 7:
            return True
        else:
            return False