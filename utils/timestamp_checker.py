from datetime import datetime

class TimestampChecker():
    def __init__(self, timestamp_string_format : str):
        self.timestamp_string_format = timestamp_string_format
        self.x_timestamp_string_format = "%Y-%m-%dT%H:%M:%SZ"
        self.cutoff_days = 7 #default twitter developer basic plan value

    def check_format(self, timestamp_string)-> datetime:
        try:
            return datetime.strftime(datetime.strptime(timestamp_string, self.timestamp_string_format), self.x_timestamp_string_format)
        except ValueError:
            print("time data %s does not match format %s" % (timestamp_string, self.timestamp_string_format))
            raise

    def check_cutoff(self, start_timestamp : str) -> bool:
        '''we check default 7 day parser history limit from twitter developer basic plan'''
        now_timestamp = datetime.now()
        delta_days = now_timestamp - datetime.strptime(start_timestamp, self.timestamp_string_format)
        if delta_days.days < 7:
            return True
        else:
            return False