from datetime import datetime

def format_date(date_str):
    """
    Converts a date string in the format YYYYMMDD to a datetime object.
    Example: "20050301" -> 2005-03-01 00:00:00
    """
    return datetime.strptime(date_str, "%Y%m%d")

# This function is currently not used in the main processing flow, but is available for future use if needed.
def format_datetime(datetime_str):
    """
    Converts a datetime string in the format YYYYMMDDHHMMSS to a datetime object.
    Example: "20050310121004" -> 2005-03-10 12:10:04
    """
    return datetime.strptime(datetime_str, "%Y%m%d%H%M%S")