from datetime import datetime

def convert_to_yymmdd(dt:datetime):
    """Convert a datetime object to a string in the format 'YYMMDD'."""
    return dt.strftime('%y%m%d')