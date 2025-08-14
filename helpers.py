from datetime import datetime

def yymmdd(dt:datetime):
    """Convert a datetime object to a string in the format 'YYMMDD'."""
    return dt.strftime('%y%m%d')