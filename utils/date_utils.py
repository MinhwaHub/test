import sys
from datetime import datetime, timedelta


def get_date():
    today = datetime.now()
    start_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = (today).strftime("%Y-%m-%d")

    try:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    except:
        pass
    print(start_date, end_date)
    return start_date, end_date
