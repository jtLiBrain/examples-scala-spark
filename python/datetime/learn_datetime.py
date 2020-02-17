from datetime import datetime, timedelta
import time

dateTimeStr = "2019-09-12 11:28:00"

# 1. parsestring to a datetime object
dateTime = datetime.strptime(dateTimeStr, "%Y-%m-%d %H:%M:%S")
print(type(dateTime))

# 2. format datetime object to string
formatedDateTimeStr = dateTime.strftime("%Y-%m-%d 12:00:00")
print(type(formatedDateTimeStr))

# 3
dateTimeAfter1Day = dateTime + timedelta(days=1)
print(dateTimeAfter1Day)

# 4
currentTime = datetime.now()
print(currentTime)

# 5
dateTime2 = datetime(2019, 9, 12, 11)
print(dateTime2)