from datetime import datetime

print(datetime.today().strftime('%Y_%m_%d'))
print(datetime.strptime("2018-01-31", "%Y-%m-%d").strftime('%Y_%m_%d'))
print(datetime.strptime("2018-01-31", "%Y-%m-%d"))
