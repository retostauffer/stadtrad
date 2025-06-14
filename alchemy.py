


from bikedb import *
import datetime as dt

xxx = int(dt.datetime.now(dt.timezone.utc).timestamp())

db = BikeDB()

Places = Places(db)
Bikes  = Bikes(db)

db.create_all()

Places.insert_or_ignore(_id = 1, timestamp = xxx, lon = 99.11, lat = -134.3)
Places.insert_or_ignore(_id = 1, timestamp = xxx, lon = 99.11, lat = 343434.34)
Places.insert_or_ignore(_id = 1, timestamp = xxx, lon = 99.11, lat = 343434.34)
Places.insert_or_ignore(_id = 1, timestamp = xxx, lon = 99.11, lat = 343434.34)
Places.insert_or_ignore(_id = 1, timestamp = xxx, lon = 99.11, lat = 343434.34)

Bikes.insert_or_ignore(timestamp = xxx,
             number = 111, bike_type = 150, active = True,
             state = "ok", place_id = 2002002)
Bikes.insert_or_ignore(timestamp = xxx,
             number = 666, bike_type = 150, active = True,
             state = "ok", place_id = 2002002)
