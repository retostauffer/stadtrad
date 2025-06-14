


from bikedb import *

db = BikeDB()

Places = Places(db)

db.create_all()

Places.insert(_id = 1, lon = 99.11, lat = -134.3)
Places.upsert(_id = 1, lon = 99.11, lat = 343434.34)

