
from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column, ForeignKey, UniqueConstraint
from sqlalchemy import Integer, Float, Boolean, String


# -------------------------------------------------------------------
# Database handler; SQLAlchemy
# -------------------------------------------------------------------
class BikeDB:

    def __init__(self, connection_string = "sqlite+pysqlite:///test.db", echo = False):
        """BikeDB(connection_string = "sqlite+pysqlite:///test.db")

        Params
        ======
        connection_string : str
            connection string for SQLAlchemy. Defaults to
            SQLite 'test.db' for now.
        echo : bool
            Forwarded to `sqlalchemy.create_engine()`, defaults to `False`.

        Return
        ======
        No return, initializes an object of class 'BikeDB'.
        """

        if not isinstance(connection_string, str):
            raise TypeError("'connection_string' must be str")
        if not isinstance(echo, bool):
            raise TypeError("'echo' must be bool")

        try:
            self.engine   = create_engine(connection_string, echo = echo)
        except Exception as e:
            raise Exception(e)
        self.metadata = MetaData()

    def create_all(self):
        """create_all()

        Creates the database tables (if not existing).
        """
        self.metadata.create_all(self.engine)

    def begin(self):
        """begin()

        Construct new connection.

        Return
        ======
        RootTransaction : Returns root transaction handler.
        """
        return self.engine.begin() # Context-managed connection


# -------------------------------------------------------------------
# Places handler
# -------------------------------------------------------------------
class Places:

    def __init__(self, db: BikeDB):
        """Places(db)

        Handler for 'places'.

        Params
        ======
        db : BikeDB
            database handler (SQLAlchemy).
        """
        if not isinstance(db, BikeDB):
            raise TypeError("'db' must be a BikeDB object")

        self.db = db
        self.table = Table("places", db.metadata,
            Column("id",        Integer, nullable = False),
            Column("timestamp", Integer, nullable = False),
            Column("lon",       Float,   nullable = False),
            Column("lat",       Float,   nullable = False),

            UniqueConstraint("id", "lon", "lat", name = "places_index_id_lon_lat")
        )

        # Define insert method (diaclect dependent)
        if db.engine.dialect.name == "sqlite":
            from sqlalchemy.dialects.sqlite import insert
        elif db.engine.dialect.name == "mysql":
            from sqlalchemy.dialects.mysql import insert
        else:
            raise NotImplementedError(f"self.db {db.engine.dialect.name} not implemented")
        # Insert method used in .insert(), .upsert() etc
        self.__insert = insert

    def _compile(self, timestamp, _id: int, lon: float, lat: float):
        """Helper function used by both .insert() and .upsert()."""
        if not isinstance(timestamp, int): raise TypeError("'timestamp' must be int")
        if not isinstance(_id, int):       raise TypeError("'_id' must be int")
        if not isinstance(lon, float):     raise TypeError("'lon' must be int")
        if not isinstance(lat, float):     raise TypeError("'lat' must be int")
        return self.__insert(self.table).values(timestamp = timestamp, id = _id,
                                                lon = lon, lat = lat)

    def insert(self, _id: int, timestamp: int, lon: float, lat: float):
        """insert(_id, timestamp, lon, lat)

        Inserts a place into the databse.

        Params
        ======
        _id : int
            Unique place identifier.
        timestamp : int
            Unix timestamp.
        lon : float
            Longitude (deg East) of the place.
        lat : float
            Latitude (deg Norty) of the place.
        """
        stmt = self._compile(_id, timestamp, lon, lat)
        with self.db.begin() as con:
            result = con.execute(stmt)


    def insert_or_ignore(self, _id: int, timestamp: int, lon: float, lat: float):
        """insert_or_ignore(_id, timestamp, lon, lat)

        Inserts a place into the databse. If existing (see table constraints)
        the insert will be ignored.

        Params
        ======
        _id : int
            Unique place identifier.
        timestamp : int
            Unix timestamp.
        lon : float
            Longitude (deg East) of the place.
        lat : float
            Latitude (deg Norty) of the place.
        """
        stmt = self._compile(_id, timestamp, lon, lat)
        # Adding ignore rules
        if self.db.engine.dialect.name == "sqlite":
            stmt = stmt.prefix_with("OR IGNORE")
        elif self.db.engine.dialect.name == "mysql":
            stmt = stmt.prefix_with("IGNORE")
        with self.db.begin() as con:
            result = con.execute(stmt)

    ####def upsert(self, _id: int, timestamp: int, lon: float, lat: float):
    ####    """insert(_id, timestamp, lon, lat)

    ####    Insert a place into the database or update an existing
    ####    entry. On update, `lon` and `lat` will be updated.

    ####    Params
    ####    ======
    ####    _id : int
    ####        Unique place identifier.
    ####    timestamp : int
    ####        Unix timestamp.
    ####    lon : float
    ####        Longitude (deg East) of the place.
    ####    lat : float
    ####        Latitude (deg Norty) of the place.
    ####    """
    ####    stmt = self._compile(_id, timestamp, lon, lat)
    ####    # Adding upsert rules
    ####    if self.db.engine.dialect.name == "sqlite":
    ####        stmt = stmt.on_conflict_do_update(
    ####                    index_elements = ["id"],
    ####                    set_ = dict(lon = stmt.excluded.lon, lat = stmt.excluded.lat)
    ####               )
    ####    elif self.db.engine.dialect.name == "mysql":
    ####        stmt = stmt.on_duplicate_key_update(
    ####                    lon = stmt.inserted.lon,
    ####                    lat = stmt.inserted.lat
    ####               )
    ####    else:
    ####        raise NotImplementedError(f"engine {self.db.dialect.name} not implemented")

    ####    with self.db.begin() as con:
    ####        result = con.execute(stmt)



# -------------------------------------------------------------------
# Bikes handler
# -------------------------------------------------------------------
class Bikes:

    def __init__(self, db: BikeDB):
        """Bikes(db)

        Handler for 'bikes'.

        Params
        ======
        db : BikeDB
            database handler (SQLAlchemy).
        """
        if not isinstance(db, BikeDB):
            raise TypeError("'db' must be a BikeDB object")

        self.db = db
        self.table  = Table("bikes", self.db.metadata,
            Column("timestamp", Integer,                 nullable = False),
            Column("number",    Integer,                 nullable = False),
            Column("bike_type", Integer,                 nullable = False),
            Column("place_id",  ForeignKey("places.id"), nullable = False),
            Column("active",    Boolean,                 nullable = False),
            Column("state",     String(5),               nullable = False),

            UniqueConstraint("timestamp", "number", name = "bikes_index_timestamp_number")
        )

        # Define insert method (diaclect dependent)
        if db.engine.dialect.name == "sqlite":
            from sqlalchemy.dialects.sqlite import insert
        elif db.engine.dialect.name == "mysql":
            from sqlalchemy.dialects.mysql import insert
        else:
            raise NotImplementedError(f"self.db {db.engine.dialect.name} not implemented")
        # Insert method used in .insert(), .upsert() etc
        self.__insert = insert

    def _compile(self, timestamp: int, number: int, bike_type: int,
                 place_id: int, active: bool, state: str):
        """Helper function used by both .insert() and .upsert()."""
        if not isinstance(timestamp, int): raise TypeError("'timestamp' must be int")
        if not isinstance(number, int):    raise TypeError("'number' must be int")
        if not isinstance(bike_type, int): raise TypeError("'bike_type' must be int")
        if not isinstance(place_id, int):  raise TypeError("'place_id' must be int")
        if not isinstance(active, bool):   raise TypeError("'active' must be bool")
        if not isinstance(state, str):     raise TypeError("'state' must be str")
        return self.__insert(self.table).values(timestamp = timestamp, number = number,
                                                bike_type = bike_type, place_id = place_id,
                                                active = active, state = state)

    def insert(self, timestamp: int, number: int, bike_type: int,
               place_id: int, active: bool, state: str):
        """insert(timestamp, number, bike_type, place_id, active, state)

        Inserts a bike into the databse.

        Params
        ======
        timestamp : int
            Time information (Unix time stamp).
        number : int
            Identifier of the bike.
        bike_type: int
            Type of bike.
        place_id : int
            Identifier of the place the bike is located.
        active : bool
            Whether the bike is currently active or not.
        status : str
            Status of the bike.
        """
        stmt = self._compile(timestamp, number, bike_type, place_id, active, state)
        with self.db.begin() as con:
            result = con.execute(stmt)

    def insert_or_ignore(self, timestamp: int, number: int, bike_type: int,
               place_id: int, active: bool, state: str):
        """insert_or_ignore(timestamp, number, bike_type, place_id, active, state)

        Inserts a bike into the databse, insert is ignored if the 
        entry already exists (see table constraint).

        Params
        ======
        timestamp : int
            Time information (Unix time stamp).
        number : int
            Identifier of the bike.
        bike_type: int
            Type of bike.
        place_id : int
            Identifier of the place the bike is located.
        active : bool
            Whether the bike is currently active or not.
        status : str
            Status of the bike.
        """
        stmt = self._compile(timestamp, number, bike_type, place_id, active, state)
        # Adding ignore rules
        if self.db.engine.dialect.name == "sqlite":
            stmt = stmt.prefix_with("OR IGNORE")
        elif self.db.engine.dialect.name == "mysql":
            stmt = stmt.prefix_with("IGNORE")
        with self.db.begin() as con:
            result = con.execute(stmt)

