
from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column, ForeignKey
from sqlalchemy import Integer, Float, Boolean, String

class BikeDB:

    def __init__(self, connection_string = "sqlite+pysqlite:///test.db"):
        """BikeDB(connection_string = "sqlite+pysqlite:///test.db")

        Params
        ======
        connection_string : str
            connection string for SQLAlchemy. Defaults to
            SQLite 'test.db' for now.

        Return
        ======
        No return, initializes an object of class 'BikeDB'.
        """

        if not isinstance(connection_string, str):
            raise TypeError("'connection_string' must be str")

        try:
            self.engine   = create_engine(connection_string, echo=True)
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


class Places:

    def __init__(self, db: BikeDB):
        if not isinstance(db, BikeDB):
            raise TypeError("'db' must be a BikeDB object")
        self.db = db
        self.table = Table("places", db.metadata,
            Column("id",  Integer, primary_key = True),
            Column("lon", Float,   nullable = False),
            Column("lat", Float,   nullable = False)
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

    def _compile(self, _id: int, lon: float, lat: float):
        if not isinstance(_id, int):   raise TypeError("'id' must be int")
        if not isinstance(lon, float): raise TypeError("'lon' must be int")
        if not isinstance(lat, float): raise TypeError("'lat' must be int")
        return self.__insert(self.table).values(id = _id, lon = lon, lat = lat)

    #def insert(self, _id: int, lon: float, lat: float):
    def insert(self, _id: int, lon: float, lat: float):
        stmt = self._compile(_id, lon, lat)
        with self.db.begin() as con:
            result = con.execute(stmt)

    def upsert(self, _id: int, lon: float, lat: float):
        stmt = self._compile(_id, lon, lat)
        # Adding upsert rules
        if self.db.engine.dialect.name == "sqlite":
            stmt = stmt.on_conflict_do_update(
                        index_elements = ["id"],
                        set_ = dict(lon = stmt.excluded.lon, lat = stmt.excluded.lat)
                   )
        elif self.db.engine.dialect.name == "mysql":
            stmt = stmt.on_duplicate_key_update(
                        lon = stmt.inserted.lon,
                        lat = stmt.inserted.lat
                   )
        else:
            raise NotImplementedError(f"engine {self.db.dialect.name} not implemented")

        with self.db.begin() as con:
            result = con.execute(stmt)


#stmt = insert(places_table).values(id = 1, lon = 1.23, lat = 5.55)
#        stmt = self.insert(self.table).values(id = 1, lon = 8888.99, lat = 5.55)


#        # Define tables, create (if needed)
#        self._define_table_places()
#        self._define_table_bikes()
#        self._meta.create_all(self._engine)
#
#    def _define_table_places(self):
#        self._places = Table("places", self._meta,
#            Column("id",  Integer, primary_key = True),
#            Column("lon", Float, nullable = False),
#            Column("lat", Float, nullable = False)
#        )
#    def _define_table_bikes(self):
#        self._bikes = Table("bikes", self._meta,
#            Column("number", Integer, nullable = False),
#            Column("active", Boolean, nullable = False),
#            Column("state", String(5), nullable = False),
#            Column("place_id", ForeignKey("places.id"), nullable = False)
#        )
#
#    ##if engine.dialect.name == "sqlite":
#    ##    from sqlalchemy.dialects.sqlite import insert
#    ##elif engine.dialect.name == "mysql":
#    ##    from sqlalchemy.dialects.mysql import insert
#    ##else:
#    ##    raise NotImplementedError(f"engine {engine.dialect.name} not implemented")



###stmt = insert(places_table).values(id = 1, lon = 1.23, lat = 5.55)
##stmt = insert(places_table).values(id = 1, lon = 8888.99, lat = 5.55)
##if engine.dialect.name == "sqlite":
##    stmt = stmt.on_conflict_do_update(
##                index_elements = ["id"],
##                set_ = dict(lon = stmt.excluded.lon, lat = stmt.excluded.lat)
##           )
##elif engine.dialect.name == "mysql":
##    stmt = stmt.on_duplicate_key_update(
##                lon = stmt.inserted.lon,
##                lat = stmt.inserted.lat
##           )
##else:
##    raise NotImplementedError(f"engine {engine.dialect.name} not implemented")
##
##stmt.compile()
##result = con.execute(stmt)
##print(result)
##con.commit()


