
class bikedb:
    from sqlalchemy import create_engine, MetaData

    _engine = None
    _meta   = MetaData()

    def __init__(self, connection_string = "sqlite+pysqlite:///test.db"):
        """bikedb(connection_string = "sqlite+pysqlite:///test.db")

        Params
        ======
        connection_string : str
            connection string for SQLAlchemy. Defaults to
            SQLite 'test.db' for now.

        Return
        ======
        No return, initializes an object of class 'bikedb'.
        """

        if not isinstance(connection_string, str):
            raise TypeError("'connection_string' must be str")

        from sqlalchemy import create_engine
        try:
            self._engine = create_engine(connection_string, echo=True)
        except Exception as e:
            raise Exception(e)

        # Define tables, create (if needed)
        self._define_table_places()
        self._define_table_bikes()
        self._meta.create_all(self._engine)

    def _define_table_places(self):
        from sqlalchemy import Table, Column
        from sqlalchemy import Integer, Float
        self._places = Table("places", self._meta,
            Column("id",  Integer, primary_key = True),
            Column("lon", Float, nullable = False),
            Column("lat", Float, nullable = False)
        )
    def _define_table_bikes(self):
        from sqlalchemy import Table, Column, ForeignKey
        from sqlalchemy import Integer, Boolean, String
        self._bikes = Table("bikes", self._meta,
            Column("number", Integer, nullable = False),
            Column("active", Boolean, nullable = False),
            Column("state", String(5), nullable = False),
            Column("place_id", ForeignKey("places.id"), nullable = False)
        )

    ##if engine.dialect.name == "sqlite":
    ##    from sqlalchemy.dialects.sqlite import insert
    ##elif engine.dialect.name == "mysql":
    ##    from sqlalchemy.dialects.mysql import insert
    ##else:
    ##    raise NotImplementedError(f"engine {engine.dialect.name} not implemented")



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


