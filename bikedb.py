
from sqlalchemy import create_engine, MetaData
from sqlalchemy import select, func
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
            Column("id",        Integer,     primary_key = True),
            Column("timestamp", Integer,     nullable = True),
            Column("name",      String(100), nullable = False),
            Column("lon",       Float,       nullable = False),
            Column("lat",       Float,       nullable = False)
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

    def bulk_insert(self, rows, ignore_on_duplicate = True):
        """bulk_insert(rows, ignore_on_duplicate = True)

        Params
        ======
        rows : list of dict
            list of dictionaries defining the rows.
        ignore_on_duplicate : bool
            if set True (default) 'insert or ignore' is used.
        """
        if not isinstance(rows, list): raise TypeError("'rows' must be list")

        stmt = self.__insert(self.table).values(rows)
        # Adding ignore rules
        if ignore_on_duplicate:
            if self.db.engine.dialect.name == "sqlite":
                stmt = stmt.prefix_with("OR IGNORE")
            elif self.db.engine.dialect.name == "mysql":
                stmt = stmt.prefix_with("IGNORE")
        with self.db.begin() as con:
            result = con.execute(stmt)


# -------------------------------------------------------------------
# Rentals table; how many bikes are available at a certain station
# at a specific type. Separated from 'Places' to avoid storing the
# location each time.
# -------------------------------------------------------------------
class Rentals:

    def __init__(self, db: BikeDB):
        """Places(db)

        Handler for 'number of available rental bikes'.

        Params
        ======
        db : BikeDB
            database handler (SQLAlchemy).
        """
        if not isinstance(db, BikeDB):
            raise TypeError("'db' must be a BikeDB object")

        self.db = db
        self.table = Table("rentals", db.metadata,
            Column("place_id",  ForeignKey("places.id"), nullable = False),
            Column("timestamp", Integer, nullable = False),
            Column("bikes",     Integer, nullable = False),
            Column("available", Integer, nullable = False),
            UniqueConstraint("place_id", "timestamp", name = "rentals_index_place_id_timestamp")
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

    def bulk_insert(self, rows, ignore_on_duplicate = True):
        """bulk_insert(rows, ignore_on_duplicate = True)

        Params
        ======
        rows : list of dict
            list of dictionaries defining the rows.
        ignore_on_duplicate : bool
            if set True (default) 'insert or ignore' is used.
        """
        if not isinstance(rows, list): raise TypeError("'rows' must be list")

        stmt = self.__insert(self.table).values(rows)
        # Adding ignore rules
        if ignore_on_duplicate:
            if self.db.engine.dialect.name == "sqlite":
                stmt = stmt.prefix_with("OR IGNORE")
            elif self.db.engine.dialect.name == "mysql":
                stmt = stmt.prefix_with("IGNORE")
        with self.db.begin() as con:
            result = con.execute(stmt)



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
            Column("first_seen", Integer,                 nullable = False),
            Column("last_seen",  Integer,                 nullable = False),
            Column("number",     Integer,                 nullable = False),
            Column("bike_type",  Integer,                 nullable = False),
            Column("place_id",   ForeignKey("places.id"), nullable = False),
            Column("active",     Boolean,                 nullable = False),
            Column("state",      String(5),               nullable = False),

            UniqueConstraint("first_seen", "number", name = "bikes_index_first_seen_number")
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

    def bulk_insert_or_update(self, rows):
        """bulk_insert_or_ignore(rows)

        Params
        ======
        rows : list of dict
            list of dictionaries defining the rows.
        """
        stmt = self.__insert(self.table).values(rows)

        # Adding update rulez
        if self.db.engine.dialect.name == "sqlite":
            stmt = stmt.on_conflict_do_update(
                       index_elements = ["first_seen", "number"],
                       set_ = dict(last_seen = stmt.excluded.last_seen)
                   )
        elif self.db.engine.dialect.name == "mysql":
            stmt = stmt.on_duplicate_key_update(
                       last_seen = stmt.inserted.last_seen
                   )
        else:
            raise NotImplementedError(f"self.db {db.engine.dialect.name} not implemented")


        with self.db.begin() as con:
            result = con.execute(stmt)


    def latest_entry(self):
        """latest_entry()

        Return
        ======
        None, int : Returns None if the database is currently empty, else
        the latest (max) timestamp from latest (newest) record in the database.
        """
        stmt = select(func.max(self.table.c.last_seen))
        with self.db.engine.begin() as con:
            res = con.execute(stmt).scalar_one_or_none()
        return res

    def get_previous_records(self):
        """get_previous_records()

        Loads the latest record for each bike used to check if the status
        of the bike changed since the last data point we stored.

        Return
        ======
        dict : The keys of the dictionary corresponds to the bike number (str),
        the items contain the last recorded status.
        """
        stmt = select(self.table.c.first_seen,
                      self.table.c.number,
                      self.table.c.bike_type,
                      self.table.c.place_id,
                      self.table.c.active,
                      self.table.c.state,
                      func.row_number().over(
                          partition_by = self.table.c.number,
                          order_by     = self.table.c.first_seen.desc()
                      ).label("rnk")
                     ).subquery()
        latest = select(stmt).where(stmt.c.rnk == 1)
        res    = {}
        with self.db.engine.begin() as con:
            tmp = con.execute(latest).mappings().all()

        for rec in tmp: res[str(rec["number"])] = rec
        return res

