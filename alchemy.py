#!/usr/bin/env python3
# -------------------------------------------------------------------
# Parsing 'live' json files.
# -------------------------------------------------------------------

import os, sys
import json
import re
from bikedb import *
import datetime as dt

from bikeconfig import bikeconfig

import logging
logging.basicConfig(stream = sys.stdout, level = logging.WARNING)

def get_json_files(dir, domain):
    """get_json_files(dir, domain)

    Params
    ======
    dir : str
        Directory containing the json files.
    domain : str
        Domain used for file names.
    """
    if not isinstance(dir, str): raise TypeError("'dir' must be string")
    if not os.path.isdir(dir):   raise NotADirectoryError(f"\"{dir}\" does not exist")
    if not isinstance(domain, str): raise TypeError("'domain' must be string")

    from re import compile
    pat = compile(f".*\\/([0-9]+)_{domain}\\.json$")

    from glob import glob
    files      = []
    timestamps = []

    # Pattern the files must follow
    tmp = glob("**", root_dir = dir, recursive = True)
    tmp.sort()
    for file in tmp:
        tmp = pat.search(file)
        if not tmp: continue
        files.append(file)
        timestamps.append(int(tmp.group(1)))

    files = [os.path.join(dir, x) for x in files]
    return [files, timestamps]





# -------------------------------------------------------------------
# Main part
# -------------------------------------------------------------------
if __name__ == "__main__":

    # Reading config file
    cnf = bikeconfig("innsbruck.cnf")

    # Initializing/setting up database connection and data handler
    db      = BikeDB()
    Places  = Places(db)
    Rentals = Rentals(db)
    Bikes   = Bikes(db)
    db.create_all()

    # Used to check if the place is an official station
    # or just a BIKE left somewhere.
    no_station = re.compile("^BIKE.*")

    # Searching for available files in the live folder
    files,timestamps = get_json_files(cnf.livedir, cnf.domain)
    if len(files) > 0:
        print(f"Found {len(files)} json files to process in {cnf.livedir}")

        for i in range(len(files)):
            # We MUST process the files in order (oldest to newest) as else
            # the system how we store the 'bikes' information will not be
            # correct. Thus, loading the latest timestamp from the 'bikes'
            # table and ensure that timestamps[i] > last recorded entry.
            latest_entry = Bikes.latest_entry()
            if not latest_entry is None and latest_entry >= timestamps[i]:
                raise Exception(f"File {files[i]} older than latest processed data ({latest_entry}). Not allowed.")

            # Parsing the file
            print(f"Reading file \"{files[i]}\"")
            with open(files[i], "r") as fid: x = "".join(fid.readlines())
            x = json.loads(x)
            if not "places" in x.keys() or not "bikes" in x.keys():
                raise Exception("not found 'places' or 'bikes' in parsed json data")

            # Inserting places
            print(f"  Found {len(x['places'])} places to be inserted/updated")
            tmp_places  = []
            tmp_rentals = []
            for rec in x["places"]:
                tmp_places.append(dict(id        = rec["uid"],
                                       timestamp = timestamps[i] if no_station.match(rec["name"]) else None,
                                       name      = rec["name"],
                                       lon       = rec["lng"],
                                       lat       = rec["lat"]))

                tmp_rentals.append(dict(place_id  = rec["uid"],
                                        timestamp = timestamps[i],
                                        bikes     = rec["bikes"],
                                        available = rec["bikes_available_to_rent"]))
            Places.bulk_insert(tmp_places)
            Rentals.bulk_insert(tmp_rentals)

            # For each bike, extract the latest record used to check if a bike
            # status or position has changed since last time.
            previous = Bikes.get_previous_records()

            # Inserting places
            print(f"  Found {len(x['bikes'])} bikes to be inserted/updated")
            tmp_bikes = []
            for rec in x["bikes"]:
                # If the current bike is not in 'previous' (i.e., never seen before)
                # we can add it to the database; we do so by using timestamps[i] as
                # 'first_seen' which will create a new line.
                if not str(rec["number"]) in previous.keys():
                    first_seen = timestamps[i]
                # Else we check if the status of the bike has changed.
                # If not, 'first_seen' is taken from the previous record, forcing
                # the database to update the existing row (only updating 'last_seen' with
                # current timestamps[i]). However, if the status has changed
                # we use timestamps[i] as 'first_seen' triggering a new line in the table.
                else:
                    p = previous[str(rec["number"])]
                    first_seen = p["first_seen"]  # Default
                    if p["bike_type"] != rec["bike_type"]:
                        first_seen = timestamps[i] # Type changed
                    elif p["place_id"] != rec["place_id"]:
                        first_seen = timestamps[i] # Bike location changed
                    elif p["active"] != rec["active"]:
                        first_seen = timestamps[i] # Active flag changed
                    elif p["state"] != rec["state"]:
                        first_seen = timestamps[i] # Bike state changed

                # Append
                tmp = dict(first_seen = first_seen,
                           last_seen  = timestamps[i],
                           number     = int(rec["number"]),
                           bike_type  = rec["bike_type"],
                           active     = rec["active"],
                           state      = rec["state"],
                           place_id   = rec["place_id"])
                tmp_bikes.append(tmp)

            # Execute: Remember we have a unique constraint on 'number' and 'first_seen'
            # which controls whether or not a row is updated, or a new is added (when
            # the bike status changed).
            Bikes.bulk_insert_or_update(tmp_bikes)
            del tmp_bikes



