


import os, sys
import json
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
    for file in glob("**", root_dir = dir, recursive = True):
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
    db     = BikeDB()
    Places = Places(db)
    Bikes  = Bikes(db)
    db.create_all()

    # Searching for available files in the live folder
    files,timestamps = get_json_files(cnf.livedir, cnf.domain)
    if len(files) > 0:
        print(f"Found {len(files)} json files to process in {cnf.livedir}")

        for i in range(len(files)):
            # Parsing the file
            print(f"Reading file \"{files[i]}\"")
            with open(files[i], "r") as fid: x = "".join(fid.readlines())
            x = json.loads(x)
            if not "places" in x.keys() or not "bikes" in x.keys():
                raise Exception("not found 'places' or 'bikes' in parsed json data")

            # Inserting places
            print(f"  Found {len(x['places'])} places to be inserted/updated")
            tmp = []
            for rec in x["places"]:
                tmp.append(dict(id = rec["uid"],
                                timestamp = timestamps[i],
                                lon = rec["lng"],
                                lat = rec["lat"]))
            Places.bulk_insert_or_ignore(tmp)

            # Inserting places
            print(f"  Found {len(x['bikes'])} bikes to be inserted/updated")
            tmp = []
            for rec in x["bikes"]:
                tmp.append(dict(timestamp = timestamps[i],
                                number    = int(rec["number"]),
                                bike_type = rec["bike_type"],
                                active    = rec["active"],
                                state     = rec["state"],
                                place_id  = rec["place_id"]))
            Bikes.bulk_insert_or_ignore(tmp)
