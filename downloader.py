#!/usr/bin/env python3
# -------------------------------------------------------------------
# Downloading, storing, and archiving nextbike data
# -------------------------------------------------------------------

import sys
import os
import configparser
import datetime as dt

from requests import get
import logging
logging.basicConfig(stream = sys.stdout, level = logging.INFO)

def get_dir_today(dir):
    """get_dir_today(dir)

    Params
    ======
    dir : str
        Path to the parent directory

    Return
    ======
    str : Returns the path to the folder to store todays data.
    """
    if not isinstance(dir, str): raise TypeError("'dir' must be str")
    x = dt.datetime.now(dt.timezone.utc)
    return os.path.join(dir, x.strftime("%Y"), x.strftime("%m"), x.strftime("%d"))

def get_dir_yesterday(dir):
    """get_dir_yesterday(dir)

    Params
    ======
    dir : str
        Path to the parent directory

    Return
    ======
    str : Returns the path to the folder where we stored yesterdays data.
    """
    if not isinstance(dir, str): raise TypeError("'dir' must be str")
    x = dt.datetime.now(dt.timezone.utc) - dt.timedelta(1)
    return os.path.join(dir, x.strftime("%Y"), x.strftime("%m"), x.strftime("%d"))

def get_jsonfilename(dir, domain, fileext = "json"):
    """get_jsonfilename(dir, domain, fileext = "json")

    Params
    ======
    dir : str
        path to the parent directory
    domain : str
        added to the json file name returned.
    fileext : str
        File name extension, defaults to 'json'.

    Return
    ======
    str : Returns the json file name to store the (current) live data.
    The format is <dir>/YYYY/mm/dd/<timestamp>_<domain>.json by default.
    """
    if not isinstance(dir, str):     raise TypeError("'dir' must be str")
    if not isinstance(domain, str):  raise TypeError("'domain' must be str")
    if not isinstance(fileext, str): raise TypeError("'fileext' must be str")
    x = dt.datetime.now(dt.timezone.utc)
    return os.path.join(get_dir_today(dir),
                        f"{x.timestamp():.0f}_{domain}.{fileext}")

def archive_yesterday(indir, outdir, domain):
    """archive_yesterday(indir, outdir, domain)

    Params
    ======
    indir : str
        name of the directory with the live data (json files).
    outdir : str
        name of the directory where to store the zip file.
    domain : str
        added to the json file name returned.

    Return
    ======
    Archives the data and removes the original unpacked files.
    """
    if not isinstance(indir, str):     raise TypeError("'indir' must be str")
    if not isinstance(outdir, str):    raise TypeError("'outdir' must be str")

    logging.info("Archiving data from yesterday")
    x = dt.datetime.now(dt.timezone.utc) - dt.timedelta(1)
    indir   = os.path.join(indir, x.strftime("%Y"), x.strftime("%m"), x.strftime("%d"))
    if not os.path.isdir(indir):
        raise Exception(f"whoops, trying to archive \"{indir}\" but that does not exist.")
    zipfile = os.path.join(outdir, f"{x.strftime('%Y-%m-%d')}_{domain}")
    logging.info(f"zipping {indir} to {zipfile}")

    import shutil
    shutil.make_archive(zipfile, "zip", indir)
    shutil.rmtree(indir)

class bikeconfig(configparser.ConfigParser):
    def __init__(self, file):
        if not isinstance(file, str):
            raise TypeError("'file' must be str")
        if not os.path.isfile(file):
            raise FileNotFoundError(f"'file = \"{file}\" not found")

        super(bikeconfig, self).__init__(allow_no_value = True)

        self.read(file)
        self.baseurl = self.get("general", "baseurl")
        self.domain  = self.get("general", "domain")
        self.country = self.get("general", "country")

        self.livedir = self.get("general", "livedir")
        if not os.path.isdir(self.livedir):
            try:
                os.makedirs(self.livedir)
            except Exception as e:
                raise Exception(e)

        self.archivedir = self.get("general", "archivedir")
        if not os.path.isdir(self.archivedir):
            try:
                os.makedirs(self.archivedir)
            except Exception as e:
                raise Exception(e)


if __name__ == "__main__":

    cnf = bikeconfig("innsbruck.cnf")

    # Getting current time to build the output file name (before calling API)
    jsonfile = get_jsonfilename(cnf.livedir, cnf.domain)
    logging.info(f"JSON file name: {jsonfile}")

    logging.info("Calling nextbike API")
    req = get(cnf.baseurl, params = dict(domains = cnf.domain, countries = cnf.country))
    if not req.status_code // 100 == 2:
        raise Exception("API request was not successful")

    # Else we can continue
    logging.info("Request successful, writing to file")

    # Create folder structure (if needed)
    today_dir     = get_dir_today(cnf.livedir)
    logging.info(f"Today dir:      {today_dir}")

    # Make sure the directory where we try to store the data does exist
    if not os.path.isdir(os.path.dirname(jsonfile)):
        try:
            os.makedirs(os.path.dirname(jsonfile))
        except Exception as e:
            raise Exception(e)

    # Save data
    with open(jsonfile, "w") as fid:
        fid.write(req.text)

    # Archiving data (if needed)
    yesterday_dir = get_dir_yesterday(cnf.livedir)
    logging.info(f"Yesterday dir:  {yesterday_dir}")

    if os.path.isdir(yesterday_dir):
        archive_yesterday(cnf.livedir, cnf.archivedir, cnf.domain)







