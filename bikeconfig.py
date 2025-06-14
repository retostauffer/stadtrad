
import configparser
import os

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
