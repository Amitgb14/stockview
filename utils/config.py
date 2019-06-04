import os
import sys
import yaml


def config(filename):
    if not os.path.isfile(filename):
        print("File {} not exists.".format(filename))
        sys.exit(1)

    with open(filename, "r") as f:
        conf = yaml.load(f, Loader=yaml.Loader)
    return conf