# -*- coding: utf-8 -*-

import os
from glob import glob
from ddf_utils.factory import ILOLoader


def remove_source():
    for f in glob("../source/*csv.gz"):
        os.remove(f)


def main():
    ilo = ILOLoader()
    md = ilo.load_metadata()
    indicators = md[md['freq'] == 'A']['id'].values

    ilo.bulk_download('../source/', indicators)


if __name__ == '__main__':
    remove_source()
    main()
