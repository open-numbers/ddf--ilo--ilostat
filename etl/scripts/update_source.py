# -*- coding: utf-8 -*-

from ddf_utils.factory import ilo


def main():
    md = ilo.load_metadata()
    indicators = md[md['freq'] == 'A']['id'].values

    ilo.bulk_download('../source/', indicators)


if __name__ == '__main__':
    main()
