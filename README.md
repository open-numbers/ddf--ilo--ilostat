# ddf--ilo--ilostat

ILO database of labour statistics in DDF format

## Indicators

All Annual Indicators from ILOSTAT are in this dataset.

Check the [ILOSTAT Website][1] for details.

[1]: http://www.ilo.org/ilostat/

## Versions

### Revision history

## Notes

Sometimes multiple observation occurred for a datapoint, from
different sources.  In this case, we consult the metadata for
[source][2] and [note_source][3] in ILO bulk download website and
assume the ones in the beginning are better sources, and only keep
the best source for particular country in a indicator.

[2]: https://www.ilo.org/ilostat-files/WEB_bulk_download/dic/source_en.csv
[3]: https://www.ilo.org/ilostat-files/WEB_bulk_download/dic/note_source_en.csv
