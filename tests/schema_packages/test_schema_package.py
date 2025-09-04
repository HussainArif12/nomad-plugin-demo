import os.path

from nomad.client import normalize_all, parse


def test_schema_package():
    test_file = os.path.join("tests", "data", "BZ011_Rohdaten.dat")
    entry_archive = parse(test_file)[0]
    normalize_all(entry_archive)
    assert (entry_archive.data.Datum) is not None
