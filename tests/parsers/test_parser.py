import logging

from nomad.datamodel import EntryArchive

from nomad_plugin_demo.parsers.parser import NewParser


def test_parse_file():
    parser = NewParser()
    archive = EntryArchive()
    parser.parse('tests/data/BZ011_Rohdaten.dat', archive, logging.getLogger())
    assert 'Datum' in (archive.data.quantities)[0] and archive.data.name is not None
