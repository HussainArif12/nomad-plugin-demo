from typing import (
    TYPE_CHECKING,
)
from nomad_plugin_demo.schema_packages.schema_package import NewSchemaPackage
import pandas as pd

if TYPE_CHECKING:
    from nomad.datamodel.datamodel import (
        EntryArchive,
    )
    from structlog.stdlib import (
        BoundLogger,
    )

from nomad.config import config
from nomad.datamodel.metainfo.workflow import Workflow
from nomad.parsing.parser import MatchingParser

configuration = config.get_plugin_entry_point(
    'nomad_plugin_demo.parsers:parser_entry_point'
)


class NewParser(MatchingParser):
    def parse(
        self,
        mainfile: str,
        archive: 'EntryArchive',
        logger: 'BoundLogger',
        child_archives: dict[str, 'EntryArchive'] = None,
    ) -> None:
        logger.info('NewParser.parse', parameter=configuration.parameter)
        datetime_format = '%d.%m.%y %H:%M:%S'
        dataframe = pd.read_csv(mainfile, sep='\t', decimal=',')
        dataframe['Datum'] = pd.to_datetime(
            dataframe['Datum'], format=datetime_format
        ).dt.strftime(datetime_format)
        archive.data = NewSchemaPackage()

        archive.data.quantities = dataframe[
            [
                'Datum',
                'Kommentar',
                'Set aktuell',
                'Set Kommentar',
                'U1',
                'MW p Ali Kat aus',
                'V Luft aus',
                'p_Luft/bar_ein',
                'T_Luft_ein',
                'Strom I / A',
            ]
        ].to_dict(orient='records')
