from typing import (
    TYPE_CHECKING,
)
from nomad_plugin_demo.schema_packages.schema_package import Entry, NewSchemaPackage
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
import os

configuration = config.get_plugin_entry_point(
    "nomad_plugin_demo.parsers:parser_entry_point"
)


def clean_dataframe_columns(dataframe):
    dataframe.columns = [
        column.replace(" ", "_").replace("/", "_") for column in dataframe.columns
    ]
    return dataframe


class NewParser(MatchingParser):
    def parse(
        self,
        mainfile: str,
        archive: "EntryArchive",
        logger: "BoundLogger",
        child_archives: dict[str, "EntryArchive"] = None,
    ) -> None:
        if logger is not None:
            logger.info("NewParser.parse", parameter=configuration.parameter)
        datetime_format = "%d.%m.%y %H:%M:%S"
        dataframe = pd.read_csv(mainfile, sep="\t", decimal=",")

        dataframe["Datum"] = pd.to_datetime(
            dataframe["Datum"], format=datetime_format
        ).dt.strftime(datetime_format)

        dataframe = clean_dataframe_columns(dataframe)

        archive.data = NewSchemaPackage()

        archive.data.name = os.path.basename(mainfile)
        data_dict = dataframe[dataframe.columns].to_dict(orient="records")
        for item in data_dict:
            entry = Entry()
            for key, value in item.items():
                setattr(entry, key, value)

            archive.data.entries.append(entry)
