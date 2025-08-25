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
from nomad.parsing.parser import MatchingParser
import os
from pathlib import Path
from nomad.datamodel.hdf5 import HDF5Reference
from nomad.files import StagingUploadFiles
import uuid

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
        upload_id = str(uuid.uuid4())
        archive.metadata.upload_id = upload_id
        archive.metadata.entry_id = "h5_datafile"
        archive.data = NewSchemaPackage()

        archive.data.name = os.path.basename(mainfile)

        data_dict = dataframe[dataframe.columns].to_dict(orient="records")

        stem = Path(mainfile).stem

        upload_files = StagingUploadFiles(upload_id=upload_id, create=True)

        path = f"{stem}.h5"
        archive.data.value = path

        # entry = Entry()
        # for key, value in data_dict[0].items():

        #     values = [item[key] for item in data_dict if key in item]
        #     dataset_path = f"{path}#{key}/value"
        #     HDF5Reference.write_dataset(archive, values, dataset_path)
        #     setattr(entry, key, dataset_path)

        # archive.data.entries.append(entry)

        entry = Entry()
        for key in data_dict[0]:
            values = [item[key] for item in data_dict]
            dataset_path = f"/uploads/{upload_id}/raw/{path}#{key}/value"
            HDF5Reference.write_dataset(archive, values, dataset_path)
            setattr(entry, key, dataset_path)

        archive.data.entries.append(entry)
