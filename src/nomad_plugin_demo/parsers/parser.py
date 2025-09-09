from typing import (
    TYPE_CHECKING,
)
from nomad_plugin_demo.schema_packages.schema_package import NewSchemaPackage
import pandas as pd

from nomad.datamodel.datamodel import (
    EntryArchive,
)

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
import h5py

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

        allowed_keys = [
            "Datum",
            "Set_aktuell",
            "p_Luft_bar_ein",
            "Set_Kommentar",
            "Strom_I___A",
            "U1",
        ]

        dataframe = clean_dataframe_columns(dataframe)
        # remove all columns that were unneeded
        dataframe = dataframe[allowed_keys]

        dataframe["Datum"] = pd.to_datetime(
            dataframe["Datum"], format=datetime_format
        ).dt.strftime(datetime_format)

        upload_id = str(uuid.uuid4())
        upload_id_first_chars = upload_id[:2]

        archive.metadata.upload_id = upload_id
        archive.metadata.entry_id = "h5_dataset"
        archive.data = NewSchemaPackage()

        child_archive = EntryArchive()
        archive.data.name = os.path.basename(mainfile)

        data_dict = dataframe[dataframe.columns].to_dict(orient="records")

        filename = f"{Path(mainfile).stem}.h5"

        child_archive.data = NewSchemaPackage()
        child_archive.data.hdf5_file = filename

        # even though this variable is not used, the line is necessary
        # to create the correct directory structure
        StagingUploadFiles(upload_id=upload_id, create=True)

        archive.data.value = filename

        # hack: create empty hdf5 file first
        # the problem is, initially the HDF5Reference library tries to open the file in read mode
        # which fails if the file does not exist yet
        # so we create an empty file first, then we can write to it

        # when a dataset is created, the format is:
        # .volumes/fs/staging/{first two chars of upload_id}/{upload_id}/raw/{filename}.h5
        # so for example:
        # .volumes/fs/staging/ab/abcdef12-3456-7890/raw/data.h5
        hdf5_filename = (
            f".volumes/fs/staging/{upload_id_first_chars}/{upload_id}/raw/{filename}"
        )

        with h5py.File(hdf5_filename, "w"):
            pass

        # now write to file. This is only for displaying data in the hdf5 viewer
        with archive.m_context.raw_file(filename, "w") as newfile:

            archive.data.hdf5_filename = newfile.name

            with h5py.File(newfile.name, "w") as hdf:
                for key in allowed_keys:

                    values = [item[key] for item in data_dict]
                    group = hdf.create_group(key)
                    group.create_dataset("value", data=values)

                    group.attrs["signal"] = "value"

        # finally, set data in archive
        for key in allowed_keys:
            values = [item[key] for item in data_dict]
            dataset_path = f"/uploads/{upload_id}/raw/{filename}#/{key}/value"

            HDF5Reference.write_dataset(archive, values, dataset_path)
            try:
                setattr(archive.data, key, dataset_path)
            except:
                pass

        # with h5py.File(hdf5_filename, "r") as f:
        #     alist = []
        #     ls = list(f.keys())
        #     for key in ls:
        #         group = f.get(key)
        #         print(group["value"][()])
