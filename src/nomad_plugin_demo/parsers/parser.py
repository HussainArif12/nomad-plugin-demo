from time import sleep
from typing import (
    TYPE_CHECKING,
)
from nomad_plugin_demo.schema_packages.schema_package import NewSchemaPackage, Entries
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
import numpy as np
from nomad.datamodel.context import ClientContext
import yaml
import math
import json

configuration = config.get_plugin_entry_point(
    "nomad_plugin_demo.parsers:parser_entry_point"
)


def clean_dataframe_columns(dataframe):
    dataframe.columns = [
        column.replace(" ", "_").replace("/", "_") for column in dataframe.columns
    ]
    return dataframe


def nan_equal(a, b):
    """
    Compare two values with NaN values.
    """
    if isinstance(a, float) and isinstance(b, float):
        return a == b or (math.isnan(a) and math.isnan(b))
    elif isinstance(a, dict) and isinstance(b, dict):
        return dict_nan_equal(a, b)
    elif isinstance(a, list) and isinstance(b, list):
        return list_nan_equal(a, b)
    else:
        return a == b


def list_nan_equal(list1, list2):
    """
    Compare two lists with NaN values.
    """
    if len(list1) != len(list2):
        return False
    for a, b in zip(list1, list2):
        if not nan_equal(a, b):
            return False
    return True


def dict_nan_equal(dict1, dict2):
    """
    Compare two dictionaries with NaN values.
    """
    if set(dict1.keys()) != set(dict2.keys()):
        return False
    for key in dict1:
        if not nan_equal(dict1[key], dict2[key]):
            return False
    return True


def get_reference(upload_id, entry_id):
    return f"../uploads/{upload_id}/archive/{entry_id}"


def get_entry_id(upload_id, filename):
    from nomad.utils import hash

    return hash(upload_id, filename)


def get_hash_ref(upload_id, filename):
    return f"{get_reference(upload_id, get_entry_id(upload_id, filename))}#data"


def create_archive(
    entry_dict, context, filename, file_type, logger, *, overwrite: bool = False
):
    file_exists = context.raw_path_exists(filename)
    dicts_are_equal = None
    if isinstance(context, ClientContext):
        return None
    if file_exists:
        with context.raw_file(filename, "r") as file:
            existing_dict = yaml.safe_load(file)
            dicts_are_equal = dict_nan_equal(existing_dict, entry_dict)
    if not file_exists or overwrite or dicts_are_equal:
        with context.raw_file(filename, "w") as newfile:
            if file_type == "json":
                json.dump(entry_dict, newfile)
            elif file_type == "yaml":
                yaml.dump(entry_dict, newfile)
        context.upload.process_updated_raw_file(filename, allow_modify=True)
    elif file_exists and not overwrite and not dicts_are_equal:
        logger.error(
            f"{filename} archive file already exists. "
            f"You are trying to overwrite it with a different content. "
            f"To do so, remove the existing archive and click reprocess again."
        )
    return get_hash_ref(context.upload_id, filename)


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
        # upload_id = "test_upload"
        upload_id_first_chars = upload_id[:2]

        archive.metadata.upload_id = upload_id
        archive.metadata.entry_id = "h5_dataset"
        archive.data = NewSchemaPackage()

        archive.data.name = os.path.basename(mainfile)

        data_dict = dataframe[dataframe.columns].to_dict(orient="records")

        filename = f"{Path(mainfile).stem}.h5"

        # even though this variable is not used, the line is necessary
        # to create the correct directory structure
        upload_id = contx = archive.m_context.upload_id
        upload_id_first_chars = upload_id[:2]

        StagingUploadFiles(upload_id=upload_id, create=True)

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

        child_archives = {
            "experiment": EntryArchive(),
            "instrument": EntryArchive(),
            "process": EntryArchive(),
        }
        child_archives = child_archives["experiment"]
        child_archives.data = Entries()
        # TODO, replace all instances of 'upload_id' with contx and test!!!!
        # when using 'nomad parse' this returns None but when used as a plugin in Nomad OASIS, it has a value!
        contx = archive.m_context.upload_id
        print("contx value ", contx)
        logger.info("cotx value ", contx)
        my_name = "demo"
        # now write to file. This is only for displaying data in the hdf5 viewer
        with archive.m_context.raw_file(filename, "w") as newfile:
            with h5py.File(newfile.name, "w") as hdf:
                group_name = f"my_group_{my_name}"
                group = hdf.create_group(group_name)
                group.create_dataset("value", data=[1, 2, 3])
                group.create_dataset("time", data=[4, 5, 6])
                group.attrs["signal"] = "value"
                group.attrs["axes"] = "time"
                group.attrs["NX_class"] = "NXdata"

                for key in allowed_keys:

                    values = [item[key] for item in data_dict]

                    group = hdf.create_group(key)
                    group.create_dataset("value", data=values)

                    group.attrs["signal"] = "value"
                    group.attrs["NX_class"] = "NXdata"

        # finally, set data in archive
        child_archives.data.data_value = (
            f"/uploads/{contx}/raw/{filename}#/{group_name}/value"
        )
        try:
            HDF5Reference.read_dataset(archive, child_archives.data.data_value)
        except:
            logger.info("Dataset coud not be found! ", child_archives.data.data_value)

        for key in allowed_keys:
            values = [item[key] for item in data_dict]
            dataset_path = f"{filename}#/{key}/value"

            # HDF5Reference.write_dataset(archive, values, dataset_path)
            try:
                dataset_path = f"/uploads/{contx}/raw/{filename}#/{key}/value"
                setattr(archive.data, key, dataset_path)
            except:
                pass

        archive.data.value = Entries()
        archive.data.value.data_value = archive.data.U1

        example_filename = f"{my_name}_testHDF5.archive.yaml"

        create_archive(
            child_archives.m_to_dict(),
            archive.m_context,
            example_filename,
            "yaml",
            logger,
        )

        # archive.data = Entries()

    # with h5py.File(hdf5_filename, "r") as f:
    #     ls = list(f.keys())
    #     print(ls)
    #     for key in ls:
    #         group = f.get(key)
    #         print(group["value"][()])
