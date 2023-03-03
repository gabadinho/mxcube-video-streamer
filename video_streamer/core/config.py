import logging
import os
import json

from typing import Dict, Union
from pydantic import BaseModel, Field, ValidationError


class SourceConfiguration(BaseModel):
    input_uri: str = Field("", description="Tango URI for input device")
    quality: int = Field(4, description="FFMpeg Quality")
    format: str = Field("MPEG1", description="Output format MPEG1 or MJPEG")
    hash: str = Field("", description="Server url postfix/trail")

class ServerConfiguration(BaseModel):
    sources: Dict[str, SourceConfiguration]


def get_config_from_file(fpath: str) -> Union[ServerConfiguration, None]:
    data = None

    if os.path.isfile(fpath):
        with open(fpath, "r") as _f:
            config_data = json.load(_f)

            try:
                data = ServerConfiguration(**config_data)
            except ValidationError:
                logging.exception(f"Validation error in {fpath}")

    return data


def get_config_from_dict(config_data: dict) -> Union[ServerConfiguration, None]:
    data = ServerConfiguration(**config_data)
    return data

