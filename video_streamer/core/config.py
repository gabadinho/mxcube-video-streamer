import logging
import os
import json

from typing import Dict, Union, Tuple, List
from pydantic import BaseModel, Field, ValidationError


class SourceConfiguration(BaseModel):
    input_uri: str = Field("", description="URI for input device")
    quality: int = Field(4, description="FFMpeg Quality")
    format: str = Field("MPEG1", description="Output format MPEG1 or MJPEG")
    hash: str = Field("", description="Server url postfix/trail")
    size: Tuple[int, int] = Field((0, 0), description="Image size")


class MultiSourceConfiguration(BaseModel):
    input_uri: Union[ str, List[str] ] = Field("", description="URI for input device")
    format: str = Field("MJPEGDUO", description="Output format MJPEG")
    hash: str = Field("", description="Server url postfix/trail")
    max_concurrent_streams: int = Field(2, description="Maximum stream clients")
    exposure_time: Tuple[float, float] = Field((0.05, 0.10), description="Poll sleeping times")
    size: Union[ Tuple[int, int], List[Tuple[int, int]] ] = Field((0, 0), description="Image size")
    crop: Union[ Tuple[int, int], List[Tuple[int, int]] ] = Field((0, 0), description="Image crop")
    rotate: Union[ int, List[int] ] = Field(0, description="Image rotation")
    flip: Union[ Tuple[bool, bool], List[Tuple[bool, bool]] ] = Field((False, False), description="Image flip horizontal, vertical")


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


def get_multisource_config_from_dict(config_data: dict) -> Union[MultiSourceConfiguration, None]:
    data = MultiSourceConfiguration(**config_data)
    return data
