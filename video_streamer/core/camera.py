import time
import logging
import struct
import sys
import os
import io
import multiprocessing
import multiprocessing.queues
import requests

from typing import Union, IO, Tuple

from PIL import Image

try:
    from PyTango import DeviceProxy
except ImportError:
    logging.warning("PyTango not available.")


class Camera:
    def __init__(self, device_uri: str, sleep_time: float, debug: bool = False):
        self._device_uri = device_uri
        self._sleep_time = sleep_time
        self._debug = debug
        self._width = -1
        self._height = -1
        self._output = None

    def _poll_once(self) -> None:
        pass

    def _write_data(self, data: bytearray):
        if isinstance(self._output, multiprocessing.queues.Queue):
            self._output.put(data)
        else:
            self._output.write(data)

    def poll_image(self, output: Union[IO, multiprocessing.queues.Queue]) -> None:
        self._output = output

        while True:
            try:
                self._poll_once()
            except KeyboardInterrupt:
                sys.exit(0)
            except BrokenPipeError:
                sys.exit(0)
            except Exception:
                logging.exception("")
            finally:
                pass

    @property
    def size(self) -> Tuple[float, float]:
        return (self._width, self._height)

    def get_jpeg(self, data, size=(0, 0)) -> bytearray:
        jpeg_data = io.BytesIO()
        image = Image.frombytes("RGB", self.size, data, "raw")

        if size[0]:
            image = image.resize(size)

        image.save(jpeg_data, format="JPEG")
        jpeg_data = jpeg_data.getvalue()

        return jpeg_data


class MJPEGCamera(Camera):
    def __init__(self, device_uri: str, sleep_time: float, debug: bool = False):
        super().__init__(device_uri, sleep_time, debug)

    def poll_image(self, output: Union[IO, multiprocessing.queues.Queue]) -> None:
        # auth=("user", "password")
        r = requests.get(self._device_uri, stream=True)

        buffer = bytes()
        while True:
            try:
                if r.status_code == 200:
                    for chunk in r.iter_content(chunk_size=1024):
                        buffer += chunk

                else:
                    print("Received unexpected status code {}".format(r.status_code))
            except requests.exceptions.StreamConsumedError:
                output.put(buffer)
                r = requests.get(self._device_uri, stream=True)
                buffer = bytes()

    def get_jpeg(self, data, size=None) -> bytearray:
        return data


class LimaCamera(Camera):
    def __init__(self, device_uri: str, sleep_time: int, debug: bool = False):
        super().__init__(device_uri, sleep_time, debug)

        self._lima_tango_device = self._connect(self._device_uri)
        _, self._width, self._height, _ = self._get_image()
        self._sleep_time = sleep_time
        self._last_frame_number = -1

    def _connect(self, device_uri: str) -> DeviceProxy:
        try:
            logging.info("Connecting to %s", device_uri)
            lima_tango_device = DeviceProxy(device_uri)
            lima_tango_device.ping()
        except Exception:
            logging.exception("")
            logging.info("Could not connect to %s, retrying ...", device_uri)
            sys.exit(-1)
        else:
            return lima_tango_device

    def _get_image(self) -> Tuple[bytearray, float, float, int]:
        img_data = self._lima_tango_device.video_last_image

        hfmt = ">IHHqiiHHHH"
        hsize = struct.calcsize(hfmt)
        _, _, img_mode, frame_number, width, height, _, _, _, _ = struct.unpack(
            hfmt, img_data[1][:hsize]
        )

        raw_data = img_data[1][hsize:]

        return raw_data, width, height, frame_number

    def _poll_once(self) -> None:
        frame_number = self._lima_tango_device.video_last_image_counter

        if self._last_frame_number != frame_number:
            raw_data, width, height, frame_number = self._get_image()
            self._raw_data = raw_data

            self._write_data(self._raw_data)
            self._last_frame_number = frame_number

        time.sleep(self._sleep_time / 2)


class TestCamera(Camera):
    def __init__(self, device_uri: str, sleep_time: float, debug: bool = False):
        super().__init__(device_uri, sleep_time, debug)
        self._sleep_time = 0.05
        testimg_fpath = os.path.join(os.path.dirname(__file__), "fakeimg.jpg")
        self._im = Image.open(testimg_fpath, "r")

        self._raw_data = self._im.convert("RGB").tobytes()
        self._width, self._height = self._im.size

    def _poll_once(self) -> None:
        self._write_data(self._raw_data)
        time.sleep(self._sleep_time)


class LimaCameraDuo(Camera):

    VIDEO_FORMATS = {
        "RGB24": "RGB",
        "Y8": "L",
    }

    def __init__(self, device_uri_primary: str, device_uri_secondary: str, sleep_time: float, resize = None, crop = None, rotate = None, flip = None, debug: bool = False):
        super().__init__(device_uri_primary, sleep_time, debug)

        self._device_uri_secondary = device_uri_secondary

        self._widths = [-1, -1]
        self._heights = [-1, -1]

        self._video_modes = ["RGB24", "RGB24"]

        self._images_resize = resize
        self._images_crop = crop
        self._images_rotate = rotate
        self._images_flip = flip

        try:
            self._lima_tango_device_primary, self._video_modes[0], self._widths[0], self._heights[0] = self._connect(self._device_uri)
        except:
            sys.exit(-1)
        self._lima_tango_device_secondary = None
        if len(self._device_uri_secondary):
            try:
                self._lima_tango_device_secondary, self._video_modes[1], self._widths[1], self._heights[1]  = self._connect(self._device_uri_secondary)
            except:
                pass

        # Ugly hack to distinguish between cameras when retrieving data from the queue, by the size of the raw data (meaning this wont work if both cameras are exactly the same!)
        self._size_primary = -1
        self._size_secondary = -1

        self._sleep_time = sleep_time
        self._last_frame_number = -1

    def _connect(self, device_uri: str) -> DeviceProxy:
        try:
            logging.info("Connecting to %s", device_uri)
            lima_tango_device = DeviceProxy(device_uri)
            lima_tango_device.ping()
            video_mode = lima_tango_device.video_mode
            image_width = lima_tango_device.image_width
            image_height = lima_tango_device.image_height
        except Exception:
            logging.exception("")
            logging.info("Could not connect to %s, retrying ...", device_uri)
            raise
        else:
            return lima_tango_device, video_mode, image_width, image_height

    def _get_image(self, use_secondary=None) -> Tuple[bytearray, float, float, int]:
        if use_secondary:
            img_data = self._lima_tango_device_secondary.video_last_image
        else:
            img_data = self._lima_tango_device_primary.video_last_image

        hfmt = ">IHHqiiHHHH"
        hsize = struct.calcsize(hfmt)
        _, _, img_mode, frame_number, width, height, _, _, _, _ = struct.unpack(
            hfmt, img_data[1][:hsize]
        )

        raw_data = img_data[1][hsize:]
        if use_secondary and self._size_secondary == -1:
            self._size_secondary = len(raw_data)
        elif not use_secondary and self._size_primary == -1:
            self._size_primary = len(raw_data)

        return raw_data, width, height, frame_number

    def _poll_once(self) -> None:
        use_secondary = False
        if self._lima_tango_device_primary.video_live:
            frame_number = self._lima_tango_device_primary.video_last_image_counter
        elif self._lima_tango_device_secondary is not None and self._lima_tango_device_secondary.video_live:
            frame_number = self._lima_tango_device_secondary.video_last_image_counter
            use_secondary = True

        if self._last_frame_number != frame_number:
            raw_data, width, height, frame_number = self._get_image(use_secondary)
            self._raw_data = raw_data

            self._write_data(self._raw_data)
            self._last_frame_number = frame_number

        time.sleep(self._sleep_time / 2)

    def get_jpeg(self, data, size=None) -> bytearray:
        camera_index = 0
        if len(data) == self._size_secondary:
            camera_index = 1
        try:
            image_format = LimaCameraDuo.VIDEO_FORMATS[self._video_modes[camera_index]]
        except:
            image_format = "RGB"
        size = (self._widths[camera_index], self._heights[camera_index])

        jpeg_data = io.BytesIO()
        image = Image.frombytes(image_format, size, data, "raw")

        try:
            image_resize = self._images_resize[camera_index]
        except:
            pass
        else:
            if image_resize is not None and image_resize != (0, 0):
                image = image.resize(image_resize, resample=Image.BILINEAR)

        try:
            image_crop = self._images_crop[camera_index]
        except:
            pass
        else:
            if image_crop is not None and image_crop != (0, 0):
                new_width, new_height  = image_crop
                width, height = size
                image = image.crop(((width-new_width)//2, (height-new_height)//2, (width+new_width)//2, (height+new_height)//2))

        try:
            image_rotate = self._images_rotate[camera_index]
        except:
            pass
        else:
            if image_rotate == 90:
                image = image.transpose(Image.ROTATE_90)
            elif image_rotate == 180:
                image = image.transpose(Image.ROTATE_180)
            elif image_rotate == 270:
                image = image.transpose(Image.ROTATE_270)

        try:
            flip_horizontal = self._images_flip[camera_index][0]
        except:
            pass
        else:
            if flip_horizontal:
                image = image.transpose(Image.FLIP_LEFT_RIGHT)

        try:
            flip_vertical = self._images_flip[camera_index][1]
        except:
            pass
        else:
            if flip_vertical:
                image = image.transpose(Image.FLIP_TOP_BOTTOM)

        image.save(jpeg_data, format="JPEG")
        jpeg_data = jpeg_data.getvalue() 

        return jpeg_data
