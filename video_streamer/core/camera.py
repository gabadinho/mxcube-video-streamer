import time
import logging
import struct
import sys
import os
import io
import multiprocessing
import multiprocessing.queues
import requests
import queue

from typing import Union, IO, Tuple

from PIL import Image

try:
    import PyTango
    from PyTango import DeviceProxy
except ImportError:
    logging.warning("PyTango not available.")


class Camera:
    
    FULL_QUEUE_TIMEOUT = 60
    
    def __init__(self, device_uri: str, sleep_time: float, debug: bool = False):
        self._device_uri = device_uri
        self._sleep_time = sleep_time
        self._debug = debug
        self._width = -1
        self._height = -1
        self._output = None
        self._write_successfull_timestamp = None

    def _poll_once(self) -> None:
        pass

    def _write_data(self, data: bytearray):
        if isinstance(self._output, multiprocessing.queues.Queue):
            if data is not None:
                try:
                    self._output.put_nowait(data)
                except queue.Full:
                    pass
                else:
                    self._write_successfull_timestamp = time.time()
        else:
            self._output.write(data)

    def poll_image(self, output: Union[IO, multiprocessing.queues.Queue]) -> None:
        self._output = output

        self._write_successfull_timestamp = time.time()
        with PyTango.EnsureOmniThread():
            while True:
                try:
                    self._poll_once()
                except KeyboardInterrupt:
                    sys.exit(0)
                except BrokenPipeError:
                    sys.exit(0)
                except Exception:
                    logging.exception("")
                else:
                    if (time.time() - self._write_successfull_timestamp) > Camera.FULL_QUEUE_TIMEOUT:
                        logging.info("Nobody is pulling images anymore, quitting camera polling")
                        break

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

    VIDEO_BYTES = {
        "RGB24": 3,
        "Y8": 1,
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

        self._last_frame_number = -1
        self._last_use_secondary = None

    def _connect(self, device_uri: str) -> DeviceProxy:
        try:
            logging.info("Connecting to {}".format(device_uri))
            lima_tango_device = DeviceProxy(device_uri)
            lima_tango_device.ping()
            video_mode = lima_tango_device.video_mode
            image_width = lima_tango_device.image_width
            image_height = lima_tango_device.image_height
        except Exception:
            logging.exception("")
            logging.info("Could not connect to {}".format(device_uri))
            raise
        else:
            return lima_tango_device, video_mode, image_width, image_height

    def _get_image(self, use_secondary=None) -> Tuple[bytearray, float, float, int]:
        try:
            if use_secondary:
                img_data = self._lima_tango_device_secondary.video_last_image
            else:
                img_data = self._lima_tango_device_primary.video_last_image
        except:
            if use_secondary:
                logging.error("Problem retrieving last image from secondary camera")
            else:
                logging.error("Problem retrieving last image from primary camera")
            return None, None, None, None
            #raise ValueError
        else:
            try:
                hfmt = ">IHHqiiHHHH"
                hsize = struct.calcsize(hfmt)
                _, _, img_mode, frame_number, width, height, _, _, _, _ = struct.unpack(
                    hfmt, img_data[1][:hsize]
                )
                raw_data = img_data[1][hsize:]
            except:
                if use_secondary:
                    logging.error("Problem accessing last image from secondary camera")
                else:
                    logging.error("Problem accessing last image from primary camera")
                return None, None, None, None

        return raw_data, width, height, frame_number

    def _poll_once(self) -> None:
        use_secondary = None
        frame_number = -1
            
        try:
            if self._lima_tango_device_primary.video_live:
                frame_number = self._lima_tango_device_primary.video_last_image_counter
                use_secondary = False
            elif self._lima_tango_device_secondary is not None and self._lima_tango_device_secondary.video_live:
                frame_number = self._lima_tango_device_secondary.video_last_image_counter
                use_secondary = True
        except:
            pass
        else:
            if use_secondary is not None:
                if self._last_use_secondary != use_secondary:
                    if use_secondary:
                        logging.info("Now streaming from secondary (with sleep time={} sec): {}".format(self._sleep_time[1], self._device_uri_secondary))
                    else:
                        logging.info("Now streaming from primary (with sleep time={} sec): {}".format(self._sleep_time[0], self._device_uri))

                if self._last_frame_number != frame_number:
                    try:
                        raw_data, width, height, frame_number = self._get_image(use_secondary)
                    except:
                        time.sleep(self._sleep_time[int(use_secondary)] * 0.999)
                        return
                    else:
                        if raw_data is not None:
                            self._write_data(raw_data)
                            self._last_frame_number = frame_number

            elif self._last_use_secondary is not None:
                logging.info("No 'video_live' camera enabled, pausing stream...")

            self._last_use_secondary = use_secondary

        if use_secondary is not None:
            time.sleep(self._sleep_time[int(use_secondary)] * 0.999)
        else:
            time.sleep(1.0)

    def get_jpeg(self, data, size=None, dont_resize=None) -> bytearray:
        if data is None:
            logging.error("Empty frame, unable to convert to jpeg")
            return None, None

        try:
            data_size_0 = self._widths[0] * self._heights[0] * LimaCameraDuo.VIDEO_BYTES[self._video_modes[0]]
            data_size_1 = self._widths[1] * self._heights[1] * LimaCameraDuo.VIDEO_BYTES[self._video_modes[1]]
        except:
            logging.error("Failed to calculate data sizes for the cameras")
            return None, None
        else:
            if len(data) == data_size_0:
                camera_index = 0
            elif len(data) == data_size_1:
                camera_index = 1
            else:
                logging.error("Corrupted frame, unable to convert to jpeg")
                return None, None

        try:
            image_format = LimaCameraDuo.VIDEO_FORMATS[self._video_modes[camera_index]]
        except:
            image_format = "RGB"
            logging.warning("Defaulting to 'RGB' mode")
        size = (self._widths[camera_index], self._heights[camera_index])

        jpeg_data = io.BytesIO()
        image = Image.frombytes(image_format, size, data, "raw")

        if not dont_resize:
            try:
                image_resize = self._images_resize[camera_index]
                resize_width = int(image_resize[0])
                resize_height = int(image_resize[1])
            except:
                pass
            else:
                if resize_width>0 and resize_height>0:
                    image = image.resize(image_resize, resample=Image.BILINEAR)
                    size = image_resize

        if not dont_resize:
            try:
                image_crop = self._images_crop[camera_index]
                crop_width = int(image_crop[0])
                crop_height = int(image_crop[1])
            except:
                pass
            else:
                if crop_width>0 and crop_height>0:
                    width, height = size
                    image = image.crop(((width-crop_width)//2, (height-crop_height)//2, (width+crop_width)//2, (height+crop_height)//2))
                    size = image_crop

        try:
            image_rotate = int(self._images_rotate[camera_index])
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
            flip_horizontal = bool(self._images_flip[camera_index][0])
        except:
            pass
        else:
            if flip_horizontal:
                image = image.transpose(Image.FLIP_LEFT_RIGHT)

        try:
            flip_vertical = bool(self._images_flip[camera_index][1])
        except:
            pass
        else:
            if flip_vertical:
                image = image.transpose(Image.FLIP_TOP_BOTTOM)

        image.save(jpeg_data, format="JPEG")
        jpeg_data = jpeg_data.getvalue() 

        return camera_index, jpeg_data
