import subprocess
import multiprocessing
import queue
import time
from typing import Tuple

from video_streamer.core.camera import TestCamera, LimaCamera, MJPEGCamera, LimaCameraDuo
from video_streamer.core.config import SourceConfiguration


class Streamer:
    def __init__(self, config: SourceConfiguration, host: str, port: int, debug: bool):
        self._config = config
        self._host = host
        self._port = port
        self._debug = debug

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass


class MJPEGStreamer(Streamer):
    def __init__(self, config: SourceConfiguration, host: str, port: int, debug: bool):
        super().__init__(config, host, port, debug)
        self._poll_image_p = None
        self._expt = 0.05

        if self._config.input_uri == "test":
            self._camera = TestCamera("TANGO_URI", self._expt, False)
        elif self._config.input_uri.startswith("http"):
            self._camera = MJPEGCamera(self._config.input_uri, self._expt, False)
        else:
            self._camera = LimaCamera(self._config.input_uri, self._expt, False)

    def start(self) -> None:
        _q = multiprocessing.Queue(1)

        self._poll_image_p = multiprocessing.Process(
            target=self._camera.poll_image, args=(_q,)
        )
        self._poll_image_p.start()

        last_frame = _q.get()

        out_size = self._config.size if self._config.size[0] else self._camera.size

        while True:
            try:
                _data = _q.get_nowait()
            except queue.Empty:
                pass
            else:
                last_frame = _data

            yield (
                b"--frame\r\n--!>\nContent-type: image/jpeg\n\n"
                + self._camera.get_jpeg(last_frame, out_size)
                + b"\r\n"
            )

            time.sleep(self._expt)

    def stop(self) -> None:
        if self._poll_image_p:
            self._poll_image_p.kill()


class FFMPGStreamer(Streamer):
    def __init__(self, config: SourceConfiguration, host: str, port: int, debug: bool):
        super().__init__(config, host, port, debug)
        self._ffmpeg_process = None
        self._poll_image_p = None

    def _start_ffmpeg(
        self,
        source_size: Tuple[int, int],
        out_size: Tuple[int, int],
        quality: int = 4,
        port: int = 8000,
    ) -> None:
        """
        Start encoding with ffmpeg and stream the video with the node
        websocket relay.

        :param tuple source_size: Video size at source, width, height
        :param tuple out_size: Output size (scaling), width, height
        :param int quality: Quality (compression) option to pass to FFMPEG
        :param int port: Port (on localhost) to send stream to
        :returns: Processes performing encoding
        :rtype: tuple
        """
        source_size = "%s:%s" % source_size
        out_size = "%s:%s" % out_size

        ffmpeg_args = [
            "ffmpeg",
            "-f",
            "rawvideo",
            "-pixel_format",
            "rgb24",
            "-s",
            source_size,
            "-i",
            "-",
            "-f",
            "mpegts",
            "-q:v",
            "%s" % quality,
            "-vf",
            "scale=%s" % out_size,
            "-vcodec",
            "mpeg1video",
            "http://127.0.0.1:%s/video_input/" % port,
        ]

        stderr = subprocess.DEVNULL if not self._debug else subprocess.STDOUT

        ffmpeg = subprocess.Popen(
            ffmpeg_args,
            stderr=stderr,
            stdin=subprocess.PIPE,
            shell=False,
            close_fds=False,
        )

        return ffmpeg

    def start(self) -> None:
        if self._config.input_uri == "test":
            camera = TestCamera("TANGO_URI", 0.02, False)
        elif self._config.input_uri.startswith("http"):
            self._camera = MJPEGCamera(self._config.input_uri, self._expt, False)
        else:
            camera = LimaCamera(self._config.input_uri, 0.02, False)

        out_size = self._config.size if self._config.size[0] else camera.size

        ffmpeg_p = self._start_ffmpeg(
            camera.size, out_size, self._config.quality, self._port
        )

        self._poll_image_p = multiprocessing.Process(
            target=camera.poll_image, args=(ffmpeg_p.stdin,)
        )

        self._poll_image_p.start()
        self._ffmpeg_process = ffmpeg_p
        return ffmpeg_p

    def stop(self) -> None:
        if self._ffmpeg_process:
            self._ffmpeg_process.kill()

        if self._poll_image_p:
            self._poll_image_p.kill()


class MJPEGDuoStreamer(Streamer):
    def __init__(self, config: SourceConfiguration, host: str, port: int, debug: bool):
        super().__init__(config, host, port, debug)
        self._poll_image_p = None
        self._expt = self._config.exposure_time

        if not isinstance(self._config.input_uri, list):
            raise ValueError("MJPEGDuoStreamer requires a input_uri list")
        if len(self._config.input_uri) != 2:
            raise ValueError("MJPEGDuoStreamer only supports 2 inputs")

        self._camera = LimaCameraDuo(
            self._config.input_uri[0],
            self._config.input_uri[1],
            self._expt,
            resize = self._config.size,
            crop = self._config.crop,
            rotate = self._config.rotate,
            flip = self._config.flip,
            debug = True)

    def start(self) -> None:
        if self._poll_image_p is None:
            _q = multiprocessing.Queue(1)

            self._poll_image_p = multiprocessing.Process(
                target=self._camera.poll_image, args=(_q,)
            )
            self._poll_image_p.start()

            last_frame = _q.get()

            while True:
                try:
                    _data = _q.get_nowait()
                except queue.Empty:
                    pass
                else:
                    last_frame = _data

                yield (
                    b"--frame\r\n--!>\nContent-type: image/jpeg\n\n"
                    + self._camera.get_jpeg(last_frame)
                    + b"\r\n"
                )

                time.sleep(self._expt)

    def stop(self) -> None:
        if self._poll_image_p:
            self._poll_image_p.kill()
            self._poll_image_p = None
