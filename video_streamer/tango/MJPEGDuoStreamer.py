#!/usr/bin/env python

import logging
import time
import os
import signal
import multiprocessing

from tango import DevState, ArgType, DeviceProxy
from tango.server import Device, attribute, command, device_property

from video_streamer.server import create_app
from video_streamer.core.config import get_multisource_config_from_dict

import uvicorn
import requests



class MJPEGDuoStreamer(Device):

    VIDEO_URI    = "/video/"
    SHUTDOWN_URI = "/shutdown"

    SHUTDOWN_SLEEP = 1.0
    KILL_SLEEP     = 0.1

    host = device_property(dtype=str, doc='Host name', default_value="0.0.0.0")
    port = device_property(dtype=int, doc='Port number', default_value=8000)

    exposure_time = device_property(dtype=float, doc='Expected exposure time', default_value=0.05)

    primary_camera = device_property(dtype=str, doc='Primary camera Tango URI', mandatory=True)
    primary_resize = device_property(dtype=[int], doc='Primary camera resize', default_value=[0,0])
    primary_crop = device_property(dtype=[int], doc='Primary camera crop', default_value=[0,0])
    primary_rotate = device_property(dtype=int, doc='Primary camera rotate', default_value=0)
    primary_flip = device_property(dtype=[bool], doc='Primary camera flip', default_value=[False,False])

    secondary_camera = device_property(dtype=str, doc='Secondary camera Tango URI', default_value="")
    secondary_resize = device_property(dtype=[int], doc='Secondary camera resize', default_value=[0,0])
    secondary_crop = device_property(dtype=[int], doc='Secondary camera crop', default_value=[0,0])
    secondary_rotate = device_property(dtype=int, doc='Secondary camera rotate', default_value=0)
    secondary_flip = device_property(dtype=[bool], doc='Secondary camera flip', default_value=[False,False])

    @attribute(dtype=ArgType.DevLong, label="")
    def pid(self):
        try:
            p_pid = self.streamerProcess.pid
        except:
            p_pid = -1
        return p_pid

    @attribute(dtype=ArgType.DevString, label="")
    def stream_url(self):
        return self.videoURL

    @attribute(dtype=ArgType.DevBoolean, label="")
    def primary_video_live(self):
        if self.primaryCamera is not None:
            return self.primaryCamera.video_live
        logging.error("Primary camera {} not reachable".format(self.primary_camera))
        return False
    @primary_video_live.write
    def primary_video_live(self, value):
        if self.primaryCamera is not None:
            self.primaryCamera.video_live = value
        else:
            logging.error("Primary camera {} not reachable".format(self.primary_camera))

    @attribute(dtype=ArgType.DevBoolean, label="")
    def secondary_video_live(self):
        if self.secondaryCamera is not None:
            return self.secondaryCamera.video_live
        logging.error("Secondary camera {} not reachable".format(self.secondary_camera))
        return False
    @secondary_video_live.write
    def secondary_video_live(self, value):
        if self.secondaryCamera is not None:
            self.secondaryCamera.video_live = value
        else:
            logging.error("Secondary camera {} not reachable".format(self.secondary_camera))

    @command(dtype_in=None, doc_in='',
             dtype_out=None, doc_out='')
    def startStreaming(self):
        if self.streamerProcess is None:
            host = self.host
            port = self.port
            duo_config = {
                "format": "MJPEGDUO",
                "input_uri": (
                    self.primary_camera,
                    self.secondary_camera
                ),
                "exposure_time": self.exposure_time,
                "size": (
                    self.primary_resize,
                    self.secondary_resize
                ),
                "crop": (
                    self.primary_crop,
                    self.secondary_crop
                ),
                "rotate": (
                    self.primary_rotate,
                    self.secondary_rotate
                ),
                "flip": (
                    self.primary_flip,
                    self.secondary_flip
                )
            }
            
            logging.debug("Starting MJPEG video streamer...")
            self.streamerProcess = self.fork_uvicorn_server(host, port, duo_config)
            if self.streamerProcess.is_alive():
                self.videoURL = "{}:{}{}".format(host, port, MJPEGDuoStreamer.VIDEO_URI)
                logging.info("Streaming on {}".format(self.videoURL))
                self.startedHost = host
                self.startedPort = port
            else:
                logging.error("Something went wrong")
                p_pid = self.streamerProcess.pid
                try:
                    logging.info("Killing pid={}".format(p_pid))
                    os.kill(p_pid, signal.SIGTERM)
                    time.sleep(MJPEGDuoStreamer.KILL_SLEEP)
                    os.kill(p_pid, signal.SIGKILL)
                except:
                    pass
                self.streamerProcess = None
                self.videoURL = ""
                self.startedHost = ""
                self.startedPort = ""

    @command(dtype_in=None, doc_in='',
             dtype_out=None, doc_out='')
    def shutdownStream(self):
        if self.streamerProcess is not None:
            p_pid = self.streamerProcess.pid
            host, port = self.startedHost, self.startedPort
            shutdown_uri = "shutdown"
            logging.info("Requesting a controlled shutdown...")
            try:
                requests.get("http://{}:{}{}".format(host, port, MJPEGDuoStreamer.SHUTDOWN_URI), timeout=0.1)
            except Exception as ex:
                print(str(ex))
            finally:
                time.sleep(MJPEGDuoStreamer.SHUTDOWN_SLEEP)
            try:
                logging.info("Killing pid={} just to make sure".format(p_pid))
                os.kill(p_pid, signal.SIGTERM)
                time.sleep(MJPEGDuoStreamer.KILL_SLEEP)
                os.kill(p_pid, signal.SIGKILL)
            except:
                pass
            self.streamerProcess = None
            self.videoURL = ""
        else:
            logging.warning("Nothing to shutdown")

    def init_device(self):
        super().init_device()

        self.streamerProcess = None
        self.videoURL = ""
        self.startedHost = ""
        self.startedPort = ""

        if not self.primary_camera:
            logging.error("The property 'primary_camera' must be set")
            self.set_state(DevState.OFF)
            self.set_status("Device is OFF")
            return

        try:
            self.primaryCamera = DeviceProxy(self.primary_camera)
            self.primaryCamera.ping()
        except:
            logging.error("Unable to reach primary camera {}".format(self.primary_camera))
            self.primaryCamera = None

        try:
            self.secondaryCamera = DeviceProxy(self.secondary_camera)
            self.secondaryCamera.ping()
        except:
            logging.error("Unable to reach secondary camera {}".format(self.secondary_camera))
            self.secondaryCamera = None

        self.set_state(DevState.ON)
        self.set_status("Device is ON")

    def fork_uvicorn_server(self, host, port, multisource_dict):
        p = None

        app_config = get_multisource_config_from_dict(multisource_dict)
        app = create_app(app_config, host, int(port), debug=True)
        if app:
            logging.debug("Created FastAPI application")
            config = uvicorn.Config(
                app,
                host=host,
                port=int(port),
                reload=False,
                workers=1,
                log_level="debug",
            )

            server = uvicorn.Server(config=config)
            logging.debug("Created Uvicorn server")
            p = multiprocessing.Process(target = server.run)
            logging.debug("Forking...")
            p.start()
            logging.info("Server started with pid={}".format(p.pid))
            
        return p


def main():
    fmt = "%(asctime)s %(levelname)s %(name)s %(message)s"
    logging.basicConfig(level="DEBUG", format=fmt)
    MJPEGDuoStreamer.run_server()

if __name__ == "__main__":
    main()
