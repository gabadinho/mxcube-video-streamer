#!/usr/bin/env python

import logging
import time
import os
import signal
import socket
import multiprocessing

from tango import DevState, ArgType, DeviceProxy
from tango.server import Device, attribute, command, device_property

from video_streamer.server import create_app
from video_streamer.core.config import get_multisource_config_from_dict

import uvicorn
import requests



class MJPEGDuoStreamer(Device):

    VIDEO_URI    = "/ui/"
    SHUTDOWN_URI = "/shutdown"
    JPEG_URI     = "/last_image"

    SHUTDOWN_SLEEP = 1.0
    KILL_SLEEP     = 0.1

    host = device_property(dtype=str, doc='Host name', default_value="0.0.0.0")
    port = device_property(dtype=int, doc='Port number', default_value=8000)

    max_concurrent_streams = device_property(dtype=int, doc='Maximum stream clients', default_value=2)
    exposure_time = device_property(dtype=[float], doc='Poll sleeping times', default_value=[0.05, 0.10])

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

    @attribute(dtype=ArgType.DevULong, label="")
    def primary_video_width(self):
        if self.primaryCamera is not None:
            primary_width, primary_height = self.primaryCamera.image_width, self.primaryCamera.image_height

            resize = self.check_size_tuple_validity(self.primary_resize)
            if resize is not None:
                primary_width = resize[0]
                primary_height = resize[1]

            crop = self.check_size_tuple_validity(self.primary_crop)
            if crop is not None:
                primary_width = crop[0]
                primary_height = crop[1]
            
            rotation = self.check_size_validity(self.primary_rotate)
            if rotation in (90,270):
                primary_width = primary_height
            
            return primary_width

        logging.error("Primary camera {} not reachable".format(self.primary_camera))
        return None

    @attribute(dtype=ArgType.DevULong, label="")
    def primary_video_height(self):
        if self.primaryCamera is not None:
            primary_width, primary_height = self.primaryCamera.image_width, self.primaryCamera.image_height

            resize = self.check_size_tuple_validity(self.primary_resize)
            if resize is not None:
                primary_width = resize[0]
                primary_height = resize[1]

            crop = self.check_size_tuple_validity(self.primary_crop)
            if crop is not None:
                primary_width = crop[0]
                primary_height = crop[1]
            
            rotation = self.check_size_validity(self.primary_rotate)
            if rotation in (90,270):
                primary_height = primary_width
            
            return primary_height

        logging.error("Primary camera {} not reachable".format(self.primary_camera))
        return None

    @attribute(dtype=bytearray, label="")
    def video_last_jpeg(self):
        if self.streamerProcess is not None:
            jpeg = None
            host, port = self.startedHost, self.startedPort
            request_url = "http://{}:{}{}".format(host, port, MJPEGDuoStreamer.JPEG_URI)
            try:
                res = requests.get(request_url)
                if res.status_code == 200:
                    jpeg = res.content
            except Exception as ex:
                logging.error("Unable to retrieve last jpeg image from {}".format(request_url))
            return ('uint8', jpeg)
        else:
            return None

    @command(dtype_in=None, doc_in='',
             dtype_out=str, doc_out='')
    def startStreaming(self):
        result = "Unknown result"

        if self.streamerProcess is None:
            host = self.host
            port = self.port
            duo_config = {
                "format": "MJPEGDUO",
                "input_uri": (
                    self.primary_camera,
                    self.secondary_camera
                ),
                "max_concurrent_streams": self.max_concurrent_streams,
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
            
            if not test_socket(host, port):
                result = "Unable to bind to {}:{}, quitting...".format(host, port)
            else:
                logging.debug("Starting MJPEG video streamer...")
                self.streamerProcess = fork_uvicorn_server(host, port, duo_config)
                if self.streamerProcess.is_alive():
                    self.videoURL = "{}:{}{}".format(host, port, MJPEGDuoStreamer.VIDEO_URI)
                    result = "Streaming on {} with pid={}".format(self.videoURL, self.streamerProcess.pid)
                    logging.info(result)
                    self.startedHost = host
                    self.startedPort = port
                else:
                    result = "Something went wrong, trying to kill pid {}".format(self.streamerProcess.pid)
                    logging.error(result)
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
        else:
            result = "Already running on pid {}".format(self.streamerProcess.pid)

        return result

    @command(dtype_in=None, doc_in='',
             dtype_out=str, doc_out='')
    def shutdownStream(self):
        result = "Unknown result"

        if self.streamerProcess is not None:
            shutdown_uvicorn_server(self.startedHost, self.startedPort, self.streamerProcess.pid)
            self.streamerProcess = None
            self.videoURL = ""
            self.startedHost = ""
            self.startedPort = ""
        else:
            result = "Nothing to shutdown"
            logging.warning(result)

        return result

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

    def check_size_tuple_validity(self, size):
        valid_size = None
        try:
            width = int(size[0])
            height = int(size[1])
        except:
            pass
        else:
            if width>0 and height>0:
                valid_size = size
        return valid_size

    def check_size_validity(self, size):
        valid_size = None
        try:
            valid_size = int(size)
        except:
            pass
        else:
            if valid_size<=0:
                valid_size = None
        return valid_size

    def delete_device(self):
        if self.streamerProcess is not None:
            shutdown_uvicorn_server(self.startedHost, self.startedPort, self.streamerProcess.pid)
        super().delete_device()


def test_socket(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(0.1)
    result = False
    try:
        sock.bind((host, port))
        result = True
    except Exception as ex:
        logging.error("Unable to bind to {}:{} ({})".format(host, port, str(ex)))
    finally:
        try:
            sock.shutdown()
        except:
            pass
        try:
            sock.close()
        except:
            pass
    return result

def fork_uvicorn_server(host, port, multisource_dict):
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

def shutdown_uvicorn_server(host, port, p_pid):
    result = "Requesting a controlled shutdown to http://{}:{}{}".format(host, port, MJPEGDuoStreamer.SHUTDOWN_URI)
    logging.info("Requesting a controlled shutdown...")
    try:
        requests.get("http://{}:{}{}".format(host, port, MJPEGDuoStreamer.SHUTDOWN_URI), timeout=0.1)
    except Exception as ex:
        pass
    finally:
        time.sleep(MJPEGDuoStreamer.SHUTDOWN_SLEEP)
    try:
        logging.info("Killing pid={} just to make sure".format(p_pid))
        os.kill(p_pid, signal.SIGTERM)
        time.sleep(MJPEGDuoStreamer.KILL_SLEEP)
        os.kill(p_pid, signal.SIGKILL)
    except:
        pass


def main():
    fmt = "%(asctime)s %(levelname)s %(name)s %(message)s"
    logging.basicConfig(level="DEBUG", format=fmt)
    MJPEGDuoStreamer.run_server()

if __name__ == "__main__":
    main()
