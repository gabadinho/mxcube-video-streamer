import os
import sys
import time
import logging
import asyncio

from fastapi import FastAPI, WebSocket, Request, WebSocketDisconnect, HTTPException
from fastapi.responses import StreamingResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles

from video_streamer.core.websockethandler import WebsocketHandler
from video_streamer.core.streamer import FFMPGStreamer, MJPEGStreamer, MJPEGDuoStreamer
from fastapi.templating import Jinja2Templates


def create_app(config, host, port, debug):
    app = None
    app_cls = available_applications.get(config.format, None)

    if app_cls:
        app = app_cls(config, host, port, debug)

    return app


def create_mjpeg_app(config, host, port, debug):
    app = FastAPI()
    streamer = MJPEGStreamer(config, host, port, debug)
    ui_template_root = os.path.join(os.path.dirname(__file__), "ui/template")
    templates = Jinja2Templates(directory=ui_template_root)

    @app.get("/ui", response_class=HTMLResponse)
    async def video_ui(request: Request):
        return templates.TemplateResponse(
            "index_mjpeg.html",
            {
                "request": request,
                "source": f"http://localhost:{port}/video/{config.hash}",
            },
        )

    @app.get(f"/video/{config.hash}")
    def video_feed():
        return StreamingResponse(
            streamer.start(), media_type='multipart/x-mixed-replace;boundary="!>"'
        )

    @app.on_event("startup")
    async def startup():
        pass

    @app.on_event("shutdown")
    async def shutdown():
        streamer.stop()

    return app


def create_mpeg1_app(config, host, port, debug):
    app = FastAPI()
    manager = WebsocketHandler()
    streamer = FFMPGStreamer(config, host, port, debug)
    ui_static_root = os.path.join(os.path.dirname(__file__), "ui/static")
    ui_template_root = os.path.join(os.path.dirname(__file__), "ui/template")
    templates = Jinja2Templates(directory=ui_template_root)

    app.mount(
        "/static", StaticFiles(directory=ui_static_root, html=True), name="static"
    )

    @app.get("/ui", response_class=HTMLResponse)
    async def video_ui(request: Request):
        return templates.TemplateResponse(
            "index_mpeg1.html",
            {"request": request, "source": f"ws://localhost:{port}/ws/{config.hash}"},
        )

    @app.websocket(f"/ws/{config.hash}")
    async def websocket_endpoint(websocket: WebSocket):
        await manager.connect(websocket)

        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            manager.disconnect(websocket)
            await manager.broadcast(f"client disconnected")

    @app.post("/video_input/")
    async def video_in(request: Request):
        async for chunk in request.stream():
            await manager.broadcast(chunk)

    @app.on_event("startup")
    async def startup():
        streamer.start()

    @app.on_event("shutdown")
    async def shutdown():
        streamer.stop()

    return app


def create_mjpegduo_app(config, host, port, debug):

    app = FastAPI()
    ui_template_root = os.path.join(os.path.dirname(__file__), "ui/template")
    templates = Jinja2Templates(directory=ui_template_root)

    client2streamer = {}

    @app.get("/ui", response_class=HTMLResponse)
    async def video_ui(request: Request):
        return templates.TemplateResponse(
            "index_mjpeg.html",
            {
                "request": request,
                "source": f"{request.base_url}video/{config.hash}",
            },
        )

    @app.get(f"/video/{config.hash}")
    def video_feed(request: Request):
        try:
            streamer = client2streamer[(request.client.host,request.client.port)]
        except KeyError:
            if len(client2streamer) >= config.max_concurrent_streams:
                goodbye_client = list(client2streamer.keys())[0]
                goodbye_stream = client2streamer[goodbye_client]
                logging.warning("Reached maximum concurrent streams, closing stream of client {}:{}".format(goodbye_client[0],goodbye_client[1]))
                goodbye_stream.stop()
                client2streamer.pop(goodbye_client)
                goodbye_stream = None

            logging.info("Starting new streamer to client {}:{}".format(request.client.host,request.client.port))
            streamer = MJPEGDuoStreamer(config, host, port, app.my_loop, request, debug)
            client2streamer[(request.client.host,request.client.port)] = streamer
        return StreamingResponse(
            streamer.start(), media_type='multipart/x-mixed-replace;boundary="---MXCuBEMJPEGDuoStreamer"'
        )

    @app.get("/shutdown")
    def shutdown_request(request: Request):
        for client, streamer in client2streamer.items():
            streamer.stop()
        time.sleep(0.1)
        sys.exit(0)

    @app.get("/last_image")
    async def last_image(request: Request):
        try:
            last_client = list(client2streamer.keys())[-1]
            last_stream = client2streamer[last_client]
        except (IndexError, KeyError):
            logging.error("No streamer available")
            raise HTTPException(status_code=404, detail="Last image not found")
        last_jpeg = last_stream.get_last_jpeg()
        return Response(content=last_jpeg, media_type="image/jpg")

    @app.on_event("startup")
    async def startup():
        app.my_loop = asyncio.get_event_loop()

    @app.on_event("shutdown")
    async def shutdown():
        for client, streamer in client2streamer.items():
            streamer.stop()
        client2streamer = {}        

    return app


available_applications = {
    "MPEG1": create_mpeg1_app,
    "MJPEG": create_mjpeg_app,
    "MJPEGDUO": create_mjpegduo_app,
}
