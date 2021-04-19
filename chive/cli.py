import asyncio
import logging
import signal
import typer

from typing import List

from chive import Chive, __version__


cli = typer.Typer()


def handle_terminate():
    if app is not None:
        loop = asyncio.get_event_loop()
        loop.create_task(app.stop_worker())


def get_application(app_module: str) -> Chive:
    global app
    if ":" in app_module:
        module_path, app_name = app_module.split(":")
    else:
        module_path = app_module
        app_name = "app"

    if "." in module_path:
        _, module_name = module_path.rsplit(".", 1)
        module = getattr(__import__(module_path), module_name)
    else:
        module = __import__(module_path)

    return getattr(module, app_name)


@cli.command()
def worker(
    app_module: str = typer.Argument(
        ...,
        help="Module path to the Chive application object in format <module>[:<appname>]. Default application name is 'app'. Example: example.app:app",
    ),
    task: str = typer.Argument(..., help="Task name this worker will serve."),
    concurrency: int = typer.Option(
        1, "-c", "--concurreny", help="Concurreny of the worker."
    ),
    log_format: str = typer.Option(
        "[%(asctime)s] [%(levelname)-8s] -- %(message)s",
        "-f",
        "--log-format",
        help="Logging format to use when printing logging messages.",
    ),
    log_level: str = typer.Option(
        "info",
        "-l",
        "--log-level",
        help="Logging level. One of debug, info, warn, error and critical.",
    ),
):
    app: Chive = get_application(app_module)

    level = getattr(logging, log_level.upper())
    logging.basicConfig(
        level=level,
        format=log_format,
    )

    loop = asyncio.get_event_loop()
    for signum in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(signum, handle_terminate)

    try:
        loop.run_until_complete(app.init())
        loop.run_until_complete(app.start_worker(task))
        loop.run_until_complete(app.wait_for_stop())
    finally:
        loop.run_until_complete(app.cleanup())


@cli.command()
def version():
    typer.echo(__version__)


if __name__ == "__main__":
    cli()
