import asyncio
import logging
from functools import partial, wraps

import click

from damie.client import process_job, send_command
from damie.server import handle_request, monitor_active_jobs, log_job_stats
from damie.utils import CustomFormatter


DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 10727


def asyncc(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


@click.group()
@click.option(
    '--host',
    '-h',
    default=DEFAULT_HOST,
    help=f"server host (default: {DEFAULT_HOST})",
)
@click.option(
    '--port',
    '-p',
    default=DEFAULT_PORT,
    help=f"server port (default: {DEFAULT_PORT})",
)
@click.option(
    '--log-level',
    '-l',
    default='INFO',
    type=click.Choice(
        ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], case_sensitive=False
    ),
    help="logging level (default: INFO).",
)
@click.pass_context
def cli(ctx, host, port, log_level):
    """feed da'mie!"""
    ch = logging.StreamHandler()
    ch.setFormatter(CustomFormatter())

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(ch)

    ctx.ensure_object(dict)
    ctx.obj['HOST'] = host
    ctx.obj['PORT'] = port


@cli.command()
@click.argument('bowl')
@click.option(
    '--heartbeat-interval',
    '-i',
    default=600,
    show_default=True,
    type=int,
    help='interval between heartbeats (seconds)',
)
@click.option(
    '--max-jobs',
    '-m',
    default=None,
    type=int,
    help='maximum number of jobs to eat (default: no limit)',
)
@click.option(
    '--max-async-jobs',
    '-j',
    default=1,
    type=int,
    help='maximum number of concurrent jobs to eat (default: 1)',
)
@click.pass_context
@asyncc
async def eat(ctx, bowl, heartbeat_interval, max_jobs, max_async_jobs=1):
    """eat jobs from a bowl"""
    host = ctx.obj['HOST']
    port = ctx.obj['PORT']

    jobs_eaten = 0
    running_tasks = set()
    job_slots = set(range(max_async_jobs))

    _process_job = partial(process_job, host, port, heartbeat_interval)

    while True:
        while (max_jobs is None or jobs_eaten < max_jobs) and len(
            running_tasks
        ) < max_async_jobs:
            logging.info(f"EAT: requesting job from bowl '{bowl}'...")
            response = await send_command(f'EAT:1:{bowl}', host, port)

            if not response or response.strip() == 'EMPTY':
                logging.info(f"EAT: bowl '{bowl}' is empty.")
                break

            slot = job_slots.pop()
            task = asyncio.create_task(_process_job(response, slot))
            running_tasks.add(task)

            jobs_eaten = jobs_eaten + 1
            if max_jobs is not None and jobs_eaten == max_jobs:
                logging.warning(f"EAT: job limit {max_jobs} reached.")

        if not running_tasks:
            break

        done, running_tasks = await asyncio.wait(
            running_tasks, return_when=asyncio.FIRST_COMPLETED
        )

        for task in done:
            if (slot := task.result()) is not None:
                job_slots.add(slot)

    logging.info("* - - - - - - - - - - - - - - - - - *")
    logging.info("| da'mie has eaten his fill! (meow) |")
    logging.info("* - - - - - - - - - - - - - - - - - *")


@cli.command()
@click.pass_context
@asyncc
async def list(ctx):
    """list all bowls"""
    if response := await send_command("LIST:0:", ctx.obj['HOST'], ctx.obj['PORT']):
        click.echo(response)


@cli.command()
@click.argument('bowl', type=str)
@click.option('--max-retries', '-r', type=int, default=8)
@click.option('--command', '-c', type=str)
@click.pass_context
@asyncc
async def feed(ctx, bowl, max_retries, command):
    """add a new job to a bowl"""
    if response := await send_command(
        f"FEED:3:{bowl}:{max_retries}:{command}", ctx.obj['HOST'], ctx.obj['PORT']
    ):
        click.echo(response)


@cli.command()
@click.argument('bowl', type=str)
@click.pass_context
@asyncc
async def clean(ctx, bowl):
    """remove all jobs from a bowl"""
    if response := await send_command(
        f"CLEAN:1:{bowl}", ctx.obj['HOST'], ctx.obj['PORT']
    ):
        click.echo(response)


@cli.command()
@click.option(
    '--db-path',
    '-d',
    default='damie-stats.db',
    type=click.Path(dir_okay=False),
    help='path to sqlite3 database',
)
@click.option(
    '--check-interval',
    '-c',
    default=600,
    type=int,
    help='heartbeat check interval',
)
@click.option(
    '--response-limit',
    '-r',
    default=2000,
    type=int,
    help='maximum response time allowed',
)
@click.option(
    '--stats-interval',
    '-s',
    default=300,
    type=int,
    help='stats logging interval',
)
@click.pass_context
@asyncc
async def start(ctx, db_path, check_interval, response_limit, stats_interval):
    """server"""
    logging.info(f"[database] saving to database at '{db_path}'")

    try:
        server = await asyncio.start_server(
            lambda r, w: handle_request(r, w, db_path),
            ctx.obj['HOST'],
            ctx.obj['PORT'],
        )
        logging.info(f'listening on {server.sockets[0].getsockname()}')

        monitor_task = asyncio.create_task(
            monitor_active_jobs(check_interval, response_limit)
        )
        logging_task = asyncio.create_task(log_job_stats(db_path, stats_interval))

        await server.serve_forever()
    finally:
        logging.info("stopping tasks...")
        monitor_task.cancel()
        logging_task.cancel()
        logging.info("* - - - - - - - - - - - - - - - *")
        logging.info("| da'mie goes to sleep... (zzz) |")
        logging.info("* - - - - - - - - - - - - - - - *")


if __name__ == '__main__':
    cli(obj={})
