import asyncio
import json
import logging
import os
import time

import click


async def send_command(message, host, port):
    reader, writer = None, None
    try:
        reader, writer = await asyncio.open_connection(host, port)
        writer.write(message.encode())
        await writer.drain()
        response_data = await reader.read(8192)
        return response_data.decode()
    except ConnectionRefusedError:
        raise click.ClickException(f"connection refused by {host}:{port}")
    except Exception as e:
        raise click.ClickException(f"ERROR: {e}")
    finally:
        if writer is not None and not writer.is_closing():
            writer.close()
            await writer.wait_closed()

    return None


async def run_heartbeat_loop(job_uuid, host, port, interval):
    while True:
        await asyncio.sleep(interval)
        logging.info(f"sending heartbeat for job {job_uuid}...")
        await send_command(f'HEARTBEAT:{job_uuid}', host, port)


async def execute_job(command, env=None):
    logging.info(f"starting job: {command}")
    start = time.monotonic()

    try:
        process = await asyncio.create_subprocess_shell(command, env=env)
        await process.wait()

        runtime = time.monotonic() - start
        if (status := process.returncode) == 0:
            logging.info(f"SUCCESS: '{command}' [{runtime:.4f}s] [{status}]")
        else:
            logging.error(f"FAILURE: '{command}' [{runtime:.4f}s] [{status}]")
        return process.returncode, runtime
    except Exception as e:
        logging.error(f"ERROR: job '{command}': {e}")
        return -1, time.monotonic() - start


async def process_job(host, port, heartbeat_interval, job_response, slot):
    try:
        data = json.loads(job_response)
        bowl, job_uuid, command = data['bowl'], data['uuid'], data['command']
        logging.info(f"{bowl}/{job_uuid} received")
    except (json.JSONDecodeError, KeyError):
        logging.error(f"malformed job data: {job_response}")
        return

    heartbeat_task = asyncio.create_task(
        run_heartbeat_loop(job_uuid, host, port, heartbeat_interval)
    )

    try:
        job_env = os.environ.copy()
        job_env['__DAMIE_JOB_SLOT'] = str(slot)
        logging.info(f"EAT: {bowl}/{job_uuid} assigned slot: {slot}")

        status, runtime = await execute_job(command, env=job_env)
    finally:
        heartbeat_task.cancel()

    logging.info(f"DIGESTED: {bowl}/{job_uuid} digested with status {status}")
    await send_command(
        f'DIGESTED:4:{bowl}:{job_uuid}:{status}:{runtime:.4f}', host, port
    )

    return slot
