import asyncio
import json
import logging
import time
import uuid
from datetime import datetime

from damie.database import (
    create_job_table,
    create_stats_table,
    log_job_to_db,
    log_stats_to_db,
)


job_bowls = {}
active_jobs = {}
job_lock = asyncio.Lock()


async def handle_request(reader, writer, db_path):
    addr = writer.get_extra_info('peername')
    response = "ERROR: invalid command format"
    try:
        data = await reader.read(4096)
        request = data.decode().strip()
        if not request:
            return

        command, count_str, args = request.split(':', 2)
        count = max(int(count_str) - 1, 0)

        async with job_lock:
            if command == 'EAT':
                (bowl_name,) = args.split(':', count)
                if bowl_name in job_bowls and not job_bowls[bowl_name].empty():
                    job_details = await job_bowls[bowl_name].get()
                    job_uuid = job_details['uuid']

                    if bowl_name not in active_jobs:
                        active_jobs[bowl_name] = {}
                    active_jobs[bowl_name][job_uuid] = {
                        'last_heartbeat': time.time(),
                        'job_details': job_details,
                    }

                    response = json.dumps(
                        {
                            'uuid': job_uuid,
                            'bowl': job_details['bowl'],
                            'command': job_details['command'],
                        }
                    )
                    logging.info(f"EAT: {bowl_name}/{job_uuid} by worker {addr}")
                else:
                    response = 'EMPTY'
                    logging.info(f"EAT: bowl '{bowl_name}' empty")

            elif command == 'HEARTBEAT':
                (
                    bowl_name,
                    job_uuid,
                ) = args.split(':', count)
                if bowl_name in active_jobs and job_uuid in active_jobs[bowl_name]:
                    active_jobs[bowl_name][job_uuid]['last_heartbeat'] = time.time()
                    logging.info(f"HEARTBEAT: ACK {bowl_name}/{job_uuid}")
                else:
                    logging.warning(f"HEARTBEAT: {bowl_name}/{job_uuid} invalid")

            elif command == 'DIGESTED':
                (
                    bowl_name,
                    job_uuid,
                    exit_code_str,
                    runtime_str,
                ) = args.split(':', count)
                if bowl_name in active_jobs and job_uuid in active_jobs[bowl_name]:
                    job_details = active_jobs[bowl_name][job_uuid]['job_details']
                    job_details.update(
                        {
                            'worker_addr': str(addr),
                            'exit_code': int(exit_code_str),
                            'runtime_seconds': float(runtime_str),
                            'completion_time_utc': datetime.utcnow().isoformat() + 'Z',
                        }
                    )
                    del active_jobs[bowl_name][job_uuid]

                    asyncio.create_task(log_job_to_db(db_path, job_details))
                    response = f"SUCCESS: DIGESTED: {bowl_name}/{job_uuid}"
                    logging.info(f"DIGESTED: {bowl_name}/{job_uuid} by worker {addr}")
                else:
                    response = f"ERROR: DIGESTED: job {bowl_name}/{job_uuid} not found"
                    logging.info(f"DIGESTED: {bowl_name}/{job_uuid} not found")

            elif command == 'LIST':
                bowls = {name: q.qsize() for name, q in job_bowls.items()}
                response = json.dumps(bowls, indent=2)
                logging.info(f"LIST: {len(job_bowls)} bowls")

            elif command == 'FEED':
                (
                    bowl_name,
                    max_retries,
                    job_command,
                ) = args.split(':', count)
                if bowl_name.startswith('__'):
                    raise ValueError("names starting with '__' are reserved")

                if bowl_name not in job_bowls:
                    job_bowls[bowl_name] = asyncio.Queue()
                    await create_job_table(db_path, bowl_name)
                    logging.info(f"NEW: bowl '{bowl_name}'")

                job_uuid = str(uuid.uuid4())
                job_details = {
                    'uuid': job_uuid,
                    'bowl': bowl_name,
                    'command': job_command,
                    'submitter_addr': addr,
                    'timestamp_utc': time.time(),
                    'readable_time': datetime.utcfromtimestamp(time.time()).isoformat()
                    + 'Z',
                    'max_retries': int(max_retries),
                    'retry_count': 0,
                }
                await job_bowls[bowl_name].put(job_details)
                response = f"SUCCESS: FEED: {bowl_name}/{job_uuid}"
                logging.info(f"FEED: added {job_uuid} to bowl '{bowl_name}'")

            elif command == 'CLEAN':
                (bowl_name,) = args.split(':', count)
                if bowl_name in job_bowls:
                    job_count = job_bowls[bowl_name].qsize()
                    job_bowls[bowl_name] = asyncio.Queue()
                    response = (
                        f"SUCCESS: CLEAN: {job_count} jobs from bowl '{bowl_name}'"
                    )
                    logging.info(
                        f"CLEAN: removed {job_count} jobs from bowl '{bowl_name}'"
                    )
                else:
                    response = f"ERROR: CLEAN: bowl '{bowl_name}' not found"
                    logging.warning(f"CLEAN: bowl '{bowl_name}' not found")

            elif command == 'REMOVE':
                (bowl_name,) = args.split(':', count)
                if bowl_name in job_bowls:
                    job_count = [
                        job_bowls[bowl_name].qsize(),
                        len(active_jobs[bowl_name]),
                    ]
                    if not job_count:
                        del job_bowls[bowl_name]
                        if bowl_name in active_jobs:
                            del active_jobs[bowl_name]
                        response = f"SUCCESS: REMOVE: bowl '{bowl_name}' removed"
                        logging.info(f"REMOVE: bowl '{bowl_name}' removed")
                    else:
                        response = (
                            f"ERROR: REMOVE: bowl '{bowl_name}' not empty ({job_count})"
                        )
                        logging.warning(
                            f"REMOVE: bowl '{bowl_name}' not empty ({job_count})"
                        )
                else:
                    response = f"ERROR: REMOVE: bowl '{bowl_name}' not found"
                    logging.warning(f"REMOVE: bowl '{bowl_name}' not found")

            else:
                logging.warning(f"unknown command: {request}")

        writer.write(response.encode())
        await writer.drain()
    except Exception as e:
        logging.exception(f"connection from {addr}: {e}")
        writer.write(f"ERROR: SERVER: {e}".encode())
        await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def monitor_active_jobs(interval, response_limit):
    while True:
        await asyncio.sleep(interval)

        now = time.time()
        unresponsive_jobs = []

        active_jobs_snapshot = active_jobs.copy()
        for bowl_name, jobs in active_jobs_snapshot.items():
            for job_uuid, job_info in jobs.items():
                if now - job_info['last_heartbeat'] > response_limit:
                    unresponsive_jobs.append((bowl_name, job_uuid))

        if not unresponsive_jobs:
            continue

        async with job_lock:
            for bowl_name, job_uuid in unresponsive_jobs:
                if (
                    bowl_name not in active_jobs
                    or job_uuid not in active_jobs[bowl_name]
                ):
                    logging.error(f"{bowl_name}/{job_uuid} is untracked!")
                    continue

                job_details = active_jobs[bowl_name].pop(job_uuid)['job_details']
                if not active_jobs[bowl_name]:
                    del active_jobs[bowl_name]

                retry_count = job_details['retry_count'] + 1
                max_retries = job_details['max_retries']

                job_details['retry_count'] = retry_count

                if retry_count > max_retries:
                    logging.warning(
                        f"ACTIVE: {bowl_name}/{job_uuid} dropped (max retries {max_retries} exceeded)"
                    )
                else:
                    await job_bowls[bowl_name].put(job_details)
                    logging.warning(
                        f"ACTIVE: {bowl_name}/{job_uuid} readded to bowl '{bowl_name}' (retry {retry_count}/{max_retries})"
                    )


async def log_job_stats(db_path: str, interval: int):
    await create_stats_table(db_path)

    logging.info(f"[database] starting logger [{interval}s]")

    while True:
        timestamp = time.time()
        stats_data = []

        for bowl_name, q in job_bowls.items():
            n_queued_jobs = q.qsize()
            n_active_jobs = len(active_jobs.get(bowl_name, {}))
            stats_data.append((timestamp, bowl_name, n_queued_jobs, n_active_jobs))

        asyncio.create_task(log_stats_to_db(db_path, stats_data))

        await asyncio.sleep(interval)
