import asyncio
import logging
import re
import sqlite3


def sanitize_table_name(name: str, prefix: str):
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    return f"{prefix}_{sanitized}"


def _create_job_table_impl(db_path: str, table_name: str):
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f'''CREATE TABLE IF NOT EXISTS {table_name} (
                    uuid TEXT PRIMARY KEY,
                    bowl TEXT,
                    command TEXT,
                    submitter_addr TEXT,
                    timestamp_utc REAL,
                    readable_time TEXT,
                    worker_addr TEXT,
                    exit_code INTEGER,
                    runtime_seconds REAL,
                    completion_time_utc TEXT
                )'''
            )
            conn.commit()
            logging.info(f"[database] created table '{table_name}'")
    except sqlite3.Error as e:
        logging.exception(f"[database] create failed for table: '{table_name}'\n{e}")


def _log_job_impl(db_path: str, table_name: str, job_data: dict):
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f'''INSERT OR REPLACE INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (
                    job_data['uuid'],
                    job_data['bowl'],
                    job_data['command'],
                    str(job_data['submitter_addr']),
                    job_data['timestamp_utc'],
                    job_data['readable_time'],
                    job_data['worker_addr'],
                    job_data['exit_code'],
                    job_data['runtime_seconds'],
                    job_data['completion_time_utc'],
                ),
            )
            conn.commit()
            logging.debug(
                f"[database] logged job {job_data['bowl']}/{job_data['uuid']} to table '{table_name}'"
            )
    except sqlite3.Error as e:
        logging.exception(
            f"[database] write failed for job: {job_data['bowl']}/{job_data['uuid']}\n{e}"
        )


def _create_stats_table_impl(db_path: str):
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS __stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL NOT NULL,
                    bowl_name TEXT NOT NULL,
                    queue_size INTEGER NOT NULL,
                    active_jobs_count INTEGER NOT NULL
                )
            '''
            )
            conn.commit()
            logging.info("[database] created table '__stats'")
    except sqlite3.Error as e:
        logging.exception(f"[database] create failed for table: '__stats'\n{e}")


def _log_stats_impl(db_path: str, stats_data: list):
    if not stats_data:
        return

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.executemany(
                "INSERT INTO __stats (timestamp, bowl_name, queue_size, active_jobs_count) VALUES (?, ?, ?, ?)",
                stats_data,
            )
            conn.commit()
            logging.debug("[database] logged stats")
    except sqlite3.Error as e:
        logging.exception(f"[database] write failed for table: '__stats'\n{e}")


async def create_job_table(db_path: str, bowl_name: str):
    table_name = sanitize_table_name(bowl_name, 'bowl')
    await asyncio.to_thread(_create_job_table_impl, db_path, table_name)


async def log_job_to_db(db_path: str, job_data: dict):
    table_name = sanitize_table_name(job_data['bowl'], 'bowl')
    await asyncio.to_thread(_log_job_impl, db_path, table_name, job_data)


async def create_stats_table(db_path: str):
    await asyncio.to_thread(_create_stats_table_impl, db_path)


async def log_stats_to_db(db_path: str, stats_data: list):
    await asyncio.to_thread(_log_stats_impl, db_path, stats_data)
