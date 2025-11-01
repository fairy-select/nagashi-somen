from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime
from typing import ClassVar

import pymysql
import pymysql.cursors
from colorama import Fore, Style, init
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent

init(autoreset=True)  # Initialize colorama


class ColoredFormatter(logging.Formatter):
    COLORS: ClassVar[dict[str, str]] = {
        "DEBUG": Fore.BLUE,
        "INFO": Fore.GREEN,
        "WARNING": Fore.YELLOW,
        "ERROR": Fore.RED,
        "CRITICAL": Fore.RED + Style.BRIGHT,
    }

    def format(self, record):
        color = self.COLORS.get(record.levelname, "")
        record.levelname = f"{color}{record.levelname}{Style.RESET_ALL}"
        return super().format(record)


handler = logging.StreamHandler()
handler.setFormatter(ColoredFormatter("%(asctime)s - %(levelname)s - %(message)s"))
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)


class DatabaseMonitor:
    def __init__(self, config, database, output_dir, server_id=100):
        self.config = config
        self.database = database
        self.output_dir = output_dir
        self.server_id = server_id
        self.recording = True
        self.table_records = {}  # {table: {primary_key: record_dict}}
        self.table_schemas = {}  # {table: [column_names]}

        self._patch_show_master_status()

        # Create an output directory
        os.makedirs(self.output_dir, exist_ok=True)

    # noinspection PyMethodMayBeStatic
    def _patch_show_master_status(self):
        org_execute = pymysql.cursors.Cursor.execute

        def replace_query(self_: pymysql.cursors.Cursor, query: str, *args):
            if query == "SHOW MASTER STATUS":
                query = "SHOW BINARY LOG STATUS"
            return org_execute(self_, query, *args)

        pymysql.cursors.Cursor.execute = replace_query

    def check_mysql_configuration(self):
        """Check MySQL configuration for binlog support."""
        connection = None

        try:
            connection = pymysql.connect(**self.config)
            cursor = connection.cursor()

            # Check if binary logging is enabled
            cursor.execute("SHOW VARIABLES LIKE 'log_bin'")
            log_bin_result = cursor.fetchone()
            if not log_bin_result or log_bin_result[1].lower() != "on":
                logging.error("Binary logging is not enabled on MySQL server.")
                logging.info("Please add the following to your MySQL configuration:")
                logging.info("[mysqld]")
                logging.info("log-bin=mysql-bin")
                logging.info("server-id=1")
                logging.info("binlog-format=row")
                return False

            # Check binlog format
            cursor.execute("SHOW VARIABLES LIKE 'binlog_format'")
            binlog_format_result = cursor.fetchone()
            if binlog_format_result and binlog_format_result[1].upper() != "ROW":
                logging.error("Binary log format is not set to ROW.")
                logging.error("Current format: %s", binlog_format_result[1])
                logging.info("Please set binlog_format=ROW in your MySQL configuration.")
                return False

            # Check replication privileges
            cursor.execute("SHOW GRANTS")
            grants = cursor.fetchall()
            has_replication_slave = any("REPLICATION SLAVE" in str(grant[0]) for grant in grants)
            has_replication_client = any("REPLICATION CLIENT" in str(grant[0]) for grant in grants)

            if not (has_replication_slave and has_replication_client):
                logging.error("Missing replication privileges.")
                logging.info("Please grant the following privileges:")
                logging.info("GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '%s'@'%';", "[user]")
                logging.info("FLUSH PRIVILEGES;")
                return False

            logging.info("MySQL configuration looks good.")

        except pymysql.Error:
            logging.exception("Error checking MySQL configuration")
            return False

        finally:
            if connection:
                connection.close()

        return True

    # noinspection PyMethodMayBeStatic
    def get_primary_key(self, row):
        """Get primary key from row. Assumes primary key is 'id'."""
        return row.get("id")

    def handle_event(self, event):
        """Handle binlog events."""
        table = event.table
        self.table_records.setdefault(table, {})
        columns = self.table_schemas.get(table, [])

        if isinstance(event, WriteRowsEvent):
            for row in event.rows:
                values = dict(zip(columns, row["values"].values()))
                pk = self.get_primary_key(values)
                if pk is not None:
                    self.table_records[table][pk] = values

        elif isinstance(event, UpdateRowsEvent):
            for row in event.rows:
                after_values = dict(zip(columns, row["after_values"].values()))
                pk = self.get_primary_key(after_values)
                if pk is not None:
                    self.table_records[table][pk] = after_values

        elif isinstance(event, DeleteRowsEvent):
            for row in event.rows:
                values = dict(zip(columns, row["values"].values()))
                pk = self.get_primary_key(values)
                if pk is not None:
                    self.table_records[table].pop(pk, None)

    def save_changes(self):
        """Save the current state to JSON files."""

        class DateTimeEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                return super().default(obj)

        for table, records in self.table_records.items():
            arr = list(records.values())
            path = os.path.join(self.output_dir, f"{table}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(arr, f, ensure_ascii=False, indent=2, cls=DateTimeEncoder)
        logging.info("Snapshot saved.")

    def stop_recording(self):
        """Stop recording and save changes."""
        logging.info("Stopping monitor and saving JSON snapshots...")
        self.recording = False
        self.save_changes()

    def start(self):
        """Start monitoring binlog."""
        # Check MySQL configuration first
        if not self.check_mysql_configuration():
            logging.error("MySQL configuration check failed. Please fix the issues above before proceeding.")
            sys.exit(1)

        self._load_table_schemas()
        self._monitor()

    def _load_table_schemas(self):
        """Load table schemas from database."""
        connection = None

        try:
            connection = pymysql.connect(**self.config)
            cursor = connection.cursor()

            # noinspection SqlDialectInspection
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s", [self.database])
            tables = cursor.fetchall()

            for (table_name,) in tables:
                cursor.execute(f"SHOW COLUMNS FROM {self.database}.{table_name}")
                columns = cursor.fetchall()
                self.table_schemas[table_name] = [column[0] for column in columns]

        except pymysql.Error:
            logging.exception("Error loading table schemas")

        finally:
            if connection:
                connection.close()

        logging.info("Binlog monitoring started. Press Ctrl+C to stop.")

    def _monitor(self):
        stream = None

        try:
            stream = BinLogStreamReader(
                connection_settings=self.config,
                server_id=self.server_id,
                only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
                only_schemas=[self.database],
                blocking=True,
                resume_stream=True,
            )

            for binlog_event in stream:
                if not self.recording:
                    break
                self.handle_event(binlog_event)

        except pymysql.Error as e:
            logging.exception("MySQL Error")
            if "1064" in str(e):
                logging.exception("This might be due to MySQL version compatibility issues.")
                logging.exception("Make sure you're using a compatible MySQL server version.")

        except KeyboardInterrupt:
            self.stop_recording()

        except Exception:
            logging.exception("Unexpected error")
            raise

        finally:
            if stream:
                stream.close()
            logging.info("Binlog monitoring stopped.")


def start_monitoring(config, database, output_dir, server_id=100):
    """Start database monitoring with given configuration."""
    monitor = DatabaseMonitor(config, database, output_dir, server_id)
    monitor.start()
