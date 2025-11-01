# SPDX-FileCopyrightText: 2025-present Noritaka IZUMI <noritaka.izumi@gmail.com>
#
# SPDX-License-Identifier: MIT
import click

from nagashi_somen.__about__ import __version__


@click.group(context_settings={"help_option_names": ["-h", "--help"]}, invoke_without_command=True)
@click.version_option(version=__version__, prog_name="Nagashi Somen")
@click.pass_context
def nagashi_somen(ctx):
    """Capture database changes and save them to a file."""
    if ctx.invoked_subcommand is None:
        click.echo("Use 'nagashi-somen monitor' to start monitoring database changes.")
        click.echo("Run 'nagashi-somen --help' for more information.")


@nagashi_somen.command()
@click.option("--host", default="localhost", help="Database host")
@click.option("--port", default=3306, help="Database port")
@click.option("--user", default="root", help="Database user")
@click.option("--password", prompt=True, hide_input=True, help="Database password")
@click.option("--database", required=True, help="Database name to monitor")
@click.option("--output-dir", default="./logs", help="Output directory for JSON files")
@click.option("--server-id", default=100, help="MySQL replication server ID")
def monitor(host, port, user, password, database, output_dir, server_id):
    """Start monitoring database changes."""
    from nagashi_somen.core import start_monitoring

    config = {
        "host": host,
        "port": port,
        "user": user,
        "passwd": password,
    }

    start_monitoring(config, database, output_dir, server_id)
