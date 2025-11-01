# SPDX-FileCopyrightText: 2025-present Noritaka IZUMI <noritaka.izumi@gmail.com>
#
# SPDX-License-Identifier: MIT
import click

from nagashi_somen.__about__ import __version__


@click.group(context_settings={"help_option_names": ["-h", "--help"]}, invoke_without_command=True)
@click.version_option(version=__version__, prog_name="Nagashi Somen")
def nagashi_somen():
    click.echo("Hello world!")
