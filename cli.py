import os
import shutil
import click
from importlib.resources import files


DEPEND_FOLDER = "depend"

@click.group()
def cli():
    """ArcSync CLI"""
    pass

@click.command()
def init():
    """Inittialize depend folder with default files"""
    if not os.path.exists(DEPEND_FOLDER):
        os.makedirs(DEPEND_FOLDER)
        click.echo(f"Created '{DEPEND_FOLDER}' folder.")
    else:
        click.echo(f"'{DEPEND_FOLDER}' already exists.")

    template_dir =files("arcsync.templates")
    for template_file in template_dir.iterdir():
        dest_file = os.path.joi(DEPEND_FOLDER, template_file.name)
        if not os.path.exists(dest_file):
            shutil.copy(template_file, dest_file)
            click.echo(f"Created {dest_file}")
        else:
            click.echo(f"{dest_file} already exist, skipping.")

        click.echo("Initialization complete")

if __name__ == "__main__":
    cli()
