import click
from presentation.cli.consumers import consumers
from presentation.cli.producers import producers


@click.group()
def cli() -> None:
    pass


if __name__ == "__main__":
    cli.add_command(producers)
    cli.add_command(consumers)
    cli()
