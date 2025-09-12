import click
from infra.consumers.commands import consumers
from infra.producers.commands import producers


@click.group()
def cli() -> None:
    pass


if __name__ == "__main__":
    cli.add_command(producers)
    cli.add_command(consumers)
    cli()
