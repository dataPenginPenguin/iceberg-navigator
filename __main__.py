import click
from iceberg_navigator.commands.list import list_snapshots
from iceberg_navigator.commands.show import show_snapshot
from iceberg_navigator.commands.graph import graph_snapshots

@click.group()
def cli():
    """Iceberg Navigator CLI"""
    pass

cli.add_command(list_snapshots)
cli.add_command(show_snapshot)
cli.add_command(graph_snapshots)

if __name__ == "__main__":
    cli()
