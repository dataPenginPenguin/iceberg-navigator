import click
from iceberg_navigator.commands import list as list_cmd
from iceberg_navigator.commands import show
from iceberg_navigator.commands import graph

@click.group()
def main():
    """Iceberg Snapshot Navigator CLI"""
    pass

main.add_command(list_cmd.list_snapshots)
main.add_command(show.show_snapshot)
main.add_command(graph.graph_snapshots)