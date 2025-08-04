import click
from iceberg_navigator.aws.glue import GlueCatalog
from iceberg_navigator.utils.display import format_snapshots_table

@click.command("list")
@click.option("--table", required=True, help="Table identifier, e.g. db.table")
def list_snapshots(table):

    glue = GlueCatalog()
    snapshots = glue.list_snapshots(table)
    if not snapshots:
        click.echo("No snapshots found.")
        return

    table_str = format_snapshots_table(snapshots)
    click.echo(table_str)
