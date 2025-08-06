import click
from iceberg_navigator.aws.glue import GlueCatalog
from iceberg_navigator.utils.display import build_snapshot_graph, draw_graph

@click.command("graph")
@click.option("--table", required=True, help="Table name (e.g., db.table)")
@click.option("--output", default="snapshot_graph.png", help="Output image filename")
def graph_snapshots(table: str, output: str):
    glue_catalog = GlueCatalog()
    snapshots = glue_catalog.list_snapshots(table)
    if not snapshots:
        click.echo(f"No snapshots found for table {table}")
        return

    G = build_snapshot_graph(snapshots)
    draw_graph(G, output)
    click.echo(f"Snapshot graph saved to {output}")

if __name__ == "__main__":
    graph_snapshots()