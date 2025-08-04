import click
import networkx as nx
import matplotlib.pyplot as plt
from iceberg_navigator.aws.glue import GlueCatalog

def build_snapshot_graph(snapshots):
    G = nx.DiGraph()
    for idx, snap in enumerate(snapshots, start=1):
        label = f"{idx}: {snap['snapshot_id']}\n({snap.get('operation', '')})"
        G.add_node(snap["snapshot_id"], label=label, idx=idx)
    for snap in snapshots:
        parent_id = snap.get("parent_id")
        if parent_id:
            G.add_edge(parent_id, snap["snapshot_id"])
    print(G)
    return G

def draw_graph(G, output_file):
    pos = nx.spring_layout(G)
    labels = nx.get_node_attributes(G, 'label')

    plt.figure(figsize=(12, 8))
    nx.draw_networkx_nodes(G, pos, node_color="skyblue", node_size=1500)
    nx.draw_networkx_edges(G, pos, arrows=True, arrowstyle='-|>', arrowsize=20)
    nx.draw_networkx_labels(G, pos, labels, font_size=5, font_weight='bold', verticalalignment='center')
    plt.title("Iceberg Snapshot Lineage")
    plt.axis('off')
    plt.tight_layout()
    plt.savefig(output_file)
    plt.close()


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