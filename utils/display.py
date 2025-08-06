from tabulate import tabulate
from datetime import datetime
import click
import networkx as nx
import matplotlib.pyplot as plt

def format_snapshots_table(snapshots):
    headers = ["Snapshot ID", "Timestamp", "Operation", "Parent Snapshot ID", "Total Size (MB)", "Record Count"]
    rows = []
    for snap in snapshots:
        ts = datetime.utcfromtimestamp(snap["timestamp"] / 1000).strftime("%Y-%m-%dT%H:%M:%SZ")
        rows.append([
            snap["snapshot_id"],
            ts,
            snap["operation"],
            snap["parent_id"] or "null",
            format_number(snap["total_size_mb"]),
            format_number(snap["record_count"])
        ])
    colalign = ("left", "left", "left", "left", "right", "right")
    return tabulate(rows, headers=headers, tablefmt="github", floatfmt=".2f", colalign=colalign)


def show_snapshot_details(snapshot):
    ts = datetime.utcfromtimestamp(snapshot['timestamp'] / 1000).strftime("%Y-%m-%dT%H:%M:%SZ")

    click.echo(f"Table: {snapshot.get('table', 'Unknown')}\n")
    click.echo(f"Snapshot ID: {snapshot['snapshot_id']}")
    click.echo(f"Timestamp: {ts}")
    click.echo(f"Operation: {snapshot['operation']}")
    click.echo(f"Parent Snapshot ID: {snapshot['parent_id'] or 'None'}")
    click.echo(f"Manifest List: {snapshot['manifest_list']}\n")

    click.echo("Schema:")
    for col in snapshot['schema']:
        click.echo(f"  {col}")
    click.echo("")

    click.echo("Summary:")

    summary_keys = [
        "added-data-files",
        "total-equality-deletes",
        "added-records",
        "total-position-deletes",
        "added-files-size",
        "total-delete-files",
        "total-files-size",
        "total-data-files",
        "total-records",
    ]

    summary = snapshot.get("summary", {})

    printed_any = False
    for key in summary_keys:
        value = summary.get(key)
        if value is not None:
            click.echo(f"  {key}: {value}")
            printed_any = True

    if not printed_any:
        click.echo("  (No summary data)")

def compare_snapshot(result):

    click.echo("-" * 40)
    click.echo("Parent Snapshot")
    click.echo("-" * 40)
    click.echo(f"ID:         {result['parent_snapshot_id']}")
    click.echo(f"File Size:  {format_mb(result['parent_size'])} MB")
    click.echo(f"Records:    {format_number(result['parent_records'])}")
    click.echo("")

    click.echo("-" * 40)
    click.echo("Current Snapshot")
    click.echo("-" * 40)
    click.echo(f"ID:         {result['current_snapshot_id']}")
    click.echo(f"File Size:  {format_mb(result['current_size'])} MB")
    click.echo(f"Records:    {format_number(result['current_records'])}")
    click.echo("")

    click.echo("=" * 40)
    click.echo("Summary")
    click.echo("=" * 40)
    click.echo(f"Added Records:   {format_number(result['added'])}")
    click.echo(f"Deleted Records: {format_number(result['deleted'])}")
    click.echo("")

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

def format_number(n):
    return f"{n:,}"

def format_mb(bytes_val):
    return f"{bytes_val / (1024 * 1024):.2f}"