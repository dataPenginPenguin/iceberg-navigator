from tabulate import tabulate
from datetime import datetime
import click

def format_snapshots_table(snapshots):
    headers = ["Snapshot ID", "Timestamp", "Operation", "Parent Snapshot ID"]
    rows = []
    for snap in snapshots:
        ts = datetime.utcfromtimestamp(snap["timestamp"] / 1000).strftime("%Y-%m-%dT%H:%M:%SZ")
        rows.append([
            snap["snapshot_id"],
            ts,
            snap["operation"],
            snap["parent_id"] or "null"
        ])
    return tabulate(rows, headers=headers, tablefmt="github")


def show_snapshot_details(snapshot):
    """
    snapshot: dict, show_snapshotで返される辞書

    出力をclick.echoで行うようにしています。
    """
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