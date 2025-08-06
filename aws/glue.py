from urllib.parse import urlparse
from pyiceberg.catalog import load_catalog

class GlueCatalog:
    def __init__(self, profile_name=None, region_name=None, catalog_id="AwsDataCatalog"):
        import boto3
        if not region_name:
            session = boto3.Session(profile_name=profile_name)
            region_name = session.region_name
            if not region_name:
                raise ValueError("region_name Error")
        self.region_name = region_name
        self.catalog_id = catalog_id

        session = boto3.Session(profile_name=profile_name, region_name=region_name)
        self.glue_client = session.client("glue", region_name=region_name)

    def _get_catalog(self):
        conf = {
            "type": "rest",
            "uri": f"https://glue.{self.region_name}.amazonaws.com/iceberg",
            "s3.region": self.region_name,
            "rest.sigv4-enabled": "true",
            "rest.signing-name": "glue",
            "rest.signing-region": self.region_name,
        }
        return load_catalog(**conf)

    def get_table_location(self, table_identifier: str) -> str:
        database, table = table_identifier.split(".", 1)
        resp = self.glue_client.get_table(DatabaseName=database, Name=table)
        return resp["Table"]["Parameters"]["metadata_location"]

    def list_snapshots(self, table_identifier: str):
        catalog = self._get_catalog()
        namespace, table_name = table_identifier.split(".", 1)
        table = catalog.load_table(f"{namespace}.{table_name}")

        snapshots = []
        for snap in table.snapshots():
            total_bytes = int(snap.summary.get("total-files-size", 0)) if snap.summary else 0
            total_records = int(snap.summary.get("total-records", 0)) if snap.summary else 0

            snapshots.append({
                "snapshot_id": str(snap.snapshot_id),
                "timestamp": snap.timestamp_ms,
                "operation": snap.summary.get("operation") if snap.summary else None,
                "parent_id": str(snap.parent_snapshot_id) if snap.parent_snapshot_id else None,
                "total_size_mb": round((total_bytes) / (1024 * 1024), 2),
                "record_count": total_records
            })

        return snapshots

    def show_snapshot(self, table_identifier: str, snapshot_id: str):
        catalog = self._get_catalog()
        namespace, table_name = table_identifier.split(".", 1)
        table = catalog.load_table(f"{namespace}.{table_name}")

        snap = table.snapshot_by_id(int(snapshot_id))
        if not snap:
            return {"error": f"snapshot_id {snapshot_id} not found"}

        schema_columns = []
        for idx, col in enumerate(table.schema().columns, start=1):
            requiredness = "optional" if col.optional else "required"
            schema_columns.append(f"{idx}: {col.name}: {requiredness} {col.field_type}")

        summary_dict = {}
        if snap.summary:
            summary_dict["operation"] = snap.summary.operation
            if hasattr(snap.summary, "additional_properties"):
                summary_dict.update(snap.summary.additional_properties)


        return {
            "table": table_name,
            "snapshot_id": str(snap.snapshot_id),
            "timestamp": snap.timestamp_ms,
            "operation": summary_dict.get("operation"),
            "parent_id": str(snap.parent_snapshot_id) if snap.parent_snapshot_id else None,
            "manifest_list": snap.manifest_list,
            "schema": schema_columns,
            "summary": summary_dict,
        }

    def compare_snapshots(self, table_identifier: str, snapshot_id: str):
        catalog = self._get_catalog()
        namespace, table_name = table_identifier.split(".", 1)
        table = catalog.load_table(f"{namespace}.{table_name}")

        current_snap = table.snapshot_by_id(int(snapshot_id))
        if not current_snap:
            return {"error": f"snapshot_id {snapshot_id} not found"}

        parent_snap = table.snapshot_by_id(int(current_snap.parent_snapshot_id))
        if not parent_snap:
            return {"error": f"parent_snapshot not found"}

        current_summary_dict = {}
        if current_snap.summary:
            current_summary_dict["operation"] = current_snap.summary.operation
            if hasattr(current_snap.summary, "additional_properties"):
                current_summary_dict.update(current_snap.summary.additional_properties)

        parent_summary_dict = {}
        if parent_snap.summary:
            parent_summary_dict["operation"] = parent_snap.summary.operation
            if hasattr(parent_snap.summary, "additional_properties"):
                parent_summary_dict.update(parent_snap.summary.additional_properties)


        current_size = int(current_snap.summary.get("total-files-size", 0))
        current_records = int(current_snap.summary.get("total-records", 0))

        parent_size = int(parent_snap.summary.get("total-files-size", 0))
        parent_records = int(parent_snap.summary.get("total-records", 0))

        added = current_records - parent_records if current_records > parent_records else 0
        deleted = parent_records - current_records if parent_records > current_records else 0

        return {
                "current_snapshot_id": str(current_snap.snapshot_id),
                "current_size": current_size,
                "current_records": current_records,
                "parent_snapshot_id": str(parent_snap.snapshot_id),
                "parent_size": parent_size,
                "parent_records": parent_records,
                "added": added,
                "deleted": deleted,
            }