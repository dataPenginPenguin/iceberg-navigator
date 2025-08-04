# iceberg-navigator

A CLI tool to easily navigate and inspect Apache Iceberg snapshot histories.  
It uses PyIceberg with AWS Glue REST Catalog to list, show details, and visualize snapshot lineage graphs for Iceberg tables stored on S3.

---

## Overview

Apache Iceberg manages table versions via snapshots, enabling powerful data management and governance.  
This tool allows users to quickly explore Iceberg snapshot histories from the command line, tracing parent-child relationships and understanding snapshot details for better development and operational insights.

---

## Features

- **list**: List all snapshots of an Iceberg table
- **show**: Show detailed information for a specific snapshot (schema, partitions, summary)
- **graph**: Visualize snapshot lineage as a DAG and save as PNG

---

## Environment Requirements

- Python 3.8+
- Iceberg tables accessible via AWS Glue REST Catalog
- AWS credentials configured via environment variables or AWS CLI profiles

---

## Installation

```bash
git clone https://github.com/dataPenginPenguin/iceberg-navigator.git
cd iceberg-navigator
pip install -r requirements.txt
```

---

## Usage

### As a module

```bash
python -m iceberg_navigator <command> [options]
```

---

## Commands

---

### List snapshots

```bash
python -m iiceberg_navigator list --table <database>.<table>
```

### Example:

```bash
python -m iceberg_navigator list --table icebergdb.yellow_tripdata
|         Snapshot ID | Timestamp            | Operation        | Parent Snapshot ID   |
|---------------------|----------------------|------------------|----------------------|
| 1533347322559466931 | 2025-05-22T02:10:24Z | Operation.APPEND | null                 |
| 1485371543345582290 | 2025-05-22T02:10:54Z | Operation.DELETE | 1533347322559466931  |
|   67848960317145716 | 2025-05-22T02:15:45Z | Operation.APPEND | 1485371543345582290  |
| 3920289554540444894 | 2025-05-22T02:38:46Z | Operation.DELETE | 67848960317145716    |
| 6369576239134108166 | 2025-05-22T02:41:51Z | Operation.APPEND | 3920289554540444894  |
| 6216935665394419954 | 2025-05-22T02:41:54Z | Operation.APPEND | 6369576239134108166  |
| 9058990433822511495 | 2025-05-22T02:42:28Z | Operation.APPEND | 6216935665394419954  |
| 5224576979788468429 | 2025-05-22T02:46:53Z | Operation.DELETE | 9058990433822511495  |
| 8997131439115911397 | 2025-05-22T02:47:21Z | Operation.APPEND | 5224576979788468429  |
| 4246095293733855575 | 2025-08-02T22:51:16Z | Operation.DELETE | 8997131439115911397  |
| 8106328257365313720 | 2025-08-04T07:50:14Z | Operation.APPEND | 6369576239134108166  |
```

---

### Show snapshot details

```bash
python -m iceberg_navigator show <snapshot_id> --table <database>.<table>
```

### Example:

```bash
python -m iceberg_navigator show 1485371543345582290 --table icebergdb.yellow_tripdata

Table: yellow_tripdata

Snapshot ID: 1485371543345582290
Timestamp: 2025-08-04T07:50:14Z
Operation: Operation.APPEND
Parent Snapshot ID: 6369576239134108166
Manifest List: s3://your-bucket/warehouse/yellow_tripdata/metadata/snap-8106328257365313720-1-a4fb8059-7bf8-4254-b640-bf1fcbf100dd.avro

Schema:
  1: vendorid: optional int
  2: tpep_pickup_datetime: optional timestamp
  3: tpep_dropoff_datetime: optional timestamp
  4: passenger_count: optional long
  5: trip_distance: optional double
  6: ratecodeid: optional long
  7: store_and_fwd_flag: optional string
  8: pulocationid: optional int
  9: dolocationid: optional int
  10: payment_type: optional long
  11: fare_amount: optional double
  12: extra: optional double
  13: mta_tax: optional double
  14: tip_amount: optional double
  15: tolls_amount: optional double
  16: improvement_surcharge: optional double
  17: total_amount: optional double
  18: congestion_surcharge: optional double
  19: airport_fee: optional double

Summary:
  added-data-files: 1
  total-equality-deletes: 0
  added-records: 1
  total-position-deletes: 0
  added-files-size: 3046
  total-delete-files: 0
  total-files-size: 14138545
  total-data-files: 2
  total-records: 729733
```

---

### Generate lineage graph

```bash
python -m iceberg_navigator show <snapshot_id> --table <database>.<table>
```

### Example:

```bash
python -m iceberg_navigator graph --table icebergdb.yellow_tripdata

DiGraph with 11 nodes and 10 edges
Snapshot graph saved to snapshot_graph.png
```

![](https://storage.googleapis.com/zenn-user-upload/14d88153d9e1-20250804.png)

---
