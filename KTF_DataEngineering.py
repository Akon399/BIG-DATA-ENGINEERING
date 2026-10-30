# Databricks notebook source
# Project lifecycle

# Phase                                     Status

# Bronze ingestion and raw preservation	    Complete
# Silver staging and standardisation	    Complete
# Course/facility lookup joins	            Complete; mapping exceptions need review
# Participant identity and deduplication	Now
# Reportability rules	                    Next
# Gold reporting and reconciliation	        Pending
# README, design document and submission	Pending

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer.

# COMMAND ----------

# Objective of BRONZE is to preserve the original source files, inspect their structures,
# ingest their records into separate Delta tables and validate that no data was lost.


from pathlib import Path # File and folder management
import hashlib # Data integrity verification = 
               # Create a digital fingerprint of a file.
import shutil # Copying and moving files
import zipfile

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType
)

CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"

VOLUME_PATH = Path(
    "/Volumes/workspace/default/ktf_data_sources"
)

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS "
    f"`{CATALOG}`.`{BRONZE_SCHEMA}`"
)

print("Source volume:", VOLUME_PATH)
print("Bronze schema:", f"{CATALOG}.{BRONZE_SCHEMA}")

# This creates workspace.bronze, where your Delta tables will be stored. The source files remain in the default schema's Volume.

# COMMAND ----------

# Lets inspect the uploaded files


def show_files(folder, indent=0):
    for item in sorted(folder.iterdir()):
        print(" " * indent + item.name)
        if item.is_dir():
            show_files(item, indent + 4)

show_files(VOLUME_PATH)

# COMMAND ----------


zip_path = VOLUME_PATH / "Training Data.zip"

if zip_path.exists():
    with zipfile.ZipFile(zip_path) as archive:
        for member in archive.infolist():
            destination = (
                VOLUME_PATH / member.filename
            ).resolve()

            if not destination.is_relative_to(
                VOLUME_PATH.resolve()
            ):
                raise ValueError(
                    "Unsafe ZIP path: " + member.filename
                )

        archive.extractall(VOLUME_PATH)

    print("ZIP extracted successfully.")
else:
    print("No Training_Data.zip found.")

# COMMAND ----------

matches = list(
    VOLUME_PATH.rglob("Course and Facility Look Ups.xlsx")
)

if len(matches) != 1:
    raise ValueError(
        f"Expected exactly one lookup workbook, "
        f"found {len(matches)}"
    )

SOURCE_ROOT = matches[0].parent

print("Detected source directory:", SOURCE_ROOT)
show_files(SOURCE_ROOT)


# COMMAND ----------

# Validate all six training source files
# We must fail immediately if any required source file is missing or empty.


required_files = [
    "Course and Facility Look Ups.xlsx",
    "Online Data Export 17 Dec_SCRUBBED.csv",
    "Capturing Tool_V1c V2-27 January 2026_SCRUBBED.xlsx",
]

chw_dir = (
    SOURCE_ROOT / "Community Health Worker Completions"
)

chw_names = [
    "CHW Training Attendance_KESS_December 2025_SCRUBBED.xlsx",
    "CHW Training Attendance-West Coast-Oct 2025_SCRUBBED.xlsx",
    "WITZENBERG - July-Dec 2025_SCRUBBED.xlsx"
]

paths = (
    [SOURCE_ROOT / name for name in required_files]
    + [chw_dir / name for name in chw_names]
)

for path in paths:
    if not path.is_file():
        raise FileNotFoundError(f"Missing: {path}")

    if path.stat().st_size == 0:
        raise ValueError(f"Empty file: {path}")

print("All 6 source files are present and nonempty.")

# COMMAND ----------

# openpyxl = Excel inspection

%pip install openpyxl

# COMMAND ----------

import openpyxl

excel_paths = [
    path for path in paths
    if path.suffix.lower() == ".xlsx"
]

for file_path in excel_paths:
    print("\nFILE:", file_path.name)

    wb = openpyxl.load_workbook(
        file_path,
        read_only=True,
        data_only=True
    )

    for ws in wb.worksheets:
        header = next(
            ws.iter_rows(
                min_row=1,
                max_row=1,
                values_only=True
            ),
            ()
        )

        print("Sheet:", ws.title)
        print("Rows:", ws.max_row)
        print("Columns:", ws.max_column)
        print("Headers:", header)

    wb.close()

# COMMAND ----------

# Create a reusable Excel reader
# Because the Excel workbooks are relatively small, create Spark DataFrames.

from datetime import date, datetime

def read_excel_sheet(file_path, sheet_name):

    wb = openpyxl.load_workbook(
        file_path,
        read_only=True,
        data_only=True
    )

    try:
        ws = wb[sheet_name]
        iterator = ws.iter_rows(values_only=True)

        headers = list(next(iterator))

        if any(h is None or str(h).strip() == ""
               for h in headers):
            raise ValueError(
                f"Blank column header: {sheet_name}"
            )

        headers = [str(h) for h in headers]

        if len(headers) != len(set(headers)):
            raise ValueError(
                f"Duplicate headers: {sheet_name}"
            )

        def to_text(value):
            if value is None:
                return None
            if isinstance(value, (date, datetime)):
                return value.isoformat()
            return str(value)

        rows = [
            tuple(to_text(value) for value in row)
            for row in iterator
        ]

        if not rows:
            raise ValueError(
                f"Empty worksheet: {sheet_name}"
            )

        schema = StructType([
            StructField(name, StringType(), True)
            for name in headers
        ])

        return spark.createDataFrame(rows, schema)

    finally:
        wb.close()

# COMMAND ----------

# Ingest the online-school CSV


online_path = (
    SOURCE_ROOT /
    "Online Data Export 17 Dec_SCRUBBED.csv"
)

online_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .option("mode", "FAILFAST")
    .option("encoding", "UTF-8")
    .csv(str(online_path))
)

if len(online_df.columns) != 27:
    raise ValueError(
        "Online-school schema does not match expected width"
    )

online_count = online_df.count()

if online_count != 25824:
    raise ValueError(
        f"Online-school row count mismatch: {online_count}"
    )

print("Rows:", online_count)
print("Columns:", len(online_df.columns))

online_df.printSchema()


# COMMAND ----------

# Ingest the known Excel worksheets


lookup_file = (
    SOURCE_ROOT / "Course and Facility Look Ups.xlsx"
)

capturing_file = (
    SOURCE_ROOT /
    "Capturing Tool_V1c V2-27 January 2026_SCRUBBED.xlsx"
)

excel_sources = {
    "bronze_course_lookup": (lookup_file, "LU_Courses"),
    "bronze_facility_lookup": (lookup_file, "LU_Facility"),
    "bronze_capturer1": (capturing_file, "Capturer1"),
    "bronze_capturer2": (capturing_file, "Capturer2"),
    "bronze_capturer3": (capturing_file, "Capturer3")
}

bronze_dataframes = {
    "bronze_online_school": online_df
}

for table_name, (file_path, sheet) in excel_sources.items():
    df = read_excel_sheet(file_path, sheet)
    bronze_dataframes[table_name] = df

    print(
        table_name,
        "rows:", df.count(),
        "columns:", len(df.columns)
    )
# This prepares the six known logical datasets for writing to Bronze.
# We  have not merged them or applied cleaning.

# COMMAND ----------

# Inspect the CHW schemas before ingestion


chw_schemas = {}

for name in chw_names:
    file_path = chw_dir / name

    wb = openpyxl.load_workbook(
        file_path,
        read_only=True,
        data_only=True
    )

    print("\nFILE:", name)

    for ws in wb.worksheets:
        headers = tuple(
            str(v) if v is not None else ""
            for v in next(
                ws.iter_rows(
                    min_row=1,
                    max_row=1,
                    values_only=True
                ),
                ()
            )
        )

        print(
            f"Sheet: {ws.title} | "
            f"Rows: {ws.max_row} | "
            f"Columns: {len(headers)}"
        )
        print("Headers:", headers)

        chw_schemas[(name, ws.title)] = headers

    wb.close()


# COMMAND ----------

# We must use this configuration to ingest the correct worksheets


chw_sheet_names = {
    "bronze_chw_kess": "Training Attendance",
    "bronze_chw_west_coast": "Training Attendance",
    "bronze_chw_witzenberg": "Training Attendance"
}

chw_table_files = {
    "bronze_chw_kess": chw_names[0],
    "bronze_chw_west_coast": chw_names[1],
    "bronze_chw_witzenberg": chw_names[2]
}

# COMMAND ----------

from datetime import date, datetime
from pyspark.sql.types import (
    StructType, StructField, StringType
)

def read_chw_bronze(file_path, sheet_name):

    wb = openpyxl.load_workbook(
        file_path,
        read_only=True,
        data_only=True
    )

    try:
        ws = wb[sheet_name]

        column_count = ws.max_column

        if column_count != 49:
            raise ValueError(
                f"Expected 49 columns in {file_path.name}, "
                f"found {column_count}"
            )

        column_names = [
            f"column_{i:02d}"
            for i in range(1, column_count + 1)
        ]

        schema = StructType([
            StructField(name, StringType(), True)
            for name in column_names
        ])

        def to_text(value):
            if value is None:
                return None
            if isinstance(value, (date, datetime)):
                return value.isoformat()
            return str(value)

        rows = [
            tuple(to_text(value) for value in row)
            for row in ws.iter_rows(values_only=True)
        ]

        if not rows:
            raise ValueError(
                f"Empty CHW worksheet: {file_path.name}"
            )

        return spark.createDataFrame(rows, schema)

    finally:
        wb.close()
# This keeps the original header and data rows together in the Bronze table.

# COMMAND ----------

# Write the Bronze Delta tables


chw_sheet_names = {
    "bronze_chw_kess": "Training Attendance",
    "bronze_chw_west_coast": "Training Attendance",
    "bronze_chw_witzenberg": "Training Attendance"
}

chw_table_files = {
    "bronze_chw_kess": chw_names[0],
    "bronze_chw_west_coast": chw_names[1],
    "bronze_chw_witzenberg": chw_names[2]
}

# Actually load the three CHW DataFrames, after defining read_chw_bronze()


for table_name, filename in chw_table_files.items():

    file_path = chw_dir / filename
    sheet_name = chw_sheet_names[table_name]

    # Read the Excel worksheet
    df = read_chw_bronze(
        file_path,
        sheet_name
    )

    # Store the DataFrame for Bronze ingestion
    bronze_dataframes[table_name] = df

    print(
        f"{table_name}: "
        f"{df.count()} rows, "
        f"{len(df.columns)} columns"
    )


# COMMAND ----------

# Verify all nine DataFrames
# Before writing any Delta tables, run:


expected_tables = {
    "bronze_online_school",
    "bronze_course_lookup",
    "bronze_facility_lookup",
    "bronze_capturer1",
    "bronze_capturer2",
    "bronze_capturer3",
    "bronze_chw_kess",
    "bronze_chw_west_coast",
    "bronze_chw_witzenberg"
}

actual_tables = set(bronze_dataframes.keys())

missing = expected_tables - actual_tables

print("DataFrames loaded:", len(actual_tables))
print("Missing DataFrames:", missing)

if missing:
    raise ValueError(
        f"Missing Bronze DataFrames: {missing}"
    )

print("All nine Bronze DataFrames are ready.")

# COMMAND ----------

# # Write Bronze tables with column mapping
# Delta column mapping allows column names containing spaces, parentheses and other characters.

# CELL 9: Write Bronze Delta tables
# Preserve original source column names

expected_tables = {
    "bronze_online_school",
    "bronze_course_lookup",
    "bronze_facility_lookup",
    "bronze_capturer1",
    "bronze_capturer2",
    "bronze_capturer3",
    "bronze_chw_kess",
    "bronze_chw_west_coast",
    "bronze_chw_witzenberg"
}

if set(bronze_dataframes) != expected_tables:
    raise ValueError(
        "Missing Bronze DataFrames: "
        f"{expected_tables - set(bronze_dataframes)}"
    )

source_counts = {}

# Validate all nine source DataFrames
for name, df in bronze_dataframes.items():
    count = df.count()

    if count == 0:
        raise ValueError(f"Empty dataset: {name}")

    source_counts[name] = count

print("All nine source DataFrames validated.")

# Write Delta tables with column mapping
for name, df in bronze_dataframes.items():

    table_name = (
        f"`{CATALOG}`.`{BRONZE_SCHEMA}`.`{name}`"
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.columnMapping.mode", "name")
        .option("delta.minReaderVersion", "2")
        .option("delta.minWriterVersion", "5")
        .saveAsTable(table_name)
    )

    print(
        f"Created: {CATALOG}.{BRONZE_SCHEMA}.{name} "
        f"({source_counts[name]} rows)"
    )

print("All nine Bronze Delta tables written.")


# COMMAND ----------

# Reconcile source and Bronze records


# CELL 10: Bronze row-count reconciliation

from pyspark.sql import functions as F

reconciliation = []

for name, expected_count in source_counts.items():

    table_name = (
        f"`{CATALOG}`.`{BRONZE_SCHEMA}`.`{name}`"
    )

    actual_count = spark.table(table_name).count()

    status = (
        "PASS" if actual_count == expected_count
        else "FAIL"
    )

    reconciliation.append((
        name,
        expected_count,
        actual_count,
        status
    ))

result = spark.createDataFrame(
    reconciliation,
    [
        "table_name",
        "source_rows",
        "bronze_rows",
        "status"
    ]
)

display(result.orderBy("table_name"))

failed_count = result.filter(
    F.col("status") == "FAIL"
).count()

if failed_count > 0:
    raise AssertionError(
        f"{failed_count} Bronze reconciliation checks failed"
    )

print("All Bronze row-count checks passed.")


# COMMAND ----------

# Verify all nine tables exist


# CELL 11: Verify Bronze tables

tables = spark.sql(
    f"SHOW TABLES IN `{CATALOG}`.`{BRONZE_SCHEMA}`"
)

display(tables)

actual_tables = {
    row.tableName
    for row in tables.collect()
}

missing = expected_tables - actual_tables

if missing:
    raise AssertionError(
        f"Missing Bronze tables: {missing}"
    )

print("All nine Bronze tables exist.")


# COMMAND ----------

# Check for missing or unexpected columns


# CELL 12: Bronze schema reconciliation

schema_checks = []

for name, source_df in bronze_dataframes.items():

    table_name = (
        f"`{CATALOG}`.`{BRONZE_SCHEMA}`.`{name}`"
    )

    bronze_df = spark.table(table_name)

    source_columns = source_df.columns
    bronze_columns = bronze_df.columns

    status = (
        "PASS" if source_columns == bronze_columns
        else "FAIL"
    )

    schema_checks.append((
        name,
        len(source_columns),
        len(bronze_columns),
        status
    ))

schema_result = spark.createDataFrame(
    schema_checks,
    [
        "table_name",
        "source_columns",
        "bronze_columns",
        "status"
    ]
)

display(schema_result.orderBy("table_name"))

if schema_result.filter(
    F.col("status") == "FAIL"
).count() > 0:
    raise AssertionError(
        "Bronze schema reconciliation failed"
    )

print("All Bronze column checks passed.")

# This verifies column counts and exact column-name ordering, not just the number of columns.

# COMMAND ----------

# Preview the Bronze tables


# Preview online-school data

display(
    spark.table(
        "workspace.bronze.bronze_online_school"
    ).limit(5)
)

# Preview CHW data

display(
    spark.table(
        "workspace.bronze.bronze_chw_kess"
    ).limit(5)
)


# COMMAND ----------

# What follows after Bronze?

# 1. Source file validation  
#    Six physical files located and verified as nonempty.

# 2. Bronze DataFrame preparation 
#    Nine DataFrames loaded.

# 3. Bronze Delta ingestion
#    Write nine Delta tables.

# 4. Bronze reconciliation
#    Compare source and Delta row counts.

# 5. Bronze verification
#    Confirm the tables exist in Unity Catalog

# SILVER LAYER.

# Before Silver, we'll strengthen source traceability, then create a Silver architecture aligned with the assessment requirements: structure conformance, course and facility mapping, profession standardisation, participant identity matching, deduplication, reportable rules and reconciling reports.

# COMMAND ----------

# Bronze enhancement — Source file audit manifest

# We will create a Delta table named workspace.bronze.source_file_manifest.
# It will record each file's location, size, SHA-256 hash and ingestion metadata. A hash is a digital fingerprint: it helps us detect whether a source file has changed.

# Generate file hashes


from pathlib import Path
from datetime import datetime, timezone
import hashlib

from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType
)

def calculate_sha256(file_path):
    sha = hashlib.sha256()

    with open(file_path, "rb") as file:
        while True:
            chunk = file.read(1024 * 1024)

            if not chunk:
                break

            sha.update(chunk)

    return sha.hexdigest()


audit_time = datetime.now(timezone.utc).isoformat()

manifest_rows = []

for file_path in paths:

    relative_path = str(
        file_path.relative_to(SOURCE_ROOT)
    )

    manifest_rows.append((
        file_path.name,
        relative_path,
        str(file_path),
        file_path.suffix.lower(),
        file_path.stat().st_size,
        calculate_sha256(file_path),
        audit_time
    ))

manifest_schema = StructType([
    StructField("file_name", StringType(), False),
    StructField("relative_path", StringType(), False),
    StructField("source_path", StringType(), False),
    StructField("file_extension", StringType(), False),
    StructField("file_size_bytes", LongType(), False),
    StructField("sha256", StringType(), False),
    StructField("audit_timestamp_utc", StringType(), False)
])

manifest_df = spark.createDataFrame(
    manifest_rows,
    manifest_schema
)

assert manifest_df.count() == 6

display(manifest_df)


# COMMAND ----------

# Save the manifest


(
    manifest_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.bronze.source_file_manifest"
    )
)

print("Source file manifest saved.")

# This is a current-source manifest, not a complete historical ingestion log. For repeated production runs, we would assign a run ID and append immutable audit records rather than overwrite them.

# COMMAND ----------

from datetime import date, datetime

def to_text(value):
    if value is None:
        return None

    if isinstance(value, (date, datetime)):
        return value.isoformat()

    return str(value)

print("to_text() is ready.")

# COMMAND ----------

# Bronze enhancement — CHW row-level lineage


import json
import openpyxl
import hashlib

chw_lineage_rows = []

for table_name, filename in chw_table_files.items():

    file_path = chw_dir / filename
    sheet_name = chw_sheet_names[table_name]

    wb = openpyxl.load_workbook(
        file_path,
        read_only=True,
        data_only=True
    )

    try:
        ws = wb[sheet_name]

        for row_number, row in enumerate(
            ws.iter_rows(values_only=True),
            start=1
        ):

            # Use the same conversion as the CHW reader
            values = [
                to_text(value)
                for value in row
            ]

            row_json = json.dumps(
                values,
                ensure_ascii=False,
                separators=(",", ":")
            )

            row_hash = hashlib.sha256(
                row_json.encode("utf-8")
            ).hexdigest()

            chw_lineage_rows.append((
                table_name,
                filename,
                str(file_path),
                sheet_name,
                row_number,
                row_hash,
                row_json
            ))

    finally:
        wb.close()


# COMMAND ----------

# The following helper must match the conversion used in your existing read_chw_bronze() function:
# It ensures that the row values are represented consistently.

from datetime import date, datetime

def to_text(value):
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)

# COMMAND ----------

# Save and validate lineage


from pyspark.sql.types import IntegerType

lineage_schema = StructType([
    StructField("bronze_table", StringType(), False),
    StructField("source_file", StringType(), False),
    StructField("source_path", StringType(), False),
    StructField("source_sheet", StringType(), False),
    StructField("source_excel_row", IntegerType(), False),
    StructField("row_sha256", StringType(), False),
    StructField("row_values_json", StringType(), False)
])

chw_lineage_df = spark.createDataFrame(
    chw_lineage_rows,
    lineage_schema
)

for table_name in chw_table_files:

    expected = spark.table(
        f"workspace.bronze.{table_name}"
    ).count()

    actual = chw_lineage_df.filter(
        F.col("bronze_table") == table_name
    ).count()

    if expected != actual:
        raise AssertionError(
            f"{table_name}: Bronze={expected}, "
            f"lineage={actual}"
        )

(
    chw_lineage_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.bronze.chw_row_lineage"
    )
)

display(
    spark.table(
        "workspace.bronze.chw_row_lineage"
    ).groupBy("bronze_table").count()
)
# This preserves original Excel row coordinates in a companion table. It does not change or reorder the existing Bronze records.

# COMMAND ----------

# MAGIC %md
# MAGIC # SILVER
# MAGIC ### Silver design

# COMMAND ----------

# SILVER Assessment

# [Assessment requirement] , [Silver responsibility] , [Validation].
# Source structure conformance , Convert three source families into a common training-record structure , Required fields and source counts.
# Course lookup mapping , Match course names to approved reference values	Matched , unmatched and ambiguous counts.
# Facility lookup mapping ,	Standardise facility and district information , Coverage and unmatched records.
# Profession standardisation , Map different profession descriptions , 	Mapped and unmapped values.
# Participant identity matching , Resolve participant identifiers across sources , Identifier coverage and match confidence.
# Deduplication	Identify repeated training records	Duplicates flagged with reasons
# Reportable rules	Determine records eligible for reporting	Included/excluded counts and reasons
# Reconciliation	Account for every source record through processing	Source-to-output reconciliation.

# COMMAND ----------

# We will use these logical Silver datasets:

# [Proposed table]          [Purpose]
# silver_source_profile	    Source data-quality summary
# silver_training_staging	Conformed training records
# silver_course_mapping	    Course matching results
# silver_facility_mapping	Facility matching results
# silver_profession_mapping	Profession standardisation
# silver_participant_identity	Participant identity resolution
# silver_training_deduplicated	Training records with duplicate decisions
# silver_training_reportable	Records with reportability decisions

# Begin Silver — Create the schema and profile Bronze
# The first Silver step is source profiling, not immediately merging everything.

# Create Silver schema


spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.silver
""")

print("Silver schema ready: workspace.silver")

# COMMAND ----------

# Profile all nine Bronze tables
# This is a basic null/blank profile.


from pyspark.sql import functions as F

profile_rows = []

for table_name in sorted(expected_tables):

    df = spark.table(
        f"workspace.bronze.{table_name}"
    )

    total_rows = df.count()

    for column_name in df.columns:

        value = F.col(f"`{column_name.replace('`', '``')}`")

        missing_count = df.filter(
            value.isNull() |
            (F.trim(value.cast("string")) == "")
        ).count()

        profile_rows.append((
            table_name,
            column_name,
            total_rows,
            missing_count,
            round(
                100 * missing_count / total_rows, 2
            ) if total_rows else 0.0
        ))

profile_df = spark.createDataFrame(
    profile_rows,
    [
        "source_table",
        "column_name",
        "total_rows",
        "missing_count",
        "missing_pct"
    ]
)

display(
    profile_df.orderBy(
        F.desc("missing_pct"),
        "source_table"
    )
)


# COMMAND ----------

# Save the Silver source profile


(
    profile_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.silver.silver_source_profile"
    )
)

print("Silver source profile created.")


# COMMAND ----------

# Inspect the CHW attendance template before transforming it

# The CHW files need special attention. Their 49 columns are not 49 ordinary fields: the worksheets contain grouped module headings, potentially multiple header rows and attendance/completion values.


import json

sample_rows = (
    spark.table("workspace.bronze.chw_row_lineage")
    .filter(F.col("bronze_table") == "bronze_chw_kess")
    .orderBy("source_excel_row")
    .limit(12)
    .collect()
)

for row in sample_rows:
    values = json.loads(row["row_values_json"])

    print(f"\nEXCEL ROW {row['source_excel_row']}")

    for index, value in enumerate(values, start=1):
        if value is not None and str(value).strip():
            print(f"  Column {index:02d}: {value}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver transformations

# COMMAND ----------

# MAGIC %md
# MAGIC The important discovery is that each CHW worksheet contains one row per participant, with 15 possible training modules stored across columns.

# COMMAND ----------

# MAGIC %md
# MAGIC One important distinction: a populated module start date demonstrates a recorded training activity, but does not necessarily prove completion. The worksheet does not contain an explicit completion-status field in the displayed headers.

# COMMAND ----------

# Create the CHW module lookup from the source header


import json
from pyspark.sql import functions as F

lineage = spark.table(
    "workspace.bronze.chw_row_lineage"
)

module_header = (
    lineage
    .filter(
        (F.col("bronze_table") == "bronze_chw_kess") &
        (F.col("source_excel_row") == 1)
    )
    .select("row_values_json")
    .first()
)

if module_header is None:
    raise ValueError("CHW module header row missing")

header_values = json.loads(
    module_header["row_values_json"]
)

module_lookup_rows = []

for module_number in range(1, 16):
    date_column = 20 + (module_number - 1) * 2
    trainer_column = date_column + 1

    course_name = header_values[date_column - 1]

    if not course_name:
        raise ValueError(
            f"Missing module name: {module_number}"
        )

    module_lookup_rows.append((
        module_number,
        course_name,
        date_column,
        trainer_column
    ))

module_lookup_df = spark.createDataFrame(
    module_lookup_rows,
    [
        "module_number",
        "source_course_name",
        "date_column_number",
        "trainer_column_number"
    ]
)

display(module_lookup_df)

# This produces a source-derived mapping for the 15 modules. We can later reconcile their names to the official course lookup.

# COMMAND ----------

# Validate CHW template consistency

# The assessment states that all three CHW files have the same structure.


expected_modules = [
    row[1] for row in module_lookup_rows
]

for table_name in chw_table_files:

    header = (
        lineage
        .filter(
            (F.col("bronze_table") == table_name) &
            (F.col("source_excel_row") == 1)
        )
        .select("row_values_json")
        .first()
    )

    if header is None:
        raise ValueError(
            f"Missing header row in {table_name}"
        )

    values = json.loads(
        header["row_values_json"]
    )

    actual_modules = [
        values[19 + (i * 2)]
        for i in range(15)
    ]

    if actual_modules != expected_modules:
        raise AssertionError(
            f"CHW module structure mismatch: {table_name}"
        )

    print(f"{table_name}: 15 modules verified")

print("All three CHW module structures match.")


# COMMAND ----------

# Transform CHW into training records

# We will create a long-format dataset with one row for every participant-module combination where a module date is recorded.


from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType
)

def clean_text(value):
    if value is None:
        return None

    text_value = str(value).strip()
    return text_value if text_value else None


training_rows = []
participant_rows = []

for source in (
    lineage
    .filter(F.col("source_excel_row") >= 3)
    .collect()
):

    values = json.loads(source["row_values_json"])

    def cell(position):
        return clean_text(values[position - 1])

    source_record_id = (
        f"{source['bronze_table']}:"
        f"{source['source_excel_row']}"
    )

    participant = {
        "source_record_id": source_record_id,
        "source_table": source["bronze_table"],
        "source_file": source["source_file"],
        "source_excel_row": source["source_excel_row"],
        "participant_number": cell(1),
        "last_name": cell(2),
        "first_name": cell(3),
        "email": cell(4),
        "gender": cell(5),
        "race": cell(6),
        "disability": cell(7),
        "employer_group": cell(8),
        "employer_name": cell(9),
        "id_number": cell(10),
        "employee_number": cell(11),
        "profession_raw": cell(12),
        "job_title": cell(13),
        "professional_registration_number": cell(14),
        "district_raw": cell(15),
        "subdistrict_raw": cell(16),
        "facility_raw": cell(17),
        "other_facility": cell(18),
        "supervisor_email": cell(19)
    }

    # Retain the participant record even if no module date exists
    participant_rows.append(participant)

    for module_number, course_name, date_pos, trainer_pos in module_lookup_rows:

        module_date = cell(date_pos)

        if module_date is None:
            continue

        training_rows.append({
            **participant,
            "module_number": module_number,
            "course_name_raw": course_name,
            "training_date_raw": module_date,
            "trainer_name": cell(trainer_pos),
            "source_completion_assumption": "DATE_PRESENT"
        })

print("CHW participant source rows:", len(participant_rows))
print("CHW module training records:", len(training_rows))


# COMMAND ----------

# Save CHW staging tables

# Step 1 — Replace your current CHW staging cell


# Cell 24 — Save CHW staging tables with explicit schemas

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)

# 1. Validate the extracted Python records

if not participant_rows:
    raise ValueError("No CHW participant rows found")

if not training_rows:
    raise ValueError("No CHW module records found")


# 2. Define the participant schema

participant_fields = [
    StructField("source_record_id", StringType(), False),
    StructField("source_table", StringType(), False),
    StructField("source_file", StringType(), False),
    StructField("source_excel_row", IntegerType(), False),

    StructField("participant_number", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("race", StringType(), True),
    StructField("disability", StringType(), True),

    StructField("employer_group", StringType(), True),
    StructField("employer_name", StringType(), True),
    StructField("id_number", StringType(), True),
    StructField("employee_number", StringType(), True),

    StructField("profession_raw", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField(
        "professional_registration_number",
        StringType(),
        True
    ),

    StructField("district_raw", StringType(), True),
    StructField("subdistrict_raw", StringType(), True),
    StructField("facility_raw", StringType(), True),
    StructField("other_facility", StringType(), True),
    StructField("supervisor_email", StringType(), True)
]

participant_schema = StructType(participant_fields)


# 3. Define the training schema

training_schema = StructType(
    participant_fields + [
        StructField("module_number", IntegerType(), False),
        StructField("course_name_raw", StringType(), True),
        StructField("training_date_raw", StringType(), True),
        StructField("trainer_name", StringType(), True),
        StructField(
            "source_completion_assumption",
            StringType(),
            True
        )
    ]
)


# 4. Create Spark DataFrames using explicit schemas

chw_participants_df = spark.createDataFrame(
    participant_rows,
    schema=participant_schema
)

chw_training_df = spark.createDataFrame(
    training_rows,
    schema=training_schema
)


# 5. Save to Silver Delta tables

(
    chw_participants_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.silver.silver_chw_participants_staging"
    )
)

(
    chw_training_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.silver.silver_chw_training_staging"
    )
)


# 6. Verify persisted tables

participants_count = spark.table(
    "workspace.silver.silver_chw_participants_staging"
).count()

training_count = spark.table(
    "workspace.silver.silver_chw_training_staging"
).count()

print("CHW staging tables created successfully.")
print(f"Participants: {participants_count:,}")
print(f"Training records: {training_count:,}")


# COMMAND ----------

# Step 3 — Validate the training records


from pyspark.sql import functions as F

chw_training = spark.table(
    "workspace.silver.silver_chw_training_staging"
)

print("Training records by source:")

display(
    chw_training
    .groupBy("source_table")
    .agg(
        F.count("*").alias("training_records"),
        F.countDistinct("source_record_id").alias(
            "participants_with_training"
        ),
        F.countDistinct("module_number").alias(
            "modules_recorded"
        )
    )
    .orderBy("source_table")
)

print("Training records by module:")

display(
    chw_training
    .groupBy("module_number", "course_name_raw")
    .count()
    .orderBy("module_number")
)


# COMMAND ----------

# MAGIC %md
# MAGIC By using explicit schemas, we achieve three things:
# MAGIC - Predictable structure: Spark always creates the same columns with the same data types.
# MAGIC - Reliable null handling: Entirely empty columns are preserved instead of causing inference errors.
# MAGIC - Repeatability: Running the same transformation on unchanged Bronze data produces the same schema and row counts.

# COMMAND ----------

# MAGIC %md
# MAGIC Your CHW Silver transformation has produced 1,112 training records across the three workbooks. The results are consistent at the source-summary level, and we can now validate them before moving to the capturing-tool data.

# COMMAND ----------

# CHW Silver results

# Training records
# 1,112

# Source participants with training
# 359

# Modules with records
# 14/15

# COMMAND ----------

# MAGIC %md
# MAGIC CHW source     Training records     Participant rows with training
# MAGIC KESS	       476	                233
# MAGIC West Coast	   22	                22
# MAGIC Witzenberg	   614	                104
# MAGIC Total	       1,112	            359

# COMMAND ----------

# Validate the module totals

# Your displayed module counts add up to 1,112, matching the source totals. However, the module breakdown shows only modules 1–14.

# Run this check to confirm that module 15 genuinely has zero records and that no invalid module numbers exist.


from pyspark.sql import functions as F

chw_training = spark.table(
    "workspace.silver.silver_chw_training_staging"
)

module_summary = (
    chw_training
    .groupBy("module_number")
    .count()
)

all_modules = spark.range(1, 16).select(
    F.col("id").cast("int").alias("module_number")
)

module_reconciliation = (
    all_modules
    .join(module_summary, "module_number", "left")
    .fillna({"count": 0})
    .orderBy("module_number")
)

display(module_reconciliation)

total_by_modules = module_reconciliation.agg(
    F.sum("count")
).first()[0]

total_training = chw_training.count()

assert total_by_modules == total_training
assert total_training == 1112

print("CHW module reconciliation: PASS")
print("Total training records:", total_training)


# COMMAND ----------

# Check date validity and duplicate module records


chw_checked = (
    chw_training
    .withColumn(
        "training_date",
        F.coalesce(
            F.to_date(
                F.col("training_date_raw"),
                "yyyy-MM-dd'T'HH:mm:ss"
            ),
            F.to_date(
                F.col("training_date_raw"),
                "yyyy-MM-dd"
            )
        )
    )
)

date_quality = chw_checked.agg(
    F.count("*").alias("total_records"),
    F.sum(
        F.when(
            F.col("training_date").isNull(), 1
        ).otherwise(0)
    ).alias("invalid_dates")
)

display(date_quality)

duplicates = (
    chw_checked
    .groupBy(
        "source_record_id",
        "module_number"
    )
    .count()
    .filter(F.col("count") > 1)
)

print("Duplicate participant-module rows:", duplicates.count())

display(duplicates)


# COMMAND ----------

# Next: Capturing-tool source conformance


capturer_tables = [
    "bronze_capturer1",
    "bronze_capturer2",
    "bronze_capturer3"
]

for table_name in capturer_tables:

    df = spark.table(
        f"workspace.bronze.{table_name}"
    )

    print(f"\n{'=' * 65}")
    print(f"TABLE: {table_name}")
    print(f"ROWS: {df.count():,}")
    print(f"COLUMNS: {len(df.columns)}")

    print("\nCOLUMN NAMES:")
    for index, column in enumerate(df.columns, start=1):
        print(f"{index:02d}. {column}")

    print("\nFIRST 3 ROWS:")
    display(df.limit(3))


# COMMAND ----------

# Validate source structures

# This fails loudly if a capturing-tool table is missing a field or has an unexpected column order.


from pyspark.sql import functions as F

capturer_tables = [
    "bronze_capturer1",
    "bronze_capturer2",
    "bronze_capturer3"
]

expected_columns = [
    "Captured By",
    "Date first captured (YYYY/MM/DD)",
    "Course Name",
    "Method of Delivery (Training modality)",
    "Start date (YYYY/MM/DD)",
    "End date (YYYY/MM/DD)",
    "Booking/enrollment status",
    "Training Venue",
    "Presenter/Facilitator name and surname",
    "Contracted Provider",
    "Funding Partner- If not PDC (eg. NDOH, Love Life, Soul City)",
    "Coordinator",
    "Participant Surname",
    "Participant First name",
    "Email Address",
    "Gender",
    "Race",
    "Disability",
    "Employer Group",
    "Employer details (eg: WCCN, Anova, Pollsmoor)",
    "ID number",
    "Persal/Employee number",
    "Professional Category",
    "Profession",
    "Job Title/Rank (if different)",
    "Professional Registration Number",
    "District",
    "Sub-structure/ sub-district",
    "Facility",
    "If Facility not on the list, please provide details (Facility name/work location)",
    "Supervisor Email",
    "HRD Representative Email",
    "Attendance status",
    "Certificate Number",
    "Date Electronic Certificate Issued to HRD (YYYY/MM/DD)",
    "Competency status (leave for now)",
    "Date finalised (YYYY/MM/DD)"
]

for table_name in capturer_tables:

    df = spark.table(
        f"workspace.bronze.{table_name}"
    )

    if df.columns != expected_columns:
        raise ValueError(
            f"Unexpected schema in {table_name}"
        )

    print(
        f"PASS: {table_name} "
        f"({len(df.columns)} columns)"
    )


# COMMAND ----------

# Define the conformed capturing-tool schema


column_mapping = {
    "Captured By": "captured_by",
    "Date first captured (YYYY/MM/DD)": "date_first_captured_raw",
    "Course Name": "course_name_raw",
    "Method of Delivery (Training modality)": "delivery_method_raw",
    "Start date (YYYY/MM/DD)": "training_start_date_raw",
    "End date (YYYY/MM/DD)": "training_end_date_raw",
    "Booking/enrollment status": "enrolment_status_raw",
    "Training Venue": "training_venue_raw",
    "Presenter/Facilitator name and surname": "trainer_name",
    "Contracted Provider": "provider_raw",
    "Funding Partner- If not PDC (eg. NDOH, Love Life, Soul City)": "funding_partner_raw",
    "Coordinator": "coordinator_raw",
    "Participant Surname": "last_name",
    "Participant First name": "first_name",
    "Email Address": "email",
    "Gender": "gender",
    "Race": "race",
    "Disability": "disability",
    "Employer Group": "employer_group",
    "Employer details (eg: WCCN, Anova, Pollsmoor)": "employer_name",
    "ID number": "id_number",
    "Persal/Employee number": "employee_number",
    "Professional Category": "professional_category_raw",
    "Profession": "profession_raw",
    "Job Title/Rank (if different)": "job_title",
    "Professional Registration Number": "professional_registration_number",
    "District": "district_raw",
    "Sub-structure/ sub-district": "subdistrict_raw",
    "Facility": "facility_raw",
    "If Facility not on the list, please provide details (Facility name/work location)": "other_facility",
    "Supervisor Email": "supervisor_email",
    "HRD Representative Email": "hrd_email",
    "Attendance status": "attendance_status_raw",
    "Certificate Number": "certificate_number",
    "Date Electronic Certificate Issued to HRD (YYYY/MM/DD)": "certificate_date_raw",
    "Competency status (leave for now)": "competency_status_raw",
    "Date finalised (YYYY/MM/DD)": "date_finalised_raw"
}

assert set(column_mapping) == set(expected_columns)
assert len(set(column_mapping.values())) == 37

print("Capturing-tool column mapping validated.")


# COMMAND ----------

# Standardise and combine the three capturing sheets


from functools import reduce

def normalise_text(column_name):
    value = F.trim(F.col(f"`{column_name}`"))

    return F.when(
        value.isNull() | (value == ""),
        F.lit(None).cast("string")
    ).otherwise(value)


capturer_frames = []

for table_name in capturer_tables:

    bronze = spark.table(
        f"workspace.bronze.{table_name}"
    )

    standardised = bronze.select(
        *[
            normalise_text(source_name).alias(target_name)
            for source_name, target_name
            in column_mapping.items()
        ]
    )

    # Fingerprint the original 37 source values
    original_values = F.to_json(
        F.array(
            *[
                F.col(f"`{name}`")
                for name in expected_columns
            ]
        ),
        options={"ignoreNullFields": "false"}
    )

    source_hashes = bronze.select(
        F.sha2(original_values, 256).alias(
            "source_row_sha256"
        )
    )

    # Compute hash within the same projection to
    # avoid joining DataFrames by row position
    standardised = bronze.select(
        *[
            normalise_text(source_name).alias(target_name)
            for source_name, target_name
            in column_mapping.items()
        ],
        F.lit(table_name).alias("source_table"),
        F.sha2(original_values, 256).alias(
            "source_row_sha256"
        )
    )

    capturer_frames.append(standardised)


capturer_staging_df = reduce(
    lambda left, right: left.unionByName(right),
    capturer_frames
)

print(
    "Combined capturing-tool rows:",
    capturer_staging_df.count()
)

assert capturer_staging_df.count() == 11607


# COMMAND ----------

# Parse dates and add provisional status flags


def parse_source_date(column_name):
    value = F.col(column_name)

    return F.coalesce(
        F.try_to_timestamp(
            value,
            F.lit("yyyy-MM-dd'T'HH:mm:ss")
        ).cast("date"),
        F.try_to_timestamp(
            value,
            F.lit("yyyy-MM-dd")
        ).cast("date"),
        F.try_to_timestamp(
            value,
            F.lit("yyyy/MM/dd")
        ).cast("date")
    )


date_columns = {
    "date_first_captured_raw": "date_first_captured",
    "training_start_date_raw": "training_start_date",
    "training_end_date_raw": "training_end_date",
    "certificate_date_raw": "certificate_date",
    "date_finalised_raw": "date_finalised"
}

for raw_name, parsed_name in date_columns.items():

    capturer_staging_df = (
        capturer_staging_df
        .withColumn(
            parsed_name,
            parse_source_date(raw_name)
        )
    )


capturer_staging_df = (
    capturer_staging_df
    .withColumn(
        "attendance_status_normalised",
        F.upper(F.trim(F.col("attendance_status_raw")))
    )
    .withColumn(
        "enrolment_status_normalised",
        F.upper(F.trim(F.col("enrolment_status_raw")))
    )
)

print("Capturing-tool dates and statuses standardised.")


# COMMAND ----------

# Save the Silver capturing-tool table


target_table = (
    "workspace.silver.silver_capturer_training_staging"
)

(
    capturer_staging_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

actual_count = spark.table(target_table).count()

assert actual_count == 11607

print("Capturing-tool Silver staging saved.")
print("Total records:", actual_count)

display(
    spark.table(target_table)
    .groupBy("source_table")
    .count()
    .orderBy("source_table")
)


# COMMAND ----------

# Inspect attendance and enrolment statuses

# This is particularly important because the assessment asks us to define a defensible reportable rule.


capturer = spark.table(
    "workspace.silver.silver_capturer_training_staging"
)

print("ATTENDANCE STATUS")
display(
    capturer
    .groupBy("attendance_status_normalised")
    .count()
    .orderBy(F.desc("count"))
)

print("ENROLMENT STATUS")
display(
    capturer
    .groupBy("enrolment_status_normalised")
    .count()
    .orderBy(F.desc("count"))
)

print("DELIVERY METHOD")
display(
    capturer
    .groupBy("delivery_method_raw")
    .count()
    .orderBy(F.desc("count"))
)

print("DATE PARSING QUALITY")
display(
    capturer.agg(
        F.count("*").alias("total_rows"),
        F.sum(
            F.when(
                F.col("training_start_date_raw").isNotNull() &
                F.col("training_start_date").isNull(),
                1
            ).otherwise(0)
        ).alias("invalid_start_dates"),
        F.sum(
            F.when(
                F.col("training_end_date_raw").isNotNull() &
                F.col("training_end_date").isNull(),
                1
            ).otherwise(0)
        ).alias("invalid_end_dates")
    )
)

# Capturing-tool Silver staging has passed its initial validation.
# all populated training start and end dates successfully parsed.
# Capturing-tool data-quality findings

# COMMAND ----------

# Classify capturing-tool attendance


from pyspark.sql import functions as F

capturer = spark.table(
    "workspace.silver.silver_capturer_training_staging"
)

capturer_classified = (
    capturer
    .withColumn(
        "training_outcome",
        F.when(
            F.col("attendance_status_normalised") == "ATTENDED",
            F.lit("COMPLETION_CANDIDATE")
        )
        .when(
            F.col("attendance_status_normalised").isin(
                "NON-ATTENDANCE",
                "PARTIAL ATTENDANCE"
            ),
            F.lit("NOT_COMPLETED")
        )
        .when(
            F.col("attendance_status_normalised") == "CANCELLED",
            F.lit("CANCELLED")
        )
        .when(
            F.col("attendance_status_normalised") == "REPLACEMENT",
            F.lit("REPLACEMENT")
        )
        .otherwise(F.lit("UNKNOWN"))
    )
)

display(
    capturer_classified
    .groupBy("training_outcome")
    .count()
    .orderBy(F.desc("count"))
)


# COMMAND ----------

# Save the classified dataset


target = (
    "workspace.silver."
    "silver_capturer_training_classified"
)

(
    capturer_classified.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target)
)

saved = spark.table(target)

assert saved.count() == 11607

print("Capturing-tool outcome classification saved.")

display(
    saved.groupBy("training_outcome").count()
)


# COMMAND ----------

# Inspect online-school schema and sample


online = spark.table(
    "workspace.bronze.bronze_online_school"
)

print("ONLINE-SCHOOL SOURCE")
print("Rows:", online.count())
print("Columns:", len(online.columns))

print("\nCOLUMN NAMES")
for index, column in enumerate(
    online.columns,
    start=1
):
    print(f"{index:02d}. {column}")

print("\nFIRST THREE ROWS")
display(online.limit(3))


# COMMAND ----------

# Inspect online-school field completeness

# This will help us identify which columns can support participant matching and completion reporting.


online_profile = []

for column_name in online.columns:

    value = F.col(
        f"`{column_name.replace('`', '``')}`"
    )

    stats = online.agg(
        F.count("*").alias("total_rows"),
        F.sum(
            F.when(
                value.isNull() |
                (F.trim(value.cast("string")) == ""),
                1
            ).otherwise(0)
        ).alias("missing_rows")
    ).first()

    online_profile.append((
        column_name,
        stats["total_rows"],
        stats["missing_rows"],
        round(
            100 * stats["missing_rows"] /
            stats["total_rows"],
            2
        )
    ))

online_profile_df = spark.createDataFrame(
    online_profile,
    [
        "column_name",
        "total_rows",
        "missing_rows",
        "missing_pct"
    ]
)

display(
    online_profile_df.orderBy(
        F.desc("missing_pct")
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Phase 3 — Online-school transformation and conformance.

# COMMAND ----------

# Validate online-school source schema


from pyspark.sql import functions as F

online = spark.table(
    "workspace.bronze.bronze_online_school"
)

expected_online_columns = [
    "Course Name",
    "Trainee Number",
    "Last Name",
    "First Name",
    "Email",
    "Gender",
    "Race (mandatory reporting requirement for DOH)",
    "Employer Category",
    "Employers / Institution",
    "Employer / Institution Name",
    "ID Number",
    "PERSAL",
    "Profession",
    "Job Title/Rank",
    "Professional Registration Number",
    "Disability",
    "Authority",
    "Type",
    "District",
    "sub-District",
    "Location Name",
    "Other Location",
    "Supervisor Email",
    "People Development/ RTC Representative (HRD) Email",
    "Start Date",
    "End Date",
    "Earliest Approval Date"
]

if len(online.columns) != 27:
    raise ValueError(
        f"Expected 27 columns, found {len(online.columns)}"
    )

if set(online.columns) != set(expected_online_columns):
    raise ValueError(
        "Unexpected online-school source columns"
    )

print("Online-school source schema validated.")


# COMMAND ----------

# Standardise online-school column names


online_column_mapping = {
    "Course Name": "course_name_raw",
    "Trainee Number": "trainee_number",
    "Last Name": "last_name",
    "First Name": "first_name",
    "Email": "email",
    "Gender": "gender",
    "Race (mandatory reporting requirement for DOH)": "race",
    "Employer Category": "employer_group",
    "Employers / Institution": "employer_category_raw",
    "Employer / Institution Name": "employer_name",
    "ID Number": "id_number",
    "PERSAL": "employee_number",
    "Profession": "profession_raw",
    "Job Title/Rank": "job_title",
    "Professional Registration Number": "professional_registration_number",
    "Disability": "disability",
    "Authority": "authority_raw",
    "Type": "delivery_method_raw",
    "District": "district_raw",
    "sub-District": "subdistrict_raw",
    "Location Name": "facility_raw",
    "Other Location": "other_facility",
    "Supervisor Email": "supervisor_email",
    "People Development/ RTC Representative (HRD) Email": "hrd_email",
    "Start Date": "training_start_date_raw",
    "End Date": "training_end_date_raw",
    "Earliest Approval Date": "approval_date_raw"
}

assert set(online_column_mapping) == set(online.columns)

online_staging = online.select(
    *[
        F.when(
            F.trim(F.col(f"`{source}`")) == "",
            F.lit(None).cast("string")
        )
        .otherwise(F.trim(F.col(f"`{source}`")))
        .alias(target)
        for source, target in online_column_mapping.items()
    ]
)

print("Online-school columns standardised.")


# COMMAND ----------

# Parse dates and add source metadata


def parse_online_date(column_name):
    value = F.col(column_name)

    return F.coalesce(
        F.try_to_timestamp(
            value,
            F.lit("yyyy-MM-dd'T'HH:mm:ss")
        ).cast("date"),
        F.try_to_timestamp(
            value,
            F.lit("yyyy-MM-dd")
        ).cast("date"),
        F.try_to_timestamp(
            value,
            F.lit("yyyy/MM/dd")
        ).cast("date"),
        F.try_to_timestamp(
            value,
            F.lit("dd/MM/yyyy")
        ).cast("date")
    )


online_staging = (
    online_staging
    .withColumn(
        "training_start_date",
        parse_online_date("training_start_date_raw")
    )
    .withColumn(
        "training_end_date",
        parse_online_date("training_end_date_raw")
    )
    .withColumn(
        "approval_date",
        parse_online_date("approval_date_raw")
    )
    .withColumn(
        "has_approval_evidence",
        F.col("approval_date").isNotNull()
    )
    .withColumn(
        "source_table",
        F.lit("bronze_online_school")
    )
)

print("Online-school dates parsed.")



# COMMAND ----------

# Save and reconcile online-school staging


target = (
    "workspace.silver.silver_online_training_staging"
)

(
    online_staging.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target)
)

source_count = online.count()
silver_count = spark.table(target).count()

assert source_count == silver_count

print("Online-school Silver staging saved.")
print("Bronze records:", source_count)
print("Silver records:", silver_count)
print("Reconciliation: PASS")


# COMMAND ----------

# Examine approval evidence and date quality


online_saved = spark.table(
    "workspace.silver.silver_online_training_staging"
)

display(
    online_saved.agg(
        F.count("*").alias("total_records"),

        F.sum(
            F.col("has_approval_evidence").cast("int")
        ).alias("approval_evidence_records"),

        F.sum(
            F.when(
                F.col("training_start_date").isNotNull(),
                1
            ).otherwise(0)
        ).alias("valid_start_dates"),

        F.sum(
            F.when(
                F.col("training_end_date").isNotNull(),
                1
            ).otherwise(0)
        ).alias("valid_end_dates"),

        F.sum(
            F.when(
                F.col("approval_date_raw").isNotNull() &
                F.col("approval_date").isNull(),
                1
            ).otherwise(0)
        ).alias("invalid_approval_dates")
    )
)

print("ONLINE DELIVERY TYPES")

display(
    online_saved
    .groupBy("delivery_method_raw")
    .count()
    .orderBy(F.desc("count"))
)

print("ONLINE APPROVAL EVIDENCE")

display(
    online_saved
    .groupBy("has_approval_evidence")
    .count()
)
# What needs correcting?
# Type, Start Date, End Date, Earliest Approval Date, Location Name.

# COMMAND ----------

# Inspect the actual online-school date values


from pyspark.sql import functions as F

online_bronze = spark.table(
    "workspace.bronze.bronze_online_school"
)

date_fields = [
    "Start Date",
    "End Date",
    "Earliest Approval Date"
]

for field in date_fields:

    print(f"\n{'=' * 60}")
    print(f"FIELD: {field}")

    display(
        online_bronze
        .groupBy(F.col(f"`{field}`").alias("raw_value"))
        .count()
        .orderBy(F.desc("count"))
        .limit(15)
    )

    print("NON-EMPTY EXAMPLES")

    display(
        online_bronze
        .filter(
            F.col(f"`{field}`").isNotNull() &
            (F.trim(F.col(f"`{field}`")) != "")
        )
        .select(F.col(f"`{field}`"))
        .limit(10)
    )


# COMMAND ----------

# Inspect online-school field meanings


fields_to_inspect = [
    "Authority",
    "Type",
    "Location Name",
    "Other Location"
]

for field in fields_to_inspect:

    print(f"\nFIELD: {field}")

    display(
        online_bronze
        .groupBy(F.col(f"`{field}`"))
        .count()
        .orderBy(F.desc("count"))
        .limit(15)
    )


# COMMAND ----------

# Check whether the online-school dates contain meaningful values


display(
    online_bronze.agg(
        F.count("*").alias("total_records"),

        F.count(
            F.when(
                F.col("Start Date").isNotNull() &
                (F.trim(F.col("Start Date")) != ""),
                1
            )
        ).alias("start_date_populated"),

        F.count(
            F.when(
                F.col("End Date").isNotNull() &
                (F.trim(F.col("End Date")) != ""),
                1
            )
        ).alias("end_date_populated"),

        F.count(
            F.when(
                F.col("Earliest Approval Date").isNotNull() &
                (F.trim(F.col("Earliest Approval Date")) != ""),
                1
            )
        ).alias("approval_date_populated")
    )
)
# The source data is present. We should not proceed with the existing online-school Silver table until its date parsing is corrected.

# COMMAND ----------

# Identify the actual date formats


from pyspark.sql import functions as F

online_bronze = spark.table(
    "workspace.bronze.bronze_online_school"
)

for field in [
    "Start Date",
    "End Date",
    "Earliest Approval Date"
]:
    print(f"\n{'=' * 60}")
    print(f"FIELD: {field}")

    values = (
        online_bronze
        .select(
            F.trim(F.col(f"`{field}`")).alias("raw_date")
        )
        .filter(
            F.col("raw_date").isNotNull() &
            (F.col("raw_date") != "")
        )
    )

    display(
        values
        .groupBy("raw_date")
        .count()
        .orderBy(F.desc("count"))
        .limit(10)
    )

    print("DATE STRING LENGTHS")

    display(
        values
        .groupBy(
            F.length("raw_date").alias("string_length")
        )
        .count()
        .orderBy(F.desc("count"))
    )


# COMMAND ----------

# Correct online-school dates and field meanings


from pyspark.sql import functions as F

online_bronze = spark.table(
    "workspace.bronze.bronze_online_school"
)

def clean_col(source):
    value = F.trim(F.col(f"`{source}`").cast("string"))
    return F.when(
        value.isNull() | (value == ""),
        F.lit(None).cast("string")
    ).otherwise(value)

online_mapping = {
    "Course Name": "course_name_raw",
    "Trainee Number": "trainee_number",
    "Last Name": "last_name",
    "First Name": "first_name",
    "Email": "email",
    "Gender": "gender",
    "Race (mandatory reporting requirement for DOH)": "race",
    "Employer Category": "employer_group",
    "Employers / Institution": "employer_category_raw",
    "Employer / Institution Name": "employer_name",
    "ID Number": "id_number",
    "PERSAL": "employee_number",
    "Profession": "profession_raw",
    "Job Title/Rank": "job_title",
    "Professional Registration Number": "professional_registration_number",
    "Disability": "disability",
    "Authority": "authority_raw",
    "Type": "facility_type_raw",
    "District": "district_raw",
    "sub-District": "subdistrict_raw",
    "Location Name": "facility_raw",
    "Other Location": "other_facility",
    "Supervisor Email": "supervisor_email",
    "People Development/ RTC Representative (HRD) Email": "hrd_email",
    "Start Date": "training_start_date_raw",
    "End Date": "training_end_date_raw",
    "Earliest Approval Date": "approval_date_raw"
}

assert set(online_mapping) == set(online_bronze.columns)

online_fixed = online_bronze.select(
    *[
        clean_col(source).alias(target)
        for source, target in online_mapping.items()
    ]
)

def parse_online_date(name):
    v = F.col(name)
    return F.coalesce(
        F.try_to_timestamp(
            v, F.lit("dd MMMM yyyy")
        ).cast("date"),
        F.try_to_timestamp(
            v, F.lit("d MMMM yyyy")
        ).cast("date"),
        F.try_to_timestamp(
            v, F.lit("dd MMM yyyy")
        ).cast("date"),
        F.try_to_timestamp(
            v, F.lit("yyyy-MM-dd'T'HH:mm:ss")
        ).cast("date"),
        F.try_to_timestamp(
            v, F.lit("yyyy-MM-dd")
        ).cast("date"),
        F.try_to_timestamp(
            v, F.lit("dd/MM/yyyy")
        ).cast("date"),
        F.try_to_timestamp(
            v, F.lit("yyyy/MM/dd")
        ).cast("date")
    )

for raw, parsed in [
    ("training_start_date_raw", "training_start_date"),
    ("training_end_date_raw", "training_end_date"),
    ("approval_date_raw", "approval_date")
]:
    online_fixed = online_fixed.withColumn(
        parsed, parse_online_date(raw)
    )

online_fixed = (
    online_fixed
    .withColumn("source_table", F.lit("bronze_online_school"))
    .withColumn("delivery_method_raw", F.lit("ONLINE"))
    .withColumn(
        "has_approval_evidence",
        F.col("approval_date").isNotNull()
    )
)

online_fixed.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "workspace.silver.silver_online_training_staging"
    )

assert online_fixed.count() == 25824

display(
    online_fixed.agg(
        F.count("*").alias("source_rows"),
        F.count("training_start_date").alias("parsed_start"),
        F.count("training_end_date").alias("parsed_end"),
        F.count("approval_date").alias("parsed_approval"),
        F.sum(
            (
                F.col("training_start_date_raw").isNotNull()
                & F.col("training_start_date").isNull()
            ).cast("int")
        ).alias("invalid_start"),
        F.sum(
            (
                F.col("training_end_date_raw").isNotNull()
                & F.col("training_end_date").isNull()
            ).cast("int")
        ).alias("invalid_end"),
        F.sum(
            (
                F.col("approval_date_raw").isNotNull()
                & F.col("approval_date").isNull()
            ).cast("int")
        ).alias("invalid_approval")
    )
)


# COMMAND ----------

# Standardise the three source families into one structure


from pyspark.sql import functions as F

chw = spark.table(
    "workspace.silver.silver_chw_training_staging"
)
capturer = spark.table(
    "workspace.silver.silver_capturer_training_classified"
)
online = spark.table(
    "workspace.silver.silver_online_training_staging"
)

common_fields = [
    "source_table", "source_record_id",
    "first_name", "last_name", "email",
    "id_number", "employee_number",
    "professional_registration_number",
    "course_name_raw", "profession_raw",
    "facility_raw", "other_facility",
    "district_raw", "subdistrict_raw",
    "delivery_method_raw",
    "training_start_date", "training_end_date",
    "training_outcome", "completion_evidence"
]

def align(df):
    return df.select(
        *[
            F.col(c).cast("string").alias(c)
            if c in df.columns
            else F.lit(None).cast("string").alias(c)
            for c in common_fields
        ]
    )

chw_events = (
    chw
    .withColumn(
        "source_record_id",
        F.concat_ws(
            ":",
            F.col("source_record_id"),
            F.col("module_number").cast("string")
        )
    )
    .withColumn(
        "training_start_date",
        F.coalesce(
            F.try_to_timestamp(
                F.col("training_date_raw"),
                F.lit("yyyy-MM-dd'T'HH:mm:ss")
            ),
            F.try_to_timestamp(
                F.col("training_date_raw"),
                F.lit("yyyy-MM-dd")
            )
        ).cast("date")
    )
    .withColumn("training_end_date", F.col("training_start_date"))
    .withColumn("delivery_method_raw", F.lit("CHW"))
    .withColumn(
        "training_outcome",
        F.lit("COMPLETION_CANDIDATE")
    )
    .withColumn(
        "completion_evidence",
        F.lit("CHW_MODULE_DATE_PRESENT")
    )
)

capturer_events = (
    capturer
    .withColumn("source_record_id", F.col("source_row_sha256"))
    .withColumn(
        "completion_evidence",
        F.when(
            F.col("training_outcome") == "COMPLETION_CANDIDATE",
            F.lit("ATTENDANCE_STATUS_ATTENDED")
        ).otherwise(F.lit(None).cast("string"))
    )
)

online_events = (
    online
    .withColumn(
        "source_record_id",
        F.sha2(
            F.to_json(
                F.struct(
                    "trainee_number",
                    "course_name_raw",
                    "training_start_date_raw",
                    "training_end_date_raw",
                    "approval_date_raw"
                )
            ),
            256
        )
    )
    .withColumn(
        "training_outcome",
        F.when(
            F.col("approval_date").isNotNull(),
            F.lit("COMPLETION_CANDIDATE")
        ).otherwise(F.lit("UNKNOWN"))
    )
    .withColumn(
        "completion_evidence",
        F.when(
            F.col("approval_date").isNotNull(),
            F.lit("ONLINE_APPROVAL_DATE")
        ).otherwise(F.lit(None).cast("string"))
    )
)

silver_events = (
    align(chw_events)
    .unionByName(align(capturer_events))
    .unionByName(align(online_events))
)

silver_events.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.silver.silver_training_events")

expected = 1112 + 11607 + 25824
actual = spark.table(
    "workspace.silver.silver_training_events"
).count()

assert actual == expected, (
    f"Silver reconciliation failed: {actual} != {expected}"
)

print("Unified Silver training records:", actual)
print("Expected:", expected)
print("RECONCILIATION PASSED")


# COMMAND ----------

# Standardise profession and create mapping keys


from pyspark.sql import functions as F

events = spark.table(
    "workspace.silver.silver_training_events"
)

def mapping_key(column):
    return F.upper(
        F.regexp_replace(
            F.trim(F.col(column)),
            r"\s+",
            " "
        )
    )

events = (
    events
    .withColumn(
        "course_lookup_key",
        mapping_key("course_name_raw")
    )
    .withColumn(
        "facility_lookup_key",
        mapping_key("facility_raw")
    )
    .withColumn(
        "profession_standardised",
        F.when(
            F.col("profession_raw").isNull(),
            F.lit("UNMAPPED")
        ).otherwise(
            F.initcap(
                F.lower(
                    F.regexp_replace(
                        F.trim(F.col("profession_raw")),
                        r"\s+",
                        " "
                    )
                )
            )
        )
    )
    .withColumn(
        "facility_source_value",
        F.coalesce(
            F.col("facility_raw"),
            F.col("other_facility")
        )
    )
    .withColumn(
        "facility_lookup_key",
        mapping_key("facility_source_value")
    )
)

events.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "workspace.silver.silver_training_events_standardised"
    )

print("Silver standardisation saved.")

display(
    events.groupBy("source_table")
    .count()
    .orderBy("source_table")
)


# COMMAND ----------

# Inspect official lookup structures for the final mapping


for name in [
    "bronze_course_lookup",
    "bronze_facility_lookup"
]:
    df = spark.table(f"workspace.bronze.{name}")

    print(f"\n{name}: {df.count()} rows")
    print("COLUMNS:")
    for c in df.columns:
        print(" -", c)

    print("SAMPLE:")
    display(df.limit(3))


# COMMAND ----------

# Prepare and validate the lookup tables


from pyspark.sql import functions as F
from pyspark.sql.window import Window

courses = spark.table("workspace.bronze.bronze_course_lookup")
facilities = spark.table("workspace.bronze.bronze_facility_lookup")

def norm(c):
    return F.upper(
        F.regexp_replace(
            F.trim(F.col(c).cast("string")),
            r"\s+", " "
        )
    )

course_lookup = (
    courses
    .select(
        norm("CourseName").alias("course_lookup_key"),
        F.col("CourseName").alias("course_standardised"),
        F.col("CourseCode").alias("course_code"),
        F.col("CourseGroup").alias("course_group"),
        F.col("CourseStatus").alias("course_status"),
        F.col("CourseStartDate").alias("course_valid_from_raw"),
        F.col("CourseEndDate").alias("course_valid_to_raw")
    )
    .filter(
        F.col("course_lookup_key").isNotNull() &
        (F.col("course_lookup_key") != "")
    )
    .dropDuplicates()
)

facility_lookup = (
    facilities
    .select(
        norm("FacilityName").alias("facility_lookup_key"),
        F.col("FacilityName").alias("facility_standardised"),
        F.col("FacilityCode").alias("facility_code"),
        F.col("FacilityReportingName").alias("facility_reporting_name"),
        F.col("HealthDistrict").alias("lookup_district"),
        F.col("HealthSubdistrict").alias("lookup_subdistrict"),
        F.col("Status").alias("facility_status")
    )
    .filter(
        F.col("facility_lookup_key").isNotNull() &
        (F.col("facility_lookup_key") != "")
    )
    .dropDuplicates()
)

# A name must resolve to one distinct lookup record.
# Ambiguous names are excluded from automatic mapping.
course_counts = (
    course_lookup
    .groupBy("course_lookup_key")
    .agg(F.count("*").alias("lookup_matches"))
)

facility_counts = (
    facility_lookup
    .groupBy("facility_lookup_key")
    .agg(F.count("*").alias("lookup_matches"))
)

course_unique = (
    course_lookup
    .join(
        course_counts.filter(F.col("lookup_matches") == 1)
        .select("course_lookup_key"),
        "course_lookup_key"
    )
)

facility_unique = (
    facility_lookup
    .join(
        facility_counts.filter(F.col("lookup_matches") == 1)
        .select("facility_lookup_key"),
        "facility_lookup_key"
    )
)

print("Unique course mapping keys:", course_unique.count())
print("Unique facility mapping keys:", facility_unique.count())

print(
    "Ambiguous course keys:",
    course_counts.filter("lookup_matches > 1").count()
)

print(
    "Ambiguous facility keys:",
    facility_counts.filter("lookup_matches > 1").count()
)


# COMMAND ----------

# Apply official course and facility mappings


events = spark.table(
    "workspace.silver.silver_training_events_standardised"
)

expected_count = events.count()

mapped = (
    events
    .join(
        course_unique,
        on="course_lookup_key",
        how="left"
    )
    .join(
        facility_unique,
        on="facility_lookup_key",
        how="left"
    )
)

mapped = (
    mapped
    .withColumn(
        "course_mapping_status",
        F.when(
            F.col("course_lookup_key").isNull(),
            "MISSING_SOURCE_VALUE"
        )
        .when(
            F.col("course_code").isNotNull(),
            "MAPPED"
        )
        .otherwise("UNMAPPED_OR_AMBIGUOUS")
    )
    .withColumn(
        "facility_mapping_status",
        F.when(
            F.col("facility_lookup_key").isNull(),
            "MISSING_SOURCE_VALUE"
        )
        .when(
            F.col("facility_code").isNotNull(),
            "MAPPED"
        )
        .otherwise("UNMAPPED_OR_AMBIGUOUS")
    )
    .withColumn(
        "district_standardised",
        F.coalesce(
            F.col("lookup_district"),
            F.col("district_raw"),
            F.lit("UNKNOWN")
        )
    )
    .withColumn(
        "subdistrict_standardised",
        F.coalesce(
            F.col("lookup_subdistrict"),
            F.col("subdistrict_raw")
        )
    )
)

actual_count = mapped.count()

assert actual_count == expected_count, (
    f"Lookup join multiplied/dropped rows: "
    f"{expected_count} -> {actual_count}"
)

(
    mapped.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.silver.silver_training_events_mapped")
)

print("Silver mapping complete:", actual_count, "rows")


# COMMAND ----------

# Publish unmapped and ambiguous-value exceptions


mapped = spark.table(
    "workspace.silver.silver_training_events_mapped"
)

course_exceptions = (
    mapped
    .filter(F.col("course_mapping_status") != "MAPPED")
    .groupBy(
        "source_table",
        F.lit("COURSE").alias("mapping_type"),
        F.col("course_name_raw").alias("source_value"),
        F.col("course_mapping_status").alias("mapping_status")
    )
    .agg(F.count("*").alias("affected_records"))
)

facility_exceptions = (
    mapped
    .filter(F.col("facility_mapping_status") != "MAPPED")
    .groupBy(
        "source_table",
        F.lit("FACILITY").alias("mapping_type"),
        F.col("facility_source_value").alias("source_value"),
        F.col("facility_mapping_status").alias("mapping_status")
    )
    .agg(F.count("*").alias("affected_records"))
)

mapping_exceptions = course_exceptions.unionByName(
    facility_exceptions
)

(
    mapping_exceptions.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.silver.silver_mapping_exceptions")
)

display(
    mapped.groupBy("source_table")
    .agg(
        F.count("*").alias("records"),
        F.sum(
            (F.col("course_mapping_status") == "MAPPED").cast("int")
        ).alias("course_mapped"),
        F.sum(
            (F.col("facility_mapping_status") == "MAPPED").cast("int")
        ).alias("facility_mapped")
    )
    .orderBy("source_table")
)

print("Top mapping exceptions")

display(
    mapping_exceptions.orderBy(
        F.desc("affected_records")
    ).limit(20)
)


# COMMAND ----------

# Final Silver reconciliation


from pyspark.sql import functions as F

source_expected = {
    "bronze_chw_kess": 476,
    "bronze_chw_west_coast": 22,
    "bronze_chw_witzenberg": 614,
    "bronze_capturer1": 2731,
    "bronze_capturer2": 6701,
    "bronze_capturer3": 2175,
    "bronze_online_school": 25824
}

silver_final = spark.table(
    "workspace.silver.silver_training_events_mapped"
)

actual_counts = {
    row["source_table"]: row["count"]
    for row in silver_final.groupBy("source_table").count().collect()
}

for source, expected in source_expected.items():
    actual = actual_counts.get(source, 0)
    assert actual == expected, (
        f"{source}: expected {expected}, got {actual}"
    )
    print(f"PASS: {source} = {actual:,}")

assert silver_final.count() == 38543

print("\nFINAL SILVER RECONCILIATION: PASS")
print("Total training events: 38,543")


# COMMAND ----------

# Participant identity and deduplication
# Create anonymised participant identity keys


from pyspark.sql import functions as F

events = spark.table(
    "workspace.silver.silver_training_events_mapped"
)

def identifier(column_name):
    return F.when(
        F.upper(F.trim(F.col(column_name))).isin(
            "", "NULL", "NONE", "N/A", "NA", "UNKNOWN", "-"
        ),
        F.lit(None).cast("string")
    ).otherwise(
        F.lower(F.trim(F.col(column_name)))
    )

identity = (
    events
    .withColumn("id_clean", identifier("id_number"))
    .withColumn("employee_clean", identifier("employee_number"))
    .withColumn(
        "registration_clean",
        identifier("professional_registration_number")
    )
    .withColumn("email_clean", identifier("email"))
    .withColumn(
        "identity_type",
        F.when(F.col("id_clean").isNotNull(), "ID")
         .when(F.col("employee_clean").isNotNull(), "EMPLOYEE")
         .when(F.col("registration_clean").isNotNull(), "REGISTRATION")
         .when(F.col("email_clean").isNotNull(), "EMAIL")
         .otherwise("UNIDENTIFIABLE")
    )
    .withColumn(
        "identity_value",
        F.coalesce(
            F.concat(F.lit("ID:"), F.col("id_clean")),
            F.concat(F.lit("EMP:"), F.col("employee_clean")),
            F.concat(F.lit("REG:"), F.col("registration_clean")),
            F.concat(F.lit("EMAIL:"), F.col("email_clean"))
        )
    )
    .withColumn(
        "participant_key",
        F.when(
            F.col("identity_value").isNotNull(),
            F.sha2(F.col("identity_value"), 256)
        )
    )
    .drop(
        "id_clean", "employee_clean",
        "registration_clean", "email_clean",
        "identity_value"
    )
)

identity.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "workspace.silver.silver_training_identity"
    )

assert identity.count() == 38543

display(
    identity.groupBy("identity_type")
    .count()
    .orderBy(F.desc("count"))
)


# COMMAND ----------

# Detect duplicate participant-course records


from pyspark.sql import functions as F
from pyspark.sql.window import Window

identity = spark.table(
    "workspace.silver.silver_training_identity"
)

dedup = (
    identity
    .withColumn(
        "event_date",
        F.coalesce(
            F.to_date("training_start_date"),
            F.to_date("training_end_date")
        )
    )
    .withColumn(
        "course_dedup_key",
        F.coalesce(
            F.col("course_code"),
            F.col("course_lookup_key")
        )
    )
    .withColumn(
        "duplicate_group_key",
        F.when(
            F.col("participant_key").isNotNull()
            & F.col("course_dedup_key").isNotNull()
            & F.col("event_date").isNotNull(),
            F.sha2(
                F.to_json(
                    F.struct(
                        "participant_key",
                        "course_dedup_key",
                        "event_date"
                    )
                ),
                256
            )
        )
    )
    .withColumn(
        "evidence_priority",
        F.when(
            F.col("completion_evidence")
            == "ATTENDANCE_STATUS_ATTENDED", 1
        )
        .when(
            F.col("completion_evidence")
            == "CHW_MODULE_DATE_PRESENT", 2
        )
        .when(
            F.col("completion_evidence")
            == "ONLINE_APPROVAL_DATE", 3
        )
        .otherwise(4)
    )
)

# Stable tie-breaker based on the full row content.
# This does not uniquely distinguish identical source rows.
dedup = dedup.withColumn(
    "row_fingerprint",
    F.sha2(
        F.to_json(F.struct(*[
            F.col(c)
            for c in identity.columns
        ])),
        256
    )
)

w = (
    Window.partitionBy("duplicate_group_key")
    .orderBy(
        "evidence_priority",
        "source_table",
        "source_record_id",
        "row_fingerprint"
    )
)

# Rank only rows with complete deduplication keys.
rankable = (
    dedup
    .filter(F.col("duplicate_group_key").isNotNull())
    .withColumn("duplicate_rank", F.row_number().over(w))
    .withColumn("is_duplicate", F.col("duplicate_rank") > 1)
)

# Keep incomplete-key records separately, without
# incorrectly marking them as duplicates.
unrankable = (
    dedup
    .filter(F.col("duplicate_group_key").isNull())
    .withColumn(
        "duplicate_rank",
        F.lit(None).cast("int")
    )
    .withColumn("is_duplicate", F.lit(False))
)

dedup_final = rankable.unionByName(unrankable)

assert dedup_final.count() == 38543

(
    dedup_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.silver.silver_training_deduplicated"
    )
)

display(
    dedup_final.agg(
        F.count("*").alias("total_records"),
        F.sum(
            F.col("is_duplicate").cast("int")
        ).alias("duplicate_records"),
        F.countDistinct(
            "participant_key"
        ).alias("estimated_unique_participants"),
        F.sum(
            F.col("duplicate_group_key").isNull().cast("int")
        ).alias("records_not_eligible_for_event_dedup"),
        F.sum(
            F.col("participant_key").isNull().cast("int")
        ).alias("unidentifiable_records")
    )
)


# COMMAND ----------

# Inspect duplicate evidence and source overlap


from pyspark.sql import functions as F

dedup = spark.table(
    "workspace.silver.silver_training_deduplicated"
)

print("DUPLICATES BY SOURCE")

display(
    dedup.groupBy("source_table")
    .agg(
        F.count("*").alias("source_records"),
        F.sum(
            F.col("is_duplicate").cast("int")
        ).alias("duplicate_records"),
        F.countDistinct(
            "participant_key"
        ).alias("distinct_identity_keys")
    )
    .orderBy("source_table")
)

print("IDENTITIES PRESENT IN MULTIPLE SOURCES")

display(
    dedup
    .filter(F.col("participant_key").isNotNull())
    .groupBy("participant_key")
    .agg(
        F.countDistinct("source_table").alias("source_count"),
        F.count("*").alias("training_records")
    )
    .filter(F.col("source_count") > 1)
    .agg(
        F.count("*").alias("cross_source_identity_keys"),
        F.sum("training_records").alias("associated_records")
    )
)


# COMMAND ----------

# Create final reportability classifications


from pyspark.sql import functions as F

dedup = spark.table(
    "workspace.silver.silver_training_deduplicated"
)

# Conservative test indicators.
# These rules must be reviewed against actual source conventions.
test_pattern = r"(?i)^(test|testing|dummy|sample|internal)$"

classified = (
    dedup
    .withColumn(
        "is_test_or_internal",
        F.coalesce(
            F.col("first_name").rlike(test_pattern),
            F.lit(False)
        )
        | F.coalesce(
            F.col("last_name").rlike(test_pattern),
            F.lit(False)
        )
    )
    .withColumn(
        "reportability_status",
        F.when(
            F.col("is_test_or_internal"),
            "TEST_OR_INTERNAL"
        )
        .when(
            F.col("participant_key").isNull(),
            "UNIDENTIFIABLE"
        )
        .when(
            F.col("is_duplicate"),
            "DUPLICATE"
        )
        .when(
            F.col("training_outcome") == "COMPLETION_CANDIDATE",
            "REPORTABLE_COMPLETION"
        )
        .when(
            F.col("training_outcome").isin(
                "NOT_COMPLETED",
                "CANCELLED",
                "REPLACEMENT"
            ),
            "NOT_COMPLETED"
        )
        .otherwise("UNKNOWN_OUTCOME")
    )
    .withColumn(
        "is_reportable_completion",
        F.col("reportability_status") ==
        "REPORTABLE_COMPLETION"
    )
)

classified.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "workspace.silver.silver_training_reportability"
    )

assert classified.count() == 38543

display(
    classified.groupBy("reportability_status")
    .count()
    .orderBy(F.desc("count"))
)


# COMMAND ----------

# Reconcile reportable completions


from pyspark.sql import functions as F

reportability = spark.table(
    "workspace.silver.silver_training_reportability"
)

summary = reportability.agg(
    F.count("*").alias("all_records"),
    F.sum(
        F.col("is_reportable_completion").cast("int")
    ).alias("reportable_completions"),
    F.countDistinct(
        F.when(
            F.col("is_reportable_completion"),
            F.col("participant_key")
        )
    ).alias("reportable_unique_participants"),
    F.sum(
        F.col("is_duplicate").cast("int")
    ).alias("duplicate_records"),
    F.sum(
        F.col("participant_key").isNull().cast("int")
    ).alias("unidentifiable_records")
)

display(summary)

status_total = (
    reportability
    .groupBy("reportability_status")
    .count()
    .agg(F.sum("count"))
    .first()[0]
)

assert status_total == 38543

print("Reportability reconciliation: PASS")


# COMMAND ----------

# MAGIC %md
# MAGIC # GOLD

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Gold reporting schema and shared reportable dataset

# COMMAND ----------


spark.sql(
    "CREATE SCHEMA IF NOT EXISTS workspace.gold"
)

reportability = spark.table(
    "workspace.silver.silver_training_reportability"
)

reportable = reportability.filter(
    F.col("is_reportable_completion")
)

reportable.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "workspace.gold.gold_reportable_completions"
    )

print(
    "Gold reportable completions:",
    reportable.count()
)


# COMMAND ----------

# Gold report 1: Completions by district and course


reportable = spark.table(
    "workspace.gold.gold_reportable_completions"
)

gold_district_course = (
    reportable
    .groupBy(
        "district_standardised",
        "course_code",
        "course_standardised"
    )
    .agg(
        F.count("*").alias("completion_count"),
        F.countDistinct(
            "participant_key"
        ).alias("unique_participants")
    )
)

gold_district_course.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "workspace.gold.gold_completions_by_district_course"
    )

display(
    gold_district_course.orderBy(
        F.desc("completion_count")
    ).limit(20)
)


# COMMAND ----------

# Gold report 2: Enrolments and completions by source and district


reportability = spark.table(
    "workspace.silver.silver_training_reportability"
)

eligible = reportability.filter(
    ~F.col("reportability_status").isin(
        "TEST_OR_INTERNAL",
        "UNIDENTIFIABLE",
        "DUPLICATE",
        "NOT_COMPLETED"
    )
    | (
        F.col("training_outcome") == "NOT_COMPLETED"
    )
)

eligible = eligible.filter(
    ~F.col("training_outcome").isin(
        "CANCELLED", "REPLACEMENT"
    )
)

gold_source_district = (
    eligible
    .groupBy(
        "source_table",
        "district_standardised"
    )
    .agg(
        F.count("*").alias("enrolment_count"),
        F.sum(
            F.col("is_reportable_completion").cast("int")
        ).alias("completion_count"),
        F.countDistinct(
            "participant_key"
        ).alias("unique_participants")
    )
    .withColumn(
        "not_completed_count",
        F.col("enrolment_count") -
        F.col("completion_count")
    )
)

gold_source_district.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "workspace.gold.gold_enrolments_by_source_district"
    )

display(
    gold_source_district.orderBy(
        F.desc("completion_count")
    ).limit(20)
)


# COMMAND ----------

# Gold reconciliation and headline total


reportable = spark.table(
    "workspace.gold.gold_reportable_completions"
)

gold1 = spark.table(
    "workspace.gold.gold_completions_by_district_course"
)

gold2 = spark.table(
    "workspace.gold.gold_enrolments_by_source_district"
)

headline = reportable.count()

gold1_total = gold1.agg(
    F.sum("completion_count")
).first()[0]

gold2_total = gold2.agg(
    F.sum("completion_count")
).first()[0]

assert headline == gold1_total == gold2_total, (
    f"Gold reconciliation failed: "
    f"headline={headline}, "
    f"gold1={gold1_total}, "
    f"gold2={gold2_total}"
)

assert gold2.filter(
    F.col("enrolment_count") !=
    F.col("completion_count") +
    F.col("not_completed_count")
).count() == 0

print("GOLD RECONCILIATION: PASS")
print("Headline reportable completions:", headline)
print("Gold report 1 completions:", gold1_total)
print("Gold report 2 completions:", gold2_total)


# COMMAND ----------

# end-to-end Bronze → Silver → Gold pipeline : COMPLETED.

# Gold reporting reconciliation

# 35,742
# Provisional reportable completions

# Gold report 1 — district and course
# 35,742

# Gold report 2 — source and district
# 35,742

# Enrolment arithmetic
# Passed

# COMMAND ----------

# PROJECT LIFE CYCLE

# Where we are now? 

# [Assessment phase]                             [Status]

# Bronze — raw ingestion	                     Complete
# Silver — transformations and lookup matching	 Complete
# Identity and deduplication	                 Complete
# Reportability classification	                 Implemented, provisional
# Gold — two reconciled reporting tables	     Complete
# Final quality audit	                         Remaining
# README, design document and submission	     Remaining

# COMMAND ----------

# MAGIC %md
# MAGIC # Final quality audit

# COMMAND ----------

# Final reporting audit


from pyspark.sql import functions as F

silver = spark.table(
    "workspace.silver.silver_training_events_mapped"
)

reportability = spark.table(
    "workspace.silver.silver_training_reportability"
)

gold1 = spark.table(
    "workspace.gold.gold_completions_by_district_course"
)

gold2 = spark.table(
    "workspace.gold.gold_enrolments_by_source_district"
)

assert silver.count() == 38543
assert reportability.count() == 38543

print("REPORTABILITY BREAKDOWN")
display(
    reportability.groupBy("reportability_status")
    .count()
    .orderBy(F.desc("count"))
)

print("MAPPING COVERAGE")
display(
    silver.agg(
        F.count("*").alias("total_records"),
        F.sum(
            (F.col("course_mapping_status") == "MAPPED").cast("int")
        ).alias("mapped_courses"),
        F.sum(
            (F.col("facility_mapping_status") == "MAPPED").cast("int")
        ).alias("mapped_facilities")
    )
)

print("GOLD ENROLMENT CHECK")
display(
    gold2.agg(
        F.sum("enrolment_count").alias("enrolments"),
        F.sum("completion_count").alias("completions"),
        F.sum("not_completed_count").alias("not_completed")
    )
)

print("SOURCE CONTRIBUTIONS")
display(
    reportability.groupBy("source_table")
    .agg(
        F.count("*").alias("source_records"),
        F.sum(
            F.col("is_reportable_completion").cast("int")
        ).alias("reportable_completions")
    )
    .orderBy("source_table")
)

assert gold1.agg(
    F.sum("completion_count")
).first()[0] == 35742

assert gold2.agg(
    F.sum("completion_count")
).first()[0] == 35742

print("FINAL REPORTING AUDIT: PASSED")


# COMMAND ----------

# MAGIC %md
# MAGIC Important final checks
# MAGIC There are three issues worth documenting or correcting before submission.
# MAGIC
# MAGIC 1. Enrolment definition: The current Gold enrolment population is a provisional eligibility definition. We should confirm that non-attendance and partial attendance remain enrolled, while cancelled and replacement records are excluded. The report must also explain whether online-school rows without approval evidence count as enrolments.
# MAGIC
# MAGIC 2. Duplicate detection: The 216 detected duplicate events use matching participant, course and date. The 3,880 records without a complete event key were not automatically deduplicated. Cross-source identity overlap may therefore remain.
# MAGIC
# MAGIC 3. Lookup history: Course and facility mapping currently uses normalised source names and the provided lookup snapshot. A production pipeline would version lookup changes and use effective dates to reproduce historical reports.
# MAGIC
# MAGIC These are not reasons to discard the work; they are exactly the kinds of assumptions and limitations the assessment expects you to surface.

# COMMAND ----------

# One final course-mapping diagnostic


from pyspark.sql import functions as F

mapped = spark.table(
    "workspace.silver.silver_training_events_mapped"
)

print("COURSE MAPPING COVERAGE BY SOURCE")

display(
    mapped.groupBy("source_table")
    .agg(
        F.count("*").alias("records"),
        F.sum(
            (F.col("course_mapping_status") == "MAPPED").cast("int")
        ).alias("mapped"),
        F.sum(
            (F.col("course_mapping_status") != "MAPPED").cast("int")
        ).alias("unmapped")
    )
    .withColumn(
        "mapping_pct",
        F.round(
            100 * F.col("mapped") / F.col("records"), 2
        )
    )
    .orderBy("source_table")
)

print("TOP 20 UNMAPPED COURSE VALUES")

display(
    mapped
    .filter(F.col("course_mapping_status") != "MAPPED")
    .groupBy(
        "course_name_raw",
        "source_table"
    )
    .count()
    .orderBy(F.desc("count"))
    .limit(20)
)


# COMMAND ----------

# This diagnostic identifies the main issue: course mapping works for the capturing-tool data but fails completely for CHW and online-school records.

# Source	    Mapping coverage	Interpretation

# Capturer 1	78.51%	            Mostly working
# Capturer 2	91.45%	            Working well
# Capturer 3	64.46%	            Needs some improvement
# CHW (all three)	0%	            Module names do not match the lookup
# Online school	0%	                Likely a different course naming convention

# Inspect source course names against the official lookup


from pyspark.sql import functions as F

events = spark.table(
    "workspace.silver.silver_training_events_mapped"
)

lookup = spark.table(
    "workspace.bronze.bronze_course_lookup"
)

print("ONLINE SCHOOL: TOP 15 COURSE NAMES")

display(
    events
    .filter(F.col("source_table") == "bronze_online_school")
    .groupBy("course_name_raw")
    .count()
    .orderBy(F.desc("count"))
    .limit(15)
)

print("CHW: COURSE/MODULE NAMES")

display(
    events
    .filter(F.col("source_table").startswith("bronze_chw"))
    .groupBy("course_name_raw")
    .count()
    .orderBy(F.desc("count"))
    .limit(20)
)

print("OFFICIAL COURSE LOOKUP: SAMPLE")

display(
    lookup.select(
        "CourseName",
        "CourseCode",
        "CourseGroup"
    )
    .orderBy("CourseName")
    .limit(25)
)

print("COURSE NAMES: NULL OR EMPTY BY SOURCE")

display(
    events.groupBy("source_table")
    .agg(
        F.sum(
            (
                F.col("course_name_raw").isNull()
                | (F.trim(F.col("course_name_raw")) == "")
            ).cast("int")
        ).alias("missing_course_names")
    )
    .orderBy("source_table")
)


# COMMAND ----------

# Results above show course names explain why mapping coverage is low.

# Test controlled course-name normalisation


from pyspark.sql import functions as F

events = spark.table(
    "workspace.silver.silver_training_events_mapped"
)

lookup = spark.table(
    "workspace.bronze.bronze_course_lookup"
)

def normalize_course(c):
    x = F.upper(F.trim(c))

    # Remove only a known online delivery suffix.
    x = F.regexp_replace(
        x,
        r"\s*-\s*ONLINE\s*\(WCGH\)\s*$",
        ""
    )

    # Handle titles ending in (WCGH).
    x = F.regexp_replace(
        x,
        r"\s*\(WCGH\)\s*$",
        ""
    )

    # Normalise whitespace.
    x = F.regexp_replace(x, r"\s+", " ")

    return F.trim(x)

lookup_keys = (
    lookup
    .filter(F.col("CourseName").isNotNull())
    .withColumn(
        "normalized_key",
        normalize_course(F.col("CourseName"))
    )
    .groupBy("normalized_key")
    .agg(
        F.countDistinct("CourseName").alias("name_count"),
        F.first("CourseName").alias("lookup_course_name"),
        F.first("CourseCode").alias("lookup_course_code")
    )
    .filter(F.col("name_count") == 1)
)

test_matches = (
    events
    .withColumn(
        "normalized_key",
        normalize_course(F.col("course_name_raw"))
    )
    .join(
        lookup_keys,
        on="normalized_key",
        how="left"
    )
)

display(
    test_matches.groupBy("source_table")
    .agg(
        F.count("*").alias("records"),
        F.sum(
            F.col("lookup_course_name").isNotNull().cast("int")
        ).alias("normalized_matches")
    )
    .withColumn(
        "match_pct",
        F.round(
            F.col("normalized_matches") /
            F.col("records") * 100,
            2
        )
    )
    .orderBy("source_table")
)


# COMMAND ----------

# Show the proposed online-school matches


from pyspark.sql import functions as F

display(
    test_matches
    .filter(
        (F.col("source_table") == "bronze_online_school")
        & F.col("lookup_course_name").isNotNull()
    )
    .groupBy(
        "course_name_raw",
        "lookup_course_name",
        "lookup_course_code"
    )
    .count()
    .orderBy(F.desc("count"))
    .limit(30)
)


# COMMAND ----------

# Apply the verified course mappings to Silver


from pyspark.sql import functions as F

silver = spark.table(
    "workspace.silver.silver_training_events_mapped"
)

lookup = spark.table(
    "workspace.bronze.bronze_course_lookup"
)

def normalize_course(c):
    x = F.upper(F.trim(c))
    x = F.regexp_replace(
        x, r"\s*-\s*ONLINE\s*\(WCGH\)\s*$", ""
    )
    x = F.regexp_replace(
        x, r"\s*\(WCGH\)\s*$", ""
    )
    return F.trim(F.regexp_replace(x, r"\s+", " "))

# A key is usable only when it identifies one distinct
# official course name/code combination.
course_candidates = (
    lookup
    .filter(F.col("CourseName").isNotNull())
    .withColumn(
        "normalized_course_key",
        normalize_course(F.col("CourseName"))
    )
    .select(
        "normalized_course_key",
        F.col("CourseName").alias("official_course_name"),
        F.col("CourseCode").alias("official_course_code"),
        F.col("CourseGroup").alias("official_course_group")
    )
    .dropDuplicates()
)

course_keys = (
    course_candidates
    .groupBy("normalized_course_key")
    .agg(
        F.countDistinct(
            F.struct(
                "official_course_name",
                "official_course_code",
                "official_course_group"
            )
        ).alias("candidate_count"),
        F.first("official_course_name").alias("new_course_name"),
        F.first("official_course_code").alias("new_course_code"),
        F.first("official_course_group").alias("new_course_group")
    )
    .filter(F.col("candidate_count") == 1)
    .drop("candidate_count")
)

updated = (
    silver
    .withColumn(
        "normalized_course_key",
        normalize_course(F.col("course_name_raw"))
    )
    .join(
        course_keys,
        on="normalized_course_key",
        how="left"
    )
    .withColumn(
        "course_standardised",
        F.coalesce(
            F.col("course_standardised"),
            F.col("new_course_name"),
            F.col("course_name_raw")
        )
    )
    .withColumn(
        "course_code",
        F.coalesce(
            F.col("course_code"),
            F.col("new_course_code")
        )
    )
    .withColumn(
        "course_mapping_status",
        F.when(
            F.col("course_mapping_status") == "MAPPED",
            "MAPPED"
        )
        .when(
            F.col("new_course_name").isNotNull(),
            "MAPPED"
        )
        .otherwise(F.col("course_mapping_status"))
    )
    .drop(
        "normalized_course_key",
        "new_course_name",
        "new_course_code",
        "new_course_group"
    )
)

assert updated.count() == silver.count() == 38543

updated.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "workspace.silver.silver_training_events_mapped"
    )

display(
    updated.groupBy("source_table")
    .agg(
        F.count("*").alias("records"),
        F.sum(
            (F.col("course_mapping_status") == "MAPPED").cast("int")
        ).alias("mapped")
    )
    .withColumn(
        "mapped_pct",
        F.round(100 * F.col("mapped") / F.col("records"), 2)
    )
    .orderBy("source_table")
)


# COMMAND ----------

# Propagate corrected course classifications into reporting


from pyspark.sql import functions as F

reportability = spark.table(
    "workspace.silver.silver_training_reportability"
)

corrected = spark.table(
    "workspace.silver.silver_training_events_mapped"
)

# Preserve row-level classification by joining on the
# original source event identifier.
# Check uniqueness before joining to avoid row multiplication.
key_cols = ["source_table", "source_record_id"]

duplicate_keys = (
    corrected.groupBy(*key_cols)
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Non-unique source event keys:", duplicate_keys)

if duplicate_keys > 0:
    print(
        "STOP: source_record_id is not unique. "
        "Use a stable source-row identifier before propagating."
    )
else:
    replacement = corrected.select(
        *key_cols,
        F.col("course_standardised").alias("corrected_course_name"),
        F.col("course_code").alias("corrected_course_code"),
        F.col("course_mapping_status").alias("corrected_mapping_status")
    )

    rebuilt = (
        reportability
        .drop(
            "course_standardised",
            "course_code",
            "course_mapping_status"
        )
        .join(replacement, key_cols, "left")
        .withColumnRenamed(
            "corrected_course_name", "course_standardised"
        )
        .withColumnRenamed(
            "corrected_course_code", "course_code"
        )
        .withColumnRenamed(
            "corrected_mapping_status", "course_mapping_status"
        )
    )

    assert rebuilt.count() == 38543

    rebuilt.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(
            "workspace.silver.silver_training_reportability"
        )

    print("Corrected course mapping propagated successfully.")


# COMMAND ----------

# Rebuild Gold report 1 and verify totals


from pyspark.sql import functions as F

reportability = spark.table(
    "workspace.silver.silver_training_reportability"
)

reportable = reportability.filter(
    F.col("is_reportable_completion")
)

gold1 = (
    reportable
    .groupBy(
        "district_standardised",
        "course_code",
        "course_standardised"
    )
    .agg(
        F.count("*").alias("completion_count"),
        F.countDistinct(
            "participant_key"
        ).alias("unique_participants")
    )
)

assert reportable.count() == 35742
assert gold1.agg(
    F.sum("completion_count")
).first()[0] == 35742

reportable.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "workspace.gold.gold_reportable_completions"
    )

gold1.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "workspace.gold.gold_completions_by_district_course"
    )

gold2 = spark.table(
    "workspace.gold.gold_enrolments_by_source_district"
)

assert gold2.agg(
    F.sum("completion_count")
).first()[0] == 35742

print("FINAL GOLD RECONCILIATION: PASS")
print("Headline completions:", 35742)


# COMMAND ----------

# Safely propagate course mappings without source-record joins


from pyspark.sql import functions as F

reportability = spark.table(
    "workspace.silver.silver_training_reportability"
)

lookup = spark.table(
    "workspace.bronze.bronze_course_lookup"
)

def normalize_course(c):
    x = F.upper(F.trim(c))
    x = F.regexp_replace(
        x, r"\s*-\s*ONLINE\s*\(WCGH\)\s*$", ""
    )
    x = F.regexp_replace(
        x, r"\s*\(WCGH\)\s*$", ""
    )
    return F.trim(F.regexp_replace(x, r"\s+", " "))

# Build a single mapping row for each unambiguous
# normalised course key.
candidates = (
    lookup
    .filter(
        F.col("CourseName").isNotNull()
        & F.col("CourseCode").isNotNull()
    )
    .withColumn(
        "normalized_course_key",
        normalize_course(F.col("CourseName"))
    )
    .select(
        "normalized_course_key",
        F.col("CourseName").alias("matched_course_name"),
        F.col("CourseCode").alias("matched_course_code")
    )
    .dropDuplicates()
)

course_map = (
    candidates
    .groupBy("normalized_course_key")
    .agg(
        F.count("*").alias("candidate_count"),
        F.first("matched_course_name").alias("matched_course_name"),
        F.first("matched_course_code").alias("matched_course_code")
    )
    .filter(F.col("candidate_count") == 1)
    .drop("candidate_count")
)

updated_reportability = (
    reportability
    .withColumn(
        "normalized_course_key",
        normalize_course(F.col("course_name_raw"))
    )
    .join(
        course_map,
        on="normalized_course_key",
        how="left"
    )
    .withColumn(
        "course_standardised",
        F.when(
            (F.col("course_mapping_status") != "MAPPED")
            & F.col("matched_course_name").isNotNull(),
            F.col("matched_course_name")
        ).otherwise(F.col("course_standardised"))
    )
    .withColumn(
        "course_code",
        F.when(
            (F.col("course_mapping_status") != "MAPPED")
            & F.col("matched_course_code").isNotNull(),
            F.col("matched_course_code")
        ).otherwise(F.col("course_code"))
    )
    .withColumn(
        "course_mapping_status",
        F.when(
            F.col("matched_course_name").isNotNull(),
            "MAPPED"
        ).otherwise(F.col("course_mapping_status"))
    )
    .drop(
        "normalized_course_key",
        "matched_course_name",
        "matched_course_code"
    )
)

# Validate before overwriting the existing table.
assert updated_reportability.count() == 38543

assert updated_reportability.filter(
    F.col("is_reportable_completion")
).count() == 35742

print("ROW COUNT: PASS")
print("COMPLETION COUNT: PASS")

display(
    updated_reportability.groupBy("source_table")
    .agg(
        F.count("*").alias("records"),
        F.sum(
            (F.col("course_mapping_status") == "MAPPED").cast("int")
        ).alias("mapped_courses")
    )
    .withColumn(
        "mapping_pct",
        F.round(
            F.col("mapped_courses") / F.col("records") * 100,
            2
        )
    )
    .orderBy("source_table")
)

# Persist only after validation.
(
    updated_reportability.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.silver.silver_training_reportability"
    )
)

print("COURSE MAPPING PROPAGATION: PASS")


# COMMAND ----------

# Rebuild Gold and validate the corrected course classifications


from pyspark.sql import functions as F

reportability = spark.table(
    "workspace.silver.silver_training_reportability"
)

reportable = reportability.filter(
    F.col("is_reportable_completion")
)

gold1 = (
    reportable
    .groupBy(
        "district_standardised",
        "course_code",
        "course_standardised"
    )
    .agg(
        F.count("*").alias("completion_count"),
        F.countDistinct("participant_key").alias("unique_participants")
    )
)

gold2 = spark.table(
    "workspace.gold.gold_enrolments_by_source_district"
)

headline = reportable.count()

gold1_total = gold1.agg(
    F.sum("completion_count")
).first()[0]

gold2_total = gold2.agg(
    F.sum("completion_count")
).first()[0]

assert headline == gold1_total == gold2_total == 35742

# Check online-school completions now have official codes.
display(
    reportable
    .filter(F.col("source_table") == "bronze_online_school")
    .agg(
        F.count("*").alias("online_completions"),
        F.sum(
            F.col("course_code").isNotNull().cast("int")
        ).alias("completions_with_official_course_code")
    )
)

(
    reportable.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.gold.gold_reportable_completions"
    )
)

(
    gold1.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.gold.gold_completions_by_district_course"
    )
)

print("FINAL GOLD RECONCILIATION: PASS")
print("Headline completions:", headline)
print("Gold report 1:", gold1_total)
print("Gold report 2:", gold2_total)


# COMMAND ----------

# MAGIC %md
# MAGIC # Final quality checks

# COMMAND ----------


from pyspark.sql import functions as F

silver = spark.table(
    "workspace.silver.silver_training_events_mapped"
)
reportability = spark.table(
    "workspace.silver.silver_training_reportability"
)
gold1 = spark.table(
    "workspace.gold.gold_completions_by_district_course"
)
gold2 = spark.table(
    "workspace.gold.gold_enrolments_by_source_district"
)

silver_count = silver.count()
classified_count = reportability.count()

headline = reportability.filter(
    F.col("is_reportable_completion")
).count()

gold1_total = gold1.agg(
    F.sum("completion_count")
).first()[0]

gold2_totals = gold2.agg(
    F.sum("enrolment_count").alias("enrolments"),
    F.sum("completion_count").alias("completions"),
    F.sum("not_completed_count").alias("not_completed")
).first()

assert silver_count == classified_count == 38543
assert headline == gold1_total == gold2_totals["completions"]
assert headline == 35742
assert (
    gold2_totals["enrolments"]
    == gold2_totals["completions"]
    + gold2_totals["not_completed"]
)

assert gold2.filter(
    F.col("enrolment_count")
    != F.col("completion_count") + F.col("not_completed_count")
).count() == 0

assert reportability.filter(
    F.col("reportability_status").isNull()
).count() == 0

assert reportability.filter(
    F.col("is_reportable_completion")
    & F.col("participant_key").isNull()
).count() == 0

print("FINAL QUALITY CHECKS: PASS")
print("Silver events:", silver_count)
print("Reportable completions:", headline)
print("Gold 1 completions:", gold1_total)
print("Gold 2 completions:", gold2_totals["completions"])
print("Enrolments:", gold2_totals["enrolments"])
print("Not completed:", gold2_totals["not_completed"])

print("\nFINAL COURSE MAPPING COVERAGE")
display(
    reportability.groupBy("source_table")
    .agg(
        F.count("*").alias("records"),
        F.sum(
            (F.col("course_mapping_status") == "MAPPED").cast("int")
        ).alias("mapped_courses")
    )
    .withColumn(
        "mapping_pct",
        F.round(
            100 * F.col("mapped_courses") / F.col("records"),
            2
        )
    )
    .orderBy("source_table")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL GOLD TABLES.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.gold.gold_completions_by_district_course;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.gold.gold_enrolments_by_source_district;
# MAGIC

# COMMAND ----------



# COMMAND ----------

