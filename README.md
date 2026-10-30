KTF Training Data Engineering Assessment
Overview
This project implements a reproducible Bronze–Silver–Gold data pipeline using Databricks, PySpark and Delta Lake to consolidate training data from heterogeneous sources.

The objective is to preserve source records, standardise training information, estimate unique participants, detect duplicate training events, classify reportable records and produce reconciling reporting outputs.

Data sources
Three Community Health Worker (CHW) training workbooks

One manual capturing workbook containing three sheets

One online-school CSV export

Official course and facility lookup datasets

Architecture
Bronze: Raw source preservation, source-file manifest, source lineage and Delta storage.

Silver: Source-specific transformations, date parsing, unified training-event schema, course/facility mapping, participant identity estimation, duplicate detection and reportability classification.

Gold: Reporting tables generated from the classified training records.

Key tables

Key tables
workspace.silver.silver_training_events_mapped

workspace.silver.silver_training_deduplicated

workspace.silver.silver_training_reportability

workspace.gold.gold_reportable_completions

workspace.gold.gold_completions_by_district_course

workspace.gold.gold_enrolments_by_source_district

Validated results
Metric Value

Unified training-event records 38,543

Potential duplicate records 216

Estimated distinct identity keys 16,743

Unidentifiable records 207

Provisional reportable completions 35,742

Gold completion reconciliation PASS

Execution
Configure the Databricks catalog, schemas and source volume.

Run Bronze ingestion and source-lineage creation.

Run source-specific Silver staging transformations.

Run Silver unification, lookup mapping and mapping-exception generation.

Apply controlled course-name normalisation.

Run participant identity, deduplication and reportability logic.

Propagate corrected course mappings into reportability.

Build both Gold reports.

Execute reconciliation and quality checks.

Execute notebook cells in dependency order. The initial source ZIP and required libraries, including openpyxl, must be available.

Reporting definitions
Completion: An identifiable, nonduplicate, non-test training event with a completion-candidate outcome.

Enrolment: An eligible training record included under the implemented enrolment policy.

Not completed: Enrolments minus reportable completions. This includes records with unknown completion evidence and should not be interpreted solely as confirmed failures.

Unique participants: Distinct deterministic identity keys; this is an estimate, not a verified person-level master index.

Quality controls
Bronze-to-Silver record reconciliation

Date parsing checks

Mapping-exception reporting

Duplicate-event detection

Reportability status reconciliation

Gold-to-Gold completion reconciliation

Enrolment arithmetic validation

Known limitations
CHW module dates and online-school approval dates are provisional completion evidence.

Identity matching does not resolve all cross-source identifier relationships.

Records lacking a complete event key cannot be reliably deduplicated by the current rule.

Some CHW modules remain unmatched to official course codes.

Course and facility mappings do not yet implement effective-dated historical joins.

The final pipeline should be rerun from a clean environment before production deployment.

Future improvements
Introduce effective-dated mapping dimensions, stable source-row identifiers, stronger participant identity resolution, explicit completion business rules, automated data-quality monitoring and a dimensional reporting model.