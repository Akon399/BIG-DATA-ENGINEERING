KTF Training Data Pipeline — Technical Design
1. Architecture and design decisions
The solution follows a Medallion architecture implemented in Databricks using PySpark and Delta Lake.

Bronze preserves source-level information and lineage. Silver handles standardisation, enrichment, identity estimation, deduplication and reportability. Gold provides business-facing reporting aggregations.

Separating these stages supports traceability, debugging and future reprocessing.

2. Source ingestion and reproducibility
The solution ingests CHW spreadsheets, capturing-tool worksheets, online-school records and official lookup files.

A source-file manifest records file-level information, including hashes. CHW source-row lineage preserves worksheet-level details needed to reconstruct module training events.

Delta tables provide persisted intermediate outputs so transformations can be rebuilt without repeatedly interpreting the original Excel and CSV files.

A full clean-environment rerun remains a final verification requirement.

3. Data standardisation and lookup mapping
Course and facility values are standardised using official lookup datasets.

Course matching uses controlled case and whitespace normalisation. For online-school records, recognised delivery suffixes such as - Online (WCGH) are removed before exact matching.

This approach preserves course-year distinctions, including PACK 2023 and PACK 2025.

Ambiguous lookup matches are not automatically assigned. Unmatched values remain visible for investigation.

CHW module names are preserved as source-defined training events where no defensible official match exists.

Profession values are standardised directly from source text.

4. Participant identity and deduplication
Participant identity is estimated using the following identifier priority:

ID number

Employee number

Professional registration number

Email address

A deterministic hash is generated for the selected identity key.

This approach supports repeatable counting but does not guarantee complete cross-source identity resolution. Records containing different identifiers for the same person may remain unlinked.

Potential duplicate events are identified using participant identity, course and event date. The current implementation detected 216 potential duplicate records.

Records lacking a complete matching key are retained rather than automatically discarded.

5. Reportability and completion evidence
Reportability classification distinguishes completion candidates, non-completions, unknown outcomes, duplicate records and unidentifiable records.

The implemented completion definition excludes duplicates, unidentifiable records and explicitly identified test/internal records.

Completion evidence differs by source:

Capturing tool: attended status

CHW: populated module training date

Online school: approval date

CHW and online-school indicators are treated as provisional completion evidence rather than independently verified completions.

The resulting headline is 35,742 provisional reportable completions.

6. Reporting and reconciliation
Gold output 1 aggregates completions by district and course.

Gold output 2 aggregates enrolments, completions, not-completed counts and distinct identity keys by source and district.

Both outputs reconcile to 35,742 completions.

The enrolment arithmetic also reconciles:

37,980 enrolments = 35,742 completions + 2,238 not completed.

Distinct participant counts are non-additive across reporting groups and must not be summed to obtain an overall unique-participant total.

7. Data quality and limitations
Implemented checks include source reconciliation, date parsing validation, mapping coverage, duplicate detection, reportability reconciliation and Gold reporting reconciliation.

Important limitations include:

Incomplete course mapping for source-defined CHW modules

Incomplete participant linkage across identifier types

Missing complete event keys for some records

Non-unique content-hash source identifiers

Provisional completion semantics

Snapshot-based lookup matching without effective-dated historical resolution

8. Historical reproducibility
A production implementation should store mapping version, effective-from date, effective-to date and processing timestamp.

Each training event would resolve to the mapping valid for its business event date, allowing historical reports to be reproduced even after reference data changes.

9. Future dimensional model
A production reporting model could introduce:

Participant dimension

Course dimension

Facility and geography dimensions

Date dimension

Training event fact table

Enrolment and completion measures

Surrogate keys, effective-dated dimensions and controlled identity resolution would improve longitudinal reporting.

10. Recommended next steps
1.Confirm completion and enrolment rules with business stakeholders.

2.Complete the clean-environment rerun.

3.Add automated validation to scheduled jobs.

4.Introduce stable source-row identifiers.

5.Version course and facility mappings.

6.Implement governed participant identity linkage.

7.Add data-quality dashboards and operational monitoring.