# Statista Healthcare – Leapfrog Data Integration (Analytics Engineering Case Study)

## Project Overview

This repository presents a dbt based data model for integrating Leapfrog hospital safety grades and patient safety metrics into the Statista Healthcare Data Platform.

The solution focuses on auditability, reproducibility, conservative entity resolution, and long term scalability across vendors and domains.

The project supports three primary stakeholders.

### Rankings Lead

- Reproducible historical snapshots
- Audit ready results for any publication window
- Transparent entity resolution rules
- Explicit handling of incomplete reporting

### Product Owner

- Longitudinal hospital benchmarking
- Drill down into component level safety metrics
- Queryable match confidence and resolution method
- Reusable quality metric foundation across vendors

### Data Platform Lead

- Minimal disruption to existing metric structures
- Clear data contracts and lineage
- Predictable compute through incremental and snapshot strategies
- Scalable architecture for future integrations

---

## Repository Structure and Domain Architecture

This repository follows a domain based mono repo structure aligned with Statista’s layered data platform approach.

The platform separates concerns across raw ingestion, domain modeling, semantic metrics, and product datasets. This repository focuses on the domain modeling layer and above.

Each domain lives at the repository root and represents an isolated business area.

Example domains include:
- healthcare
- product
- finance

Domain design rules:
- A domain does not reference models from another domain
- Domains do not depend on each other at build time
- Failures remain isolated within a single domain
- Cross domain usage happens only downstream through external tables or BI tools

This structure ensures operational resilience and clear ownership boundaries.

The healthcare domain is implemented as a standalone dbt package at the repository root. All SQL and modeling logic used in this case study live entirely inside the healthcare domain.

---

## Healthcare Domain Scope

The healthcare domain models hospital level quality, safety, and benchmarking data.

It integrates Leapfrog data into the Statista Healthcare Data Platform while preserving historical accuracy and conservative entity resolution.

This domain owns:
- Hospital master enrichment
- Vendor specific quality and safety metrics
- Unified quality metric facts
- Historical snapshots for audit use cases

All inputs enter through staging tables owned by the ingestion layer. No cross domain model references exist.

---

## Key Assumptions

- CMS Provider ID is the authoritative national identifier for hospitals
- Hospitals in different locations never share a CMS Provider ID
- stg_hospitals represents the enterprise hospital master
- Fuzzy name matching is used only when CMS Provider ID is missing
- City and state must match exactly for fuzzy resolution
- Leapfrog republishes historical results
- First seen values drive rankings and audits
- Snowflake serves as the data warehouse
- Hospital metadata changes slowly
- Conservative matching takes priority over coverage
- All quality metrics conform to a unified schema

---

## Entity Resolution Strategy

Resolution follows a strict order.

Primary rule:
- Exact CMS Provider ID match

Fallback rule:
- Exact city and state match
- Jaro Winkler similarity score greater than or equal to 90

Records without a valid match remain unmatched.

Each resolved record exposes:
- match_method
- match_confidence

---

## Layered Data Model

### Staging Layer

- Light transformations
- Naming alignment
- Type casting

### Intermediate Layer

- Entity resolution logic
- Data quality rules
- Metric normalization

### Snapshot Layer

- SCD2 snapshots
- First seen value preservation
- Full audit reproducibility

### Mart Layer

- Analytics ready dimensions and facts
- Stable schemas for BI consumption

---

## Core Models

Dimensions:
- dim_hospital

Facts:
- fct_leapfrog_safety_grades
- fct_quality_metrics_unified

---

## Materialization Strategy

- Staging models use views
- Intermediate models use views
- Snapshot models use SCD2
- Mart models use tables

---

## Data Quality and Governance

- YAML based model documentation
- Column level tests for keys and nullability
- Explicit data completeness flags
- Clear ownership within the healthcare domain

---

# Data Testing and Observability

### Ensuring Data Quality and Observability with Elementary and dbt Project Evaluator

Maintaining high data quality and monitoring the health of a data pipeline are critical to ensuring that downstream analyses and decisions are based on reliable information. In this project, Elementary and dbt Project Evaluator can used to implement data testing and provide robust data observability across the pipeline.


## Observability with Elementary
Elementary extends beyond traditional data testing by offering real-time data observability.
Observability in this context means gaining insight into the health of the data pipeline through continuous monitoring, automated alerts, and tracking data metrics over time.

## Project Evaluation with dbt Project Evaluator
In addition to data-level testing, the dbt Project Evaluator can be used to assess and monitor the overall health and performance of the dbt project itself. 


## CI/CD

![CI/CD](CI_CD_PROCESS.png)
