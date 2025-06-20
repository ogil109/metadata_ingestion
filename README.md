# Description

Metadata ingestion package with factory pattern to ingest data to delta lake from diverse sources.

## Usage

### Defining pipelines

One pipeline (JSON file in /pipelines) can contain multiple logically related sources mapped to two unique sinks (raw hub and delta lake), allowing for grouping of related pipelines of different types.
