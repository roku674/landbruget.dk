# Cadastral Parcels Pipeline

This pipeline fetches and processes Danish cadastral parcel data from Datafordeleren WFS service. It follows the project's medallion architecture with Bronze and Silver layers.

## Purpose

The Cadastral Parcels Pipeline provides clean, harmonized geospatial data about Danish land parcels. This data is useful for:

- Property analysis and mapping
- Agricultural field boundaries correlation
- Land use and ownership analysis
- Spatial reference for other datasets

## Architecture

The pipeline follows a medallion architecture:

1. **Bronze Layer**: Raw data ingestion from Datafordeleren WFS
   - Preserves original data without transformation
   - Uses EPSG:25832 projection (as provided by the source)
   - Stores data in GeoParquet format

2. **Silver Layer**: Data cleaning and harmonization
   - Validates and transforms geometries to EPSG:4326
   - Standardizes column names and data types
   - Creates clean, analysis-ready dataset in GeoParquet format

## Setup

### Prerequisites

- Python 3.10+
- Docker and Docker Compose (for containerized execution)
- Access credentials for Datafordeleren service

### Environment Variables

Create a `.env` file based on the provided `.env.example`:

```bash
cp .env.example .env
```

Then edit the `.env` file to add your credentials and configuration.

### Local Development

1. Set up a Python virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:

```bash
pip install -e .
```

3. Run the pipeline:

```bash
python main.py --log-level INFO --progress
```

### Using Docker

Build and run the pipeline using Docker Compose:

```bash
docker-compose build
docker-compose up
```

## Usage

### Command Line Arguments

The pipeline supports various command line arguments:

```
--log-level: Logging level (DEBUG, INFO, WARNING, ERROR)
--progress: Show progress information
--output-dir: Output directory for bronze data
--page-size: Number of features per WFS request
--batch-size: Number of features per batch write
--max-concurrent: Maximum number of concurrent requests
--requests-per-second: Maximum requests per second (rate limiting)
--request-timeout: Request timeout in seconds
--total-timeout: Total pipeline timeout in seconds
--limit: Limit total number of features fetched (for testing)
--run-silver: Run silver layer processing after bronze
```

### Examples

Run the full pipeline with progress information:

```bash
python main.py --log-level INFO --progress --run-silver
```

Run only the bronze layer with a limit for testing:

```bash
python main.py --limit 1000 --log-level DEBUG
```

Process existing bronze data with the silver layer:

```bash
python -c "from silver.transform import process_cadastral_data; process_cadastral_data('./bronze')"
```

## Data Schema

### Bronze Layer

The bronze layer preserves the original schema from Datafordeleren with the following key fields:

| Field | Description |
|-------|-------------|
| bfe_number | Unique parcel identifier (BFE number) |
| business_event | Business event associated with the parcel |
| business_process | Business process associated with the parcel |
| latest_case_id | Latest case ID |
| id_local | Local ID |
| id_namespace | Namespace for the ID |
| registration_from | Registration date |
| effect_from | Date when the record takes effect |
| authority | Authority responsible for the record |
| is_worker_housing | Flag for worker housing |
| is_common_lot | Flag for common lot |
| has_owner_apartments | Flag for parcels with owner apartments |
| is_separated_road | Flag for separated road |
| agricultural_notation | Agricultural notation |
| geometry | Parcel geometry in WKT format (EPSG:25832) |

### Silver Layer

The silver layer transforms the data into a cleaned schema:

| Field | Description | Type |
|-------|-------------|------|
| parcel_id | Unique parcel identifier | integer |
| business_event | Business event | string |
| business_process | Business process | string |
| latest_case_id | Latest case ID | string |
| local_id | Local ID | string |
| namespace | Namespace | string |
| registration_from | Registration date | timestamp |
| effect_from | Date when the record takes effect | timestamp |
| authority | Authority | string |
| is_worker_housing | Worker housing flag | boolean |
| is_common_lot | Common lot flag | boolean |
| has_owner_apartments | Owner apartments flag | boolean |
| is_separated_road | Separated road flag | boolean |
| agricultural_notation | Agricultural notation | string |
| geometry | Validated parcel geometry | WKT (EPSG:4326) |
| processed_at | Processing timestamp | timestamp |
| data_source | Source identifier | string |
| source_crs | Original CRS | string |
| target_crs | Target CRS | string |

## Development

### Testing

Run tests using pytest:

```bash
pytest
```

### Project Structure

```
cadastral_parcels/
├── .env.example          # Example environment variables
├── .env                  # Local environment variables (gitignored)
├── README.md             # This documentation
├── main.py               # Main pipeline code
├── Dockerfile            # Container definition
├── docker-compose.yml    # Container orchestration
├── pyproject.toml        # Python project configuration
├── bronze/               # Bronze layer modules
│   └── __init__.py       # Package initialization
└── silver/               # Silver layer modules
    ├── __init__.py       # Package initialization
    └── transform.py      # Silver layer transformation code
```

## References

- [Datafordeleren Documentation](https://datafordeler.dk/vejledning)
- [Project Standards](../../README.md)
- [Pipeline Guidelines](../README.md)