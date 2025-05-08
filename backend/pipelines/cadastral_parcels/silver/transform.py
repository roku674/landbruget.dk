#!/usr/bin/env python3
"""
Cadastral Parcels Pipeline - Silver Layer

This module transforms raw cadastral data from the Bronze layer into clean,
harmonized data following project standards.
"""

import os
import logging
from pathlib import Path
from datetime import datetime
import json
from typing import Optional, Union, Dict, Any

import ibis
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import geopandas as gpd
from shapely import wkt
from google.cloud import storage

# Import utility for geometry validation
try:
    # Try to import from main project utilities if available
    from backend.pipelines.unified_pipeline.util.geometry_validator import validate_geometries
except ImportError:
    # Otherwise define a basic validation function
    def validate_geometries(gdf):
        """Basic validation for geometries."""
        # Drop invalid geometries
        valid_mask = gdf.geometry.is_valid
        if (~valid_mask).any():
            logging.warning(f"Dropping {(~valid_mask).sum()} invalid geometries")
            gdf = gdf.loc[valid_mask]
        return gdf

# Set up logger
logger = logging.getLogger(__name__)

def setup_ibis_duckdb():
    """Set up ibis with duckdb connection."""
    conn = duckdb.connect()
    # Enable spatial extension
    conn.execute("INSTALL spatial; LOAD spatial;")
    # Register the connection with ibis
    ibis.set_backend('duckdb', conn)
    return ibis.duckdb.connect()

def get_latest_bronze_path(bronze_dir: Union[str, Path]) -> Path:
    """Get the path to the latest bronze data."""
    bronze_dir = Path(bronze_dir)
    
    # Check if we're given a specific timestamp directory
    if bronze_dir.name.isdigit() and len(bronze_dir.name) >= 8:
        # This is already a timestamped directory
        if (bronze_dir / "cadastral_parcels.parquet").exists():
            return bronze_dir / "cadastral_parcels.parquet"
        else:
            # Try to find the concatenated file
            parquet_files = list(bronze_dir.glob("*.parquet"))
            if parquet_files:
                # Find the largest file (likely the concatenated one)
                return max(parquet_files, key=lambda p: p.stat().st_size)
            
    # If it's a parent directory, look for the most recent timestamped directory
    timestamp_dirs = [d for d in bronze_dir.iterdir() if d.is_dir() and d.name.isdigit() and len(d.name) >= 8]
    
    if not timestamp_dirs:
        raise FileNotFoundError(f"No bronze data directories found in {bronze_dir}")
    
    # Sort directories by name (timestamp) in descending order
    latest_dir = sorted(timestamp_dirs, reverse=True)[0]
    
    # Look for the main parquet file
    if (latest_dir / "cadastral_parcels.parquet").exists():
        return latest_dir / "cadastral_parcels.parquet"
    else:
        # Try to find the concatenated file
        parquet_files = list(latest_dir.glob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found in {latest_dir}")
        
        # Find the largest file (likely the concatenated one)
        return max(parquet_files, key=lambda p: p.stat().st_size)

def validate_and_transform_geometries(table: ibis.expr.types.Table) -> ibis.expr.types.Table:
    """
    Validate geometries and transform to EPSG:4326 using ibis/duckdb.
    
    Args:
        table: Ibis table expression with a 'geometry' column containing WKT strings
        
    Returns:
        Transformed table with validated geometries in EPSG:4326
    """
    # Check if the table has a geometry column
    if 'geometry' not in table.columns:
        raise ValueError("Table does not have a 'geometry' column")
    
    # First convert the WKT strings to DuckDB spatial objects
    table = table.mutate(
        geo_obj=ibis.expr.operations.SQLStringLiteral(f"ST_GeomFromText(geometry, 25832)")
    )
    
    # Validate geometries
    table = table.mutate(
        is_valid=ibis.expr.operations.SQLStringLiteral("ST_IsValid(geo_obj)")
    )
    
    # Filter to valid geometries only
    valid_count_before = table.count().execute()
    table = table.filter(table.is_valid == True)
    valid_count_after = table.count().execute()
    
    if valid_count_before > valid_count_after:
        logger.warning(f"Dropped {valid_count_before - valid_count_after} invalid geometries")
    
    # Transform to EPSG:4326
    table = table.mutate(
        geometry_4326=ibis.expr.operations.SQLStringLiteral("ST_AsText(ST_Transform(geo_obj, 25832, 4326))")
    )
    
    # Clean up intermediate columns and rename the transformed geometry
    table = table.drop(['geo_obj', 'is_valid', 'geometry'])
    table = table.rename({'geometry_4326': 'geometry'})
    
    return table

def clean_column_names(table: ibis.expr.types.Table) -> ibis.expr.types.Table:
    """
    Ensure column names follow the project standard.
    
    Args:
        table: Ibis table expression
        
    Returns:
        Table with standardized column names
    """
    # Rename specific columns for harmonization
    rename_map = {
        'bfe_number': 'parcel_id',   # Standardize the main identifier
        'id_local': 'local_id',      # More consistent naming
        'id_namespace': 'namespace', # Simplify
    }
    
    # Apply specific renames
    for old_name, new_name in rename_map.items():
        if old_name in table.columns:
            table = table.rename({old_name: new_name})
    
    # Ensure all column names are lowercase with underscores
    current_columns = table.columns
    for col in current_columns:
        new_col = col.lower().replace(' ', '_')
        if col != new_col:
            table = table.rename({col: new_col})
    
    return table

def cast_proper_types(table: ibis.expr.types.Table) -> ibis.expr.types.Table:
    """
    Ensure all columns have proper data types.
    
    Args:
        table: Ibis table expression
        
    Returns:
        Table with properly typed columns
    """
    # Define type mapping for columns
    type_map = {
        'parcel_id': 'int64',               # Main identifier as integer
        'is_worker_housing': 'boolean',     # Boolean flags
        'is_common_lot': 'boolean',
        'has_owner_apartments': 'boolean',
        'is_separated_road': 'boolean',
        'registration_from': 'timestamp',   # Date fields
        'effect_from': 'timestamp'
    }
    
    # Apply type casting
    for col, dtype in type_map.items():
        if col in table.columns:
            # Handle type conversion
            if dtype == 'boolean':
                # Ensure booleans are properly converted
                table = table.mutate(**{col: table[col].cast('boolean')})
            elif dtype.startswith('int'):
                # Handle potential null values gracefully
                table = table.mutate(**{col: table[col].fillna(0).cast(dtype)})
            elif dtype == 'timestamp':
                # Ensure timestamps are properly formatted
                table = table.mutate(**{col: table[col].cast(dtype)})
            else:
                table = table.mutate(**{col: table[col].cast(dtype)})
    
    return table

def add_metadata_columns(table: ibis.expr.types.Table) -> ibis.expr.types.Table:
    """
    Add standard metadata columns to the table.
    
    Args:
        table: Ibis table expression
        
    Returns:
        Table with additional metadata columns
    """
    # Add processing timestamp
    now = datetime.now()
    table = table.mutate(
        processed_at=ibis.literal(now).cast('timestamp'),
        data_source='datafordeler_cadastral',
        source_crs='EPSG:25832',
        target_crs='EPSG:4326'
    )
    
    return table

def process_cadastral_data(bronze_dir: Union[str, Path], silver_dir: Optional[Union[str, Path]] = None) -> Path:
    """
    Process raw cadastral data from bronze to silver layer.
    
    Args:
        bronze_dir: Directory containing bronze data
        silver_dir: Directory to write silver data (default: ./silver/{timestamp})
        
    Returns:
        Path to the output silver data file
    """
    logger.info("Starting silver layer processing for cadastral parcels")
    
    # Set up silver output directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if silver_dir is None:
        silver_dir = Path("./silver") / timestamp
    else:
        silver_dir = Path(silver_dir)
    
    silver_dir.mkdir(parents=True, exist_ok=True)
    
    # Get the latest bronze data file
    try:
        bronze_file = get_latest_bronze_path(bronze_dir)
        logger.info(f"Using bronze data from: {bronze_file}")
    except FileNotFoundError as e:
        logger.error(f"Bronze data not found: {e}")
        raise
    
    # Set up ibis connection
    conn = setup_ibis_duckdb()

    # Load bronze data with ibis
    try:
        # Use ibis to load the parquet file
        bronze_table = conn.read_parquet(str(bronze_file))
        logger.info(f"Loaded {bronze_table.count().execute()} records from bronze layer")
        
        # Process data
        logger.info("Cleaning column names...")
        silver_table = clean_column_names(bronze_table)
        
        logger.info("Validating and transforming geometries...")
        silver_table = validate_and_transform_geometries(silver_table)
        
        logger.info("Casting proper types...")
        silver_table = cast_proper_types(silver_table)
        
        logger.info("Adding metadata columns...")
        silver_table = add_metadata_columns(silver_table)
        
        # Set up output paths
        output_file = silver_dir / "cadastral_parcels.parquet"
        
        # Execute and materialize the result
        logger.info(f"Writing {silver_table.count().execute()} records to {output_file}")
        silver_table.to_parquet(str(output_file))
        
        # Save metadata
        metadata = {
            "source": "Datafordeleren WFS - SamletFastEjendom_Gaeldende",
            "record_count": silver_table.count().execute(),
            "bronze_file": str(bronze_file),
            "processed_at": datetime.now().isoformat(),
            "crs": "EPSG:4326",
            "columns": silver_table.columns
        }
        
        metadata_file = silver_dir / "cadastral_parcels_metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        # Also create a pointer to the latest version
        latest_link = Path(silver_dir).parent / "latest"
        if latest_link.exists() and latest_link.is_symlink():
            latest_link.unlink()
        elif latest_link.exists():
            # If it's not a symlink, rename it
            latest_link.rename(latest_link.with_name(f"latest_old_{timestamp}"))
        
        try:
            # Create relative symlink
            os.symlink(silver_dir.name, latest_link)
        except (OSError, PermissionError):
            logger.warning(f"Could not create symlink to latest version. This is expected in some environments.")
        
        # Check if we're in production environment
        environment = os.getenv('ENVIRONMENT', 'dev')
        if environment == 'prod':
            # Upload to GCS
            bucket_name = os.getenv('GCS_BUCKET_NAME')
            if bucket_name:
                logger.info(f"Uploading to GCS bucket: {bucket_name}")
                try:
                    storage_client = storage.Client()
                    bucket = storage_client.bucket(bucket_name)
                    
                    # Upload the parquet file
                    gcs_path = f"processed/cadastral/{timestamp}/cadastral_parcels.parquet"
                    bucket.blob(gcs_path).upload_from_filename(str(output_file))
                    
                    # Upload metadata
                    gcs_meta_path = f"processed/cadastral/{timestamp}/cadastral_parcels_metadata.json"
                    bucket.blob(gcs_meta_path).upload_from_filename(str(metadata_file))
                    
                    # Upload as current version too
                    bucket.blob("processed/cadastral/current.parquet").upload_from_filename(str(output_file))
                    
                    logger.info(f"Uploaded silver data to GCS: gs://{bucket_name}/{gcs_path}")
                except Exception as e:
                    logger.error(f"Failed to upload to GCS: {e}")
            else:
                logger.warning("GCS_BUCKET_NAME not set. Skipping GCS upload.")
        
        logger.info("Silver layer processing completed successfully")
        return output_file
        
    except Exception as e:
        logger.error(f"Error processing cadastral data: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    # Set up logging when run as a script
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    import argparse
    parser = argparse.ArgumentParser(description="Process cadastral data from bronze to silver layer")
    parser.add_argument("--bronze-dir", required=True, help="Directory containing bronze data")
    parser.add_argument("--silver-dir", default=None, help="Directory to write silver data")
    
    args = parser.parse_args()
    
    process_cadastral_data(args.bronze_dir, args.silver_dir)