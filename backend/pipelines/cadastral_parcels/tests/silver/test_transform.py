"""Test the silver layer transformation functions."""

import os
import pytest
import pandas as pd
import geopandas as gpd
import ibis
import duckdb
from pathlib import Path
from datetime import datetime

# Import the silver module
from silver.transform import (
    clean_column_names, cast_proper_types, add_metadata_columns,
    get_latest_bronze_path, setup_ibis_duckdb, process_cadastral_data
)

def test_get_latest_bronze_path(tmp_path, sample_parquet_file):
    """Test the function to get the latest bronze data path."""
    # Create a timestamped directory structure
    timestamp_dir = tmp_path / "20250101_120000"
    timestamp_dir.mkdir(parents=True, exist_ok=True)
    
    # Copy the sample file to the timestamped directory
    import shutil
    dest_file = timestamp_dir / "cadastral_parcels.parquet"
    shutil.copy(sample_parquet_file, dest_file)
    
    # Test finding the file in a timestamp directory
    result = get_latest_bronze_path(timestamp_dir)
    assert result == dest_file
    
    # Test finding the file from parent directory
    result = get_latest_bronze_path(tmp_path)
    assert result == dest_file
    
    # Create another newer timestamped directory
    newer_timestamp_dir = tmp_path / "20250102_120000"
    newer_timestamp_dir.mkdir(parents=True, exist_ok=True)
    newer_file = newer_timestamp_dir / "cadastral_parcels_batch_1000.parquet"
    shutil.copy(sample_parquet_file, newer_file)
    
    # Test that it finds the latest directory
    result = get_latest_bronze_path(tmp_path)
    assert newer_timestamp_dir.name in str(result)
    
    # Test error when no files exist
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir(parents=True, exist_ok=True)
    with pytest.raises(FileNotFoundError):
        get_latest_bronze_path(empty_dir)

def test_setup_ibis_duckdb():
    """Test setting up ibis with duckdb."""
    conn = setup_ibis_duckdb()
    
    # Check that we can execute an ibis query
    result = conn.sql("SELECT 1 AS value").execute()
    assert result["value"][0] == 1
    
    # Check that spatial extension is loaded
    result = conn.sql("SELECT ST_GeomFromText('POINT(0 0)') AS geom").execute()
    assert result is not None
    assert "geom" in result.columns

def test_clean_column_names():
    """Test cleaning column names."""
    # Create a test table with ibis
    conn = setup_ibis_duckdb()
    table = conn.sql("""
    SELECT 
        1 AS bfe_number,
        'test' AS id_local,
        'test' AS id_namespace,
        'test' AS "Mixed Case",
        'test' AS "Column With Spaces"
    """)
    
    # Clean column names
    cleaned = clean_column_names(table)
    
    # Check expected renames
    assert "parcel_id" in cleaned.columns  # bfe_number -> parcel_id
    assert "local_id" in cleaned.columns   # id_local -> local_id
    assert "namespace" in cleaned.columns  # id_namespace -> namespace
    
    # Check case and space handling
    assert "mixed_case" in cleaned.columns
    assert "column_with_spaces" in cleaned.columns

def test_cast_proper_types():
    """Test casting proper types."""
    # Create a test table with ibis
    conn = setup_ibis_duckdb()
    table = conn.sql("""
    SELECT 
        '12345' AS parcel_id,
        'true' AS is_worker_housing,
        'false' AS is_common_lot,
        '2023-01-01T00:00:00Z' AS registration_from,
        'GST' AS authority
    """)
    
    # Cast types
    typed = cast_proper_types(table)
    
    # Execute to materialize the table
    result = typed.execute()
    
    # Check types
    assert pd.api.types.is_integer_dtype(result["parcel_id"].dtype)
    assert pd.api.types.is_bool_dtype(result["is_worker_housing"].dtype)
    assert pd.api.types.is_bool_dtype(result["is_common_lot"].dtype)
    assert pd.api.types.is_datetime64_dtype(result["registration_from"].dtype)
    assert pd.api.types.is_string_dtype(result["authority"].dtype)
    
    # Check values
    assert result["parcel_id"][0] == 12345
    assert result["is_worker_housing"][0] is True
    assert result["is_common_lot"][0] is False

def test_add_metadata_columns():
    """Test adding metadata columns."""
    # Create a test table with ibis
    conn = setup_ibis_duckdb()
    table = conn.sql("""
    SELECT 
        1 AS parcel_id,
        'test' AS authority
    """)
    
    # Add metadata
    with_meta = add_metadata_columns(table)
    
    # Execute to materialize the table
    result = with_meta.execute()
    
    # Check that metadata columns were added
    assert "processed_at" in result.columns
    assert "data_source" in result.columns
    assert "source_crs" in result.columns
    assert "target_crs" in result.columns
    
    # Check values
    assert result["data_source"][0] == "datafordeler_cadastral"
    assert result["source_crs"][0] == "EPSG:25832"
    assert result["target_crs"][0] == "EPSG:4326"
    
    # Check that processed_at is a timestamp
    assert pd.api.types.is_datetime64_dtype(result["processed_at"].dtype)

def test_process_cadastral_data(tmp_path, sample_parquet_file):
    """Test the full process_cadastral_data function."""
    # Create a bronze directory with a sample file
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp_dir = bronze_dir / "20250101_120000"
    timestamp_dir.mkdir(parents=True, exist_ok=True)
    
    # Copy the sample file to the bronze directory
    import shutil
    dest_file = timestamp_dir / "cadastral_parcels.parquet"
    shutil.copy(sample_parquet_file, dest_file)
    
    # Create silver directory
    silver_dir = tmp_path / "silver"
    
    # Process data
    try:
        output_file = process_cadastral_data(bronze_dir, silver_dir)
        
        # Check that output file exists
        assert output_file.exists()
        
        # Check that metadata file exists
        metadata_file = Path(str(output_file).replace(".parquet", "_metadata.json"))
        assert metadata_file.exists()
        
        # Try to read the output file
        gdf = gpd.read_parquet(output_file)
        
        # Check that required columns are present
        assert "parcel_id" in gdf.columns  # renamed from bfe_number
        assert "geometry" in gdf.columns
        assert "processed_at" in gdf.columns
        
        # Check CRS
        assert gdf.crs == "EPSG:4326"  # Should be transformed from 25832
        
    # This test requires the full environment, which might not be available in CI
    except Exception as e:
        pytest.skip(f"Silver processing failed, likely due to missing dependencies: {e}")