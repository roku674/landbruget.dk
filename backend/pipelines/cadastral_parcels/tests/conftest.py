"""Test configuration and fixtures for the Cadastral Parcels Pipeline."""

import os
import sys
import pytest
from pathlib import Path
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
from shapely import wkt
import pyarrow as pa
import pyarrow.parquet as pq

# Add the project root to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

@pytest.fixture
def mock_credentials():
    """Mock credentials for testing."""
    os.environ["DATAFORDELER_USERNAME"] = "test_user"
    os.environ["DATAFORDELER_PASSWORD"] = "test_pass"
    os.environ["DATAFORDELER_URL"] = "https://test.example.com/wfs"
    os.environ["ENVIRONMENT"] = "dev"
    yield
    # Clean up
    for key in ["DATAFORDELER_USERNAME", "DATAFORDELER_PASSWORD", "DATAFORDELER_URL", "ENVIRONMENT"]:
        if key in os.environ:
            del os.environ[key]

@pytest.fixture
def sample_feature():
    """Create a sample cadastral feature for testing."""
    return {
        "bfe_number": 12345,
        "business_event": "Opdatering",
        "business_process": "Myndighedsregister",
        "latest_case_id": "1234567890",
        "id_local": "local123",
        "id_namespace": "namespace123",
        "registration_from": "2023-01-01T00:00:00Z",
        "effect_from": "2023-01-01T00:00:00Z",
        "authority": "GST",
        "is_worker_housing": "false",
        "is_common_lot": "false",
        "has_owner_apartments": "false",
        "is_separated_road": "false",
        "agricultural_notation": "Landbrugsejendom",
        "geometry": wkt.dumps(Polygon([(10, 55), (10.1, 55), (10.1, 55.1), (10, 55.1), (10, 55)]))
    }

@pytest.fixture
def sample_features_list():
    """Create a list of sample features."""
    features = []
    for i in range(10):
        feature = {
            "bfe_number": 10000 + i,
            "business_event": "Opdatering",
            "business_process": "Myndighedsregister",
            "latest_case_id": f"case_{i}",
            "id_local": f"local_{i}",
            "id_namespace": "namespace123",
            "registration_from": "2023-01-01T00:00:00Z",
            "effect_from": "2023-01-01T00:00:00Z",
            "authority": "GST",
            "is_worker_housing": "false",
            "is_common_lot": "false",
            "has_owner_apartments": "false",
            "is_separated_road": "false",
            "agricultural_notation": "Landbrugsejendom",
            "geometry": wkt.dumps(Polygon([(10 + i/100, 55), (10.1 + i/100, 55), 
                                         (10.1 + i/100, 55.1), (10 + i/100, 55.1), 
                                         (10 + i/100, 55)]))
        }
        features.append(feature)
    return features

@pytest.fixture
def sample_parquet_file(tmp_path, sample_features_list):
    """Create a sample parquet file with test features."""
    # Create a GeoDataFrame
    df = pd.DataFrame([{k:v for k,v in f.items() if k != 'geometry'} for f in sample_features_list])
    geometries = [wkt.loads(f['geometry']) for f in sample_features_list]
    gdf = gpd.GeoDataFrame(df, geometry=geometries, crs="EPSG:25832")
    
    # Save to parquet
    output_file = tmp_path / "test_cadastral.parquet"
    gdf.to_parquet(output_file)
    
    return output_file