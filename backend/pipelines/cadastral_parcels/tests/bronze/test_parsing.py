"""Test the parsing functions in the bronze layer."""

import xml.etree.ElementTree as ET
from pathlib import Path
import pytest
import asyncio
from shapely import wkt
import os

# Import the main module
from main import CadastralFetcher, clean_value, parse_args

def test_clean_value():
    """Test the clean_value function."""
    # Test string cleaning
    assert clean_value("  test  ") == "test"
    assert clean_value("") is None
    assert clean_value(None) is None
    
    # Test non-string values
    assert clean_value(123) == 123
    assert clean_value(True) is True

@pytest.mark.parametrize("args", [
    {"log_level": "INFO", "progress": True, "output_dir": "./test_bronze"},
    {"log_level": "DEBUG", "progress": False, "output_dir": "./test_bronze", "limit": 100},
])
def test_parse_args_defaults(monkeypatch, args):
    """Test the argument parser with different configurations."""
    # Prepare command line arguments
    cmd_args = []
    for key, value in args.items():
        if isinstance(value, bool) and value:
            cmd_args.append(f"--{key.replace('_', '-')}")
        elif not isinstance(value, bool):
            cmd_args.append(f"--{key.replace('_', '-')}")
            cmd_args.append(str(value))
    
    # Mock sys.argv
    monkeypatch.setattr("sys.argv", ["main.py"] + cmd_args)
    
    # Parse arguments
    parsed = parse_args()
    
    # Check that all specified args are in parsed args
    for key, value in args.items():
        assert key in parsed
        if isinstance(value, bool):
            assert parsed[key] is value
        else:
            assert str(parsed[key]) == str(value)
    
    # Check defaults for unspecified args
    if "page_size" not in args:
        assert parsed["page_size"] == 1000

@pytest.mark.asyncio
async def test_cadastral_fetcher_initialization(mock_credentials, tmp_path):
    """Test initialization of the CadastralFetcher class."""
    # Create test args
    args = {
        "log_level": "INFO",
        "progress": True,
        "output_dir": str(tmp_path),
        "page_size": 500,
        "batch_size": 1000,
        "max_concurrent": 3,
        "requests_per_second": 1,
        "request_timeout": 60,
        "total_timeout": 300
    }
    
    # Initialize fetcher
    fetcher = CadastralFetcher(args)
    
    # Check configuration was set correctly
    assert fetcher.output_dir == Path(str(tmp_path))
    assert fetcher.page_size == 500
    assert fetcher.batch_size == 1000
    assert fetcher.max_concurrent == 3
    assert fetcher.requests_per_second == 1
    assert fetcher.request_timeout == 60
    assert fetcher.total_timeout == 300
    assert fetcher.username == "test_user"
    assert fetcher.password == "test_pass"
    assert fetcher.url == "https://test.example.com/wfs"
    
    # Check request parameters
    params = fetcher._get_params(100)
    assert params["startIndex"] == "100"
    assert params["count"] == "500"
    assert params["username"] == "test_user"
    assert params["password"] == "test_pass"
    assert params["SERVICE"] == "WFS"

def test_parse_feature(mock_credentials, tmp_path):
    """Test parsing a feature from XML."""
    # Create test args
    args = {
        "log_level": "INFO",
        "progress": False,
        "output_dir": str(tmp_path)
    }
    
    # Initialize fetcher
    fetcher = CadastralFetcher(args)
    
    # Create a sample XML feature element
    xml_string = """
    <mat:SamletFastEjendom_Gaeldende xmlns:mat="http://data.gov.dk/schemas/matrikel/1" xmlns:gml="http://www.opengis.net/gml/3.2">
        <mat:BFEnummer>12345</mat:BFEnummer>
        <mat:forretningshaendelse>Opdatering</mat:forretningshaendelse>
        <mat:forretningsproces>Myndighedsregister</mat:forretningsproces>
        <mat:senesteSagLokalId>1234567890</mat:senesteSagLokalId>
        <mat:id_lokalId>local123</mat:id_lokalId>
        <mat:id_namespace>namespace123</mat:id_namespace>
        <mat:registreringFra>2023-01-01T00:00:00Z</mat:registreringFra>
        <mat:virkningFra>2023-01-01T00:00:00Z</mat:virkningFra>
        <mat:virkningsaktoer>GST</mat:virkningsaktoer>
        <mat:arbejderbolig>false</mat:arbejderbolig>
        <mat:erFaelleslod>false</mat:erFaelleslod>
        <mat:hovedejendomOpdeltIEjerlejligheder>false</mat:hovedejendomOpdeltIEjerlejligheder>
        <mat:udskiltVej>false</mat:udskiltVej>
        <mat:landbrugsnotering>Landbrugsejendom</mat:landbrugsnotering>
        <mat:geometri>
            <gml:MultiSurface>
                <gml:surfaceMember>
                    <gml:Polygon>
                        <gml:exterior>
                            <gml:LinearRing>
                                <gml:posList>
                                    10 55 0 10.1 55 0 10.1 55.1 0 10 55.1 0 10 55 0
                                </gml:posList>
                            </gml:LinearRing>
                        </gml:exterior>
                    </gml:Polygon>
                </gml:surfaceMember>
            </gml:MultiSurface>
        </mat:geometri>
    </mat:SamletFastEjendom_Gaeldende>
    """
    
    feature_elem = ET.fromstring(xml_string)
    
    # Parse the feature
    feature = fetcher._parse_feature(feature_elem)
    
    # Check the parsed feature
    assert feature["bfe_number"] == 12345
    assert feature["business_event"] == "Opdatering"
    assert feature["business_process"] == "Myndighedsregister"
    assert feature["latest_case_id"] == "1234567890"
    assert feature["id_local"] == "local123"
    assert feature["id_namespace"] == "namespace123"
    assert feature["authority"] == "GST"
    assert feature["is_worker_housing"] is False
    assert feature["is_common_lot"] is False
    assert feature["has_owner_apartments"] is False
    assert feature["is_separated_road"] is False
    assert feature["agricultural_notation"] == "Landbrugsejendom"
    
    # Check that geometry was parsed
    assert "geometry" in feature
    geom = wkt.loads(feature["geometry"])
    assert geom.is_valid
    
def test_parse_geometry(mock_credentials, tmp_path):
    """Test parsing geometry from XML."""
    # Create test args
    args = {
        "log_level": "INFO",
        "progress": False,
        "output_dir": str(tmp_path)
    }
    
    # Initialize fetcher
    fetcher = CadastralFetcher(args)
    
    # Create a sample XML geometry element
    xml_string = """
    <gml:MultiSurface xmlns:gml="http://www.opengis.net/gml/3.2">
        <gml:surfaceMember>
            <gml:Polygon>
                <gml:exterior>
                    <gml:LinearRing>
                        <gml:posList>
                            10 55 0 10.1 55 0 10.1 55.1 0 10 55.1 0 10 55 0
                        </gml:posList>
                    </gml:LinearRing>
                </gml:exterior>
            </gml:Polygon>
        </gml:surfaceMember>
    </gml:MultiSurface>
    """
    
    geom_elem = ET.fromstring(xml_string)
    
    # Parse the geometry
    geom_wkt = fetcher._parse_geometry(geom_elem)
    
    # Check the parsed geometry
    assert geom_wkt is not None
    geom = wkt.loads(geom_wkt)
    assert geom.is_valid
    assert geom.geom_type in ("Polygon", "MultiPolygon")
    
    # Test with invalid or empty geometry
    xml_invalid = """
    <gml:MultiSurface xmlns:gml="http://www.opengis.net/gml/3.2">
        <gml:surfaceMember>
            <gml:Polygon>
                <gml:exterior>
                    <gml:LinearRing>
                        <gml:posList>
                            10 55 0 10.1 55 0
                        </gml:posList>
                    </gml:LinearRing>
                </gml:exterior>
            </gml:Polygon>
        </gml:surfaceMember>
    </gml:MultiSurface>
    """
    
    geom_elem_invalid = ET.fromstring(xml_invalid)
    geom_wkt_invalid = fetcher._parse_geometry(geom_elem_invalid)
    assert geom_wkt_invalid is None  # Not enough points to form a polygon

def test_save_to_parquet(mock_credentials, tmp_path, sample_features_list):
    """Test saving features to parquet."""
    # Create test args
    args = {
        "log_level": "INFO",
        "progress": False,
        "output_dir": str(tmp_path)
    }
    
    # Initialize fetcher
    fetcher = CadastralFetcher(args)
    
    # Define output path
    output_path = tmp_path / "test_output.parquet"
    
    # Save to parquet
    fetcher.save_to_parquet(sample_features_list, output_path)
    
    # Check that the file was created
    assert output_path.exists()
    
    # Check that metadata file was created
    metadata_path = output_path.with_suffix('.json')
    assert metadata_path.exists()
    
    # Try to read back the parquet file
    import geopandas as gpd
    gdf = gpd.read_parquet(output_path)
    
    # Check that we have the right number of features
    assert len(gdf) == len(sample_features_list)
    
    # Check that all required columns are present
    assert "bfe_number" in gdf.columns
    assert "geometry" in gdf.columns
    
    # Check CRS
    assert gdf.crs == "EPSG:25832"