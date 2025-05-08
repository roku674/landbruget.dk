#!/usr/bin/env python3
"""
Cadastral Parcels Pipeline - Bronze Layer

This pipeline fetches raw Danish cadastral parcel data from WFS and stores it
in its raw form without transformations.
"""

import argparse
import asyncio
import logging
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union, Any

import aiohttp
import backoff
from aiohttp import ClientError, ClientTimeout
from dotenv import load_dotenv
from tqdm import tqdm
import duckdb
import ibis
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
from shapely import wkt
from shapely.ops import make_valid
from google.cloud import storage

# Initialize logging
logger = logging.getLogger(__name__)

def setup_logging(log_level: str):
    """Configure logging with the specified level."""
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    # Remove all existing handlers to start fresh
    root = logging.getLogger()
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    # Set up root logger with a format that works well with tqdm
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Configure third-party loggers to WARNING or higher
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('google').setLevel(logging.WARNING)

    # Prevent log propagation for specific modules when not in DEBUG
    if numeric_level > logging.DEBUG:
        for logger_name in ['requests', 'urllib3', 'google']:
            logging.getLogger(logger_name).propagate = False

def parse_args() -> Dict[str, Any]:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Cadastral Parcels Pipeline")
    
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                      default='INFO', help='Logging level')
    parser.add_argument('--progress', action='store_true',
                      help='Show progress information')
    parser.add_argument('--output-dir', type=str, default='./bronze',
                      help='Output directory for bronze data')
    parser.add_argument('--page-size', type=int, default=1000,
                      help='Number of features per WFS request')
    parser.add_argument('--batch-size', type=int, default=5000,
                      help='Number of features per batch write')
    parser.add_argument('--max-concurrent', type=int, default=5,
                      help='Maximum number of concurrent requests')
    parser.add_argument('--requests-per-second', type=int, default=2,
                      help='Maximum requests per second (rate limiting)')
    parser.add_argument('--request-timeout', type=int, default=300,
                      help='Request timeout in seconds')
    parser.add_argument('--total-timeout', type=int, default=7200,
                      help='Total pipeline timeout in seconds')
    parser.add_argument('--limit', type=int,
                      help='Limit total number of features fetched (for testing)')
    parser.add_argument('--run-silver', action='store_true',
                      help='Run silver layer processing after bronze')
    
    return vars(parser.parse_args())

def clean_value(value):
    """Clean string values."""
    if not isinstance(value, str):
        return value
    value = value.strip()
    return value if value else None

class CadastralFetcher:
    """Fetches Danish cadastral parcels from WFS."""
    
    def __init__(self, args: Dict[str, Any]):
        """Initialize with configuration."""
        self.args = args
        
        # Set up output path
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.output_dir = Path(args['output_dir'])
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load environment variables
        load_dotenv()
        self.username = os.getenv('DATAFORDELER_USERNAME')
        self.password = os.getenv('DATAFORDELER_PASSWORD')
        self.url = os.getenv('DATAFORDELER_URL')
        if not self.username or not self.password or not self.url:
            raise ValueError("Missing required environment variables: DATAFORDELER_USERNAME, DATAFORDELER_PASSWORD, or DATAFORDELER_URL")
        
        # WFS request configuration
        self.page_size = args['page_size']
        self.batch_size = args['batch_size']
        self.max_concurrent = args['max_concurrent']
        self.request_timeout = args['request_timeout'] 
        self.total_timeout = args['total_timeout']
        self.requests_per_second = args['requests_per_second'] 
        
        # Tracking and rate limiting
        self.last_request_time = {}
        self.request_semaphore = asyncio.Semaphore(self.max_concurrent)
        
        # Timeouts for different operations
        self.request_timeout_config = ClientTimeout(
            total=self.request_timeout,
            connect=60,
            sock_read=300
        )
        
        self.total_timeout_config = ClientTimeout(
            total=self.total_timeout,
            connect=60,
            sock_read=300
        )
        
        # XML namespaces for parsing
        self.namespaces = {
            'wfs': 'http://www.opengis.net/wfs/2.0',
            'mat': 'http://data.gov.dk/schemas/matrikel/1',
            'gml': 'http://www.opengis.net/gml/3.2'
        }
        
        # Field mapping from XML to our schema
        self.field_mapping = {
            'BFEnummer': ('bfe_number', int),
            'forretningshaendelse': ('business_event', str),
            'forretningsproces': ('business_process', str),
            'senesteSagLokalId': ('latest_case_id', str),
            'id_lokalId': ('id_local', str),
            'id_namespace': ('id_namespace', str),
            'registreringFra': ('registration_from', lambda x: datetime.fromisoformat(x.replace('Z', '+00:00'))),
            'virkningFra': ('effect_from', lambda x: datetime.fromisoformat(x.replace('Z', '+00:00'))),
            'virkningsaktoer': ('authority', str),
            'arbejderbolig': ('is_worker_housing', lambda x: x.lower() == 'true'),
            'erFaelleslod': ('is_common_lot', lambda x: x.lower() == 'true'),
            'hovedejendomOpdeltIEjerlejligheder': ('has_owner_apartments', lambda x: x.lower() == 'true'),
            'udskiltVej': ('is_separated_road', lambda x: x.lower() == 'true'),
            'landbrugsnotering': ('agricultural_notation', str)
        }
        
        # State tracking
        self.is_sync_complete = False
        
        # GCS setup in production environment
        self.environment = os.getenv('ENVIRONMENT', 'dev')
        if self.environment == 'prod':
            bucket_name = os.getenv('GCS_BUCKET_NAME')
            if not bucket_name:
                raise ValueError("Missing GCS_BUCKET_NAME in production environment")
            self.storage_client = storage.Client()
            self.bucket = self.storage_client.bucket(bucket_name)
        
    def _get_base_params(self):
        """Get base WFS request parameters without pagination."""
        return {
            'username': self.username,
            'password': self.password,
            'SERVICE': 'WFS',
            'REQUEST': 'GetFeature',
            'VERSION': '2.0.0',
            'TYPENAMES': 'mat:SamletFastEjendom_Gaeldende',
            'SRSNAME': 'EPSG:25832'
        }

    def _get_params(self, start_index=0):
        """Get WFS request parameters with pagination."""
        params = self._get_base_params()
        params.update({
            'startIndex': str(start_index),
            'count': str(self.page_size)
        })
        return params

    def _parse_geometry(self, geom_elem):
        """Parse GML geometry to WKT."""
        try:
            pos_lists = geom_elem.findall('.//gml:posList', self.namespaces)
            if not pos_lists:
                return None

            polygons = []
            for pos_list in pos_lists:
                if not pos_list.text:
                    continue

                coords = [float(x) for x in pos_list.text.strip().split()]
                # Keep the original 3D coordinate handling - take x,y and skip z
                pairs = [(coords[i], coords[i+1]) 
                        for i in range(0, len(coords), 3)]

                if len(pairs) < 4:
                    logger.warning(f"Not enough coordinate pairs ({len(pairs)}) to form a polygon")
                    continue

                try:
                    # Check if the polygon is closed (first point equals last point)
                    if pairs[0] != pairs[-1]:
                        pairs.append(pairs[0])  # Close the polygon
                    
                    polygon = Polygon(pairs)
                    if polygon.is_valid:
                        polygons.append(polygon)
                    else:
                        # Try to fix invalid polygon
                        fixed_polygon = make_valid(polygon)
                        if fixed_polygon.geom_type in ('Polygon', 'MultiPolygon'):
                            polygons.append(fixed_polygon)
                        else:
                            logger.warning(f"Could not create valid polygon, got {fixed_polygon.geom_type}")
                except Exception as e:
                    logger.warning(f"Error creating polygon: {str(e)}")
                    continue

            if not polygons:
                return None

            if len(polygons) == 1:
                final_geom = polygons[0]
            else:
                try:
                    final_geom = MultiPolygon(polygons)
                except Exception as e:
                    logger.warning(f"Error creating MultiPolygon: {str(e)}, falling back to first valid polygon")
                    final_geom = polygons[0]

            return wkt.dumps(final_geom)

        except Exception as e:
            logger.error(f"Error parsing geometry: {str(e)}")
            return None

    def _parse_feature(self, feature_elem):
        """Parse a single feature."""
        try:
            feature = {}
            
            # Add validation of the feature element
            if feature_elem is None:
                logger.warning("Received None feature element")
                return None
            
            # Parse all mapped fields
            for xml_field, (db_field, converter) in self.field_mapping.items():
                elem = feature_elem.find(f'.//mat:{xml_field}', self.namespaces)
                if elem is not None and elem.text:
                    try:
                        value = clean_value(elem.text)
                        if value is not None:
                            feature[db_field] = converter(value)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Error converting field {xml_field}: {str(e)}")
                        continue

            # Parse geometry
            geom_elem = feature_elem.find('.//mat:geometri/gml:MultiSurface', self.namespaces)
            if geom_elem is not None:
                geometry_wkt = self._parse_geometry(geom_elem)
                if geometry_wkt:
                    feature['geometry'] = geometry_wkt
                else:
                    logger.warning("Failed to parse geometry for feature")

            # Add validation of required fields
            if not feature.get('bfe_number'):
                logger.warning("Missing required field: bfe_number")
            if not feature.get('geometry'):
                logger.warning("Missing required field: geometry")

            return feature if feature.get('bfe_number') and feature.get('geometry') else None

        except Exception as e:
            logger.error(f"Error parsing feature: {str(e)}")
            return None

    async def _get_total_count(self, session):
        """Get total number of features from first page metadata."""
        params = self._get_base_params()
        params.update({
            'startIndex': '0',
            'count': '1'  # Just get one feature to check metadata
        })
        
        try:
            logger.info("Getting total count from first page metadata...")
            async with session.get(self.url, params=params) as response:
                response.raise_for_status()
                text = await response.text()
                root = ET.fromstring(text)
                
                # Handle case where numberMatched might be '*'
                number_matched = root.get('numberMatched', '0')
                number_returned = root.get('numberReturned', '0')
                
                logger.info(f"WFS response metadata - numberMatched: {number_matched}, numberReturned: {number_returned}")
                
                if number_matched == '*':
                    # If server doesn't provide exact count, fetch a larger page to estimate
                    logger.warning("Server returned '*' for numberMatched, fetching sample to estimate...")
                    params['count'] = '1000'
                    async with session.get(self.url, params=params) as sample_response:
                        sample_text = await sample_response.text()
                        sample_root = ET.fromstring(sample_text)
                        feature_count = len(sample_root.findall('.//mat:SamletFastEjendom_Gaeldende', self.namespaces))
                        # Estimate conservatively
                        return feature_count * 2000  # Adjust multiplier based on expected data size
                
                if not number_matched.isdigit():
                    raise ValueError(f"Invalid numberMatched value: {number_matched}")
                    
                total_available = int(number_matched)
                
                # Add sanity check for unreasonable numbers
                if total_available > 5000000:  # Adjust threshold as needed
                    logger.warning(f"Unusually high feature count: {total_available:,}. This may indicate an issue.")
                    
                return total_available
                
        except Exception as e:
            logger.error(f"Error getting total count: {str(e)}")
            raise

    async def _wait_for_rate_limit(self):
        """Ensure we don't exceed requests_per_second."""
        worker_id = id(asyncio.current_task())
        if worker_id in self.last_request_time:
            elapsed = time.time() - self.last_request_time[worker_id]
            if elapsed < 1.0 / self.requests_per_second:
                await asyncio.sleep(1.0 / self.requests_per_second - elapsed)
        self.last_request_time[worker_id] = time.time()

    @backoff.on_exception(
        backoff.expo,
        (ClientError, asyncio.TimeoutError),
        max_tries=3,
        max_time=60
    )
    async def _fetch_chunk(self, session, start_index, timeout=None):
        """Fetch a chunk of features with rate limiting and retries."""
        async with self.request_semaphore:
            await self._wait_for_rate_limit()
            
            params = self._get_params(start_index)
            
            try:
                logger.info(f"Fetching chunk at index {start_index}")
                async with session.get(
                    self.url, 
                    params=params,
                    timeout=timeout or self.request_timeout_config
                ) as response:
                    if response.status == 429:  # Too Many Requests
                        retry_after = int(response.headers.get('Retry-After', 5))
                        logger.warning(f"Rate limited, waiting {retry_after} seconds")
                        await asyncio.sleep(retry_after)
                        raise ClientError("Rate limited")
                    
                    response.raise_for_status()
                    content = await response.text()
                    root = ET.fromstring(content)
                    
                    # Add validation of returned features count
                    number_returned = root.get('numberReturned', '0')
                    logger.info(f"WFS reports {number_returned} features returned in this chunk")
                    
                    features = []
                    feature_elements = root.findall('.//mat:SamletFastEjendom_Gaeldende', self.namespaces)
                    logger.info(f"Found {len(feature_elements)} feature elements in XML")
                    
                    for feature_elem in feature_elements:
                        feature = self._parse_feature(feature_elem)
                        if feature:
                            features.append(feature)
                    
                    valid_count = len(features)
                    logger.info(f"Chunk {start_index}: parsed {valid_count} valid features out of {len(feature_elements)} elements")
                    
                    # Validate that we're getting reasonable numbers
                    if valid_count == 0 and len(feature_elements) > 0:
                        logger.warning(f"No valid features parsed from {len(feature_elements)} elements - possible parsing issue")
                    elif valid_count < len(feature_elements) * 0.5:  # If we're losing more than 50% of features
                        logger.warning(f"Low feature parsing success rate: {valid_count}/{len(feature_elements)}")
                    
                    return features
                    
            except Exception as e:
                logger.error(f"Error fetching chunk at index {start_index}: {str(e)}")
                raise

    def save_to_parquet(self, features, output_path):
        """Save features to a local Parquet file."""
        if not features:
            logger.warning("No features to save")
            return
            
        try:
            # Create DataFrame from WKT features
            df = pd.DataFrame([{k:v for k,v in f.items() if k != 'geometry'} for f in features])
            geometries = [wkt.loads(f['geometry']) for f in features]
            gdf = gpd.GeoDataFrame(df, geometry=geometries, crs="EPSG:25832")
            
            # Save as GeoParquet
            gdf.to_parquet(output_path)
            logger.info(f"Saved {len(features)} features to {output_path}")
            
            # Save metadata
            metadata = {
                "source": "Datafordeleren WFS",
                "source_url": self.url,
                "timestamp": datetime.now().isoformat(),
                "record_count": len(features),
                "crs": "EPSG:25832"
            }
            
            metadata_path = output_path.with_suffix('.json')
            with open(metadata_path, 'w') as f:
                import json
                json.dump(metadata, f, indent=2)
                
        except Exception as e:
            logger.error(f"Error saving to Parquet: {str(e)}")
            raise

    def upload_to_gcs(self, local_path, gcs_path):
        """Upload file to Google Cloud Storage in production environment."""
        if self.environment != 'prod':
            return
            
        try:
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)
            logger.info(f"Uploaded {local_path} to gs://{self.bucket.name}/{gcs_path}")
        except Exception as e:
            logger.error(f"Error uploading to GCS: {str(e)}")
            raise

    async def run(self):
        """Run the cadastral bronze data pipeline."""
        logger.info("Starting cadastral parcel bronze pipeline...")
        
        try:
            # Create output directory with timestamp
            bronze_dir = self.output_dir / self.timestamp
            bronze_dir.mkdir(parents=True, exist_ok=True)
            
            async with aiohttp.ClientSession(timeout=self.total_timeout_config) as session:
                total_features = await self._get_total_count(session)
                logger.info(f"Found {total_features:,} total features")
                
                # Apply limit if specified (for testing)
                if self.args.get('limit'):
                    total_features = min(total_features, self.args['limit'])
                    logger.info(f"Limiting to {total_features:,} features")
                
                features_batch = []
                total_processed = 0
                failed_chunks = []
                
                # Create working file path
                working_file = bronze_dir / "cadastral_parcels_working.parquet"
                
                for start_index in range(0, total_features, self.page_size):
                    try:
                        # Check if we've reached the limit
                        if self.args.get('limit') and total_processed >= self.args['limit']:
                            logger.info(f"Reached limit of {self.args['limit']} features")
                            break
                            
                        chunk = await self._fetch_chunk(session, start_index)
                        if chunk:
                            features_batch.extend(chunk)
                            total_processed += len(chunk)
                            
                            # Log progress periodically
                            if total_processed % 10000 == 0 or len(features_batch) >= self.batch_size:
                                logger.info(f"Progress: {total_processed:,}/{total_features:,} features ({(total_processed/total_features)*100:.1f}%)")
                            
                            # Write batch if it's large enough or it's the last batch
                            is_last_batch = (start_index + self.page_size) >= total_features
                            if len(features_batch) >= self.batch_size or is_last_batch:
                                logger.info(f"Writing batch of {len(features_batch):,} features (is_last_batch: {is_last_batch})")
                                
                                # Save to local Parquet file
                                batch_file = bronze_dir / f"cadastral_parcels_batch_{total_processed}.parquet"
                                self.save_to_parquet(features_batch, batch_file)
                                
                                # In production, upload to GCS
                                if self.environment == 'prod':
                                    gcs_path = f"raw/cadastral/{self.timestamp}/batch_{total_processed}.parquet"
                                    self.upload_to_gcs(batch_file, gcs_path)
                                
                                features_batch = []
                                
                    except Exception as e:
                        logger.error(f"Error processing batch at {start_index}: {str(e)}")
                        failed_chunks.append(start_index)
                        continue
                
                # Create final output file by merging batches
                if total_processed > 0:
                    logger.info(f"Creating final output file...")
                    final_file = bronze_dir / "cadastral_parcels.parquet"
                    
                    # List all batch files
                    batch_files = list(bronze_dir.glob("cadastral_parcels_batch_*.parquet"))
                    
                    if batch_files:
                        # Merge using DuckDB for efficiency
                        try:
                            con = duckdb.connect()
                            # Create a query to union all batch files
                            query = f"COPY (SELECT * FROM read_parquet({', '.join([repr(str(f)) for f in batch_files])}) TO '{final_file}' (FORMAT 'parquet')"
                            con.execute(query)
                            logger.info(f"Created final file with {total_processed:,} features")
                            
                            # In production, upload to GCS
                            if self.environment == 'prod':
                                gcs_path = f"raw/cadastral/{self.timestamp}/cadastral_parcels.parquet"
                                self.upload_to_gcs(final_file, gcs_path)
                                
                                # Also upload as current.parquet
                                current_gcs_path = f"raw/cadastral/current.parquet"
                                self.upload_to_gcs(final_file, current_gcs_path)
                        except Exception as e:
                            logger.error(f"Error creating final file: {str(e)}")
                    else:
                        logger.warning("No batch files found to merge")
                
                if failed_chunks:
                    logger.warning(f"Failed to process chunks starting at indices: {failed_chunks}")
                
                logger.info(f"Bronze pipeline completed. Total processed: {total_processed:,} features")
                return total_processed, bronze_dir
                
        except Exception as e:
            logger.error(f"Error in bronze pipeline: {str(e)}")
            raise

async def main_async():
    """Run the cadastral parcels pipeline asynchronously."""
    args = parse_args()
    setup_logging(args['log_level'])
    
    try:
        fetcher = CadastralFetcher(args)
        total_processed, bronze_dir = await fetcher.run()
        
        # Run silver processing if requested
        if args['run_silver'] and total_processed > 0:
            logger.info("Starting silver layer processing...")
            # Import silver module only if needed
            from silver.transform import process_cadastral_data
            process_cadastral_data(bronze_dir)
            logger.info("Silver layer processing completed")
            
        return 0
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return 1

def main():
    """Main entry point."""
    try:
        exit_code = asyncio.run(main_async())
        return exit_code
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        return 130  # Standard exit code for SIGINT
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    exit(main())