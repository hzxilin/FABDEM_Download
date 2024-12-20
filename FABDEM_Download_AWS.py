import boto3
import requests
from tqdm import tqdm
import zipfile
import io
import os
from pathlib import Path
from geopandas import GeoDataFrame

def get_intersect_tile(tiles_gdf, cities_gdf):
    zips_to_download = set()
    for i in range(len(cities_gdf)):
        rect = cities_gdf['bounds'][i]
        tiles_gdf_temp = tiles_gdf
        tiles_gdf_temp["intersects"] = tiles_gdf_temp.geometry.intersects(rect)
        intersecting_tiles = tiles_gdf[tiles_gdf_temp["intersects"]]
        zips_to_download.update(set(intersecting_tiles.zipfile_name))
        cities_gdf.at[i, 'zip_file_name'] = set(intersecting_tiles.zipfile_name)
        cities_gdf.at[i, 'file_name'] = set(intersecting_tiles.file_name)
    return(zips_to_download, cities_gdf)

def __download_file_to_s3 (url, zipfile_name):
    zip_folder = 'zip_folder/'
    unzipped_folder = 'unzipped_folder/'
    # First, send a HEAD request to get the total size of the file
    response = requests.head(url)
    total_size = int(response.headers.get('content-length', 0))

    # Stream the download
    response = requests.get(url, stream=True)
    response.raise_for_status()

    # Use a buffer to hold the downloaded content
    buffer = io.BytesIO()

    # Setup the progress bar if requested
    with tqdm(
        desc=zipfile_name,
        total=total_size,
        unit='B',
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for data in response.iter_content(chunk_size=1024):
            buffer.write(data)
            bar.update(len(data))
    
    # Define the S3 ZIP file path
    zip_object_name = f"{zip_folder}/{zipfile_name}"

    # Upload to S3
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=bucket_name, Key=zip_object_name, Body=buffer)
    print(f"File successfully uploaded to {bucket_name}/{zip_object_name}")

    with zipfile.ZipFile(buffer) as zip_file:
        for file_info in zip_file.infolist():
            extracted_file = zip_file.open(file_info)
            extracted_path = f"{unzipped_folder}/{file_info.filename}"
            # Upload each extracted file to the S3 unzipped folder
            s3_client.put_object(Bucket=bucket_name, Key=extracted_path, Body=extracted_file.read())
            print(f"Extracted and uploaded {file_info.filename} to s3://{bucket_name}/{extracted_path}")

def download(zips_to_download):   
    # Convert output path to a pathlib.Path object
    # FABDEM base url
    base_url = "https://data.bris.ac.uk/datasets/s5hqmjcdj8yo2ibzi9b4ew3sn"
    # Download tiles
    for zipfile_name in zips_to_download:
        tile_url = f"{base_url}/{zipfile_name}"
        __download_file_to_s3(tile_url, zipfile_name)


# Prepare City Boundary Data
#city_gpkg = "/Users/xilin/Dropbox/Sealevel-Infrastructure/Data/Input/boundary_10.gpkg"
#cities_gdf = gpd.read_file(city_gpkg) 
#cities_gdf['centroid'] = cities_gdf.geometry.centroid
#cities_gdf['x'] = cities_gdf['centroid'].x.astype(int)
#cities_gdf['y'] = cities_gdf['centroid'].y.astype(int)
#cities_gdf['bounds'] = cities_gdf.apply(
#    lambda row: box(row['x'] - 1, row['y'] - 1, row['x'] + 1, row['y'] + 1),
#    axis=1
#)

# Download tiles Shapefile to prepare for extraction 
#base_url = "https://data.bris.ac.uk/datasets/s5hqmjcdj8yo2ibzi9b4ew3sn"
#tiles_info_url = f"{base_url}/FABDEM_v1-2_tiles.geojson"
#response = requests.get(tiles_info_url, stream=True)
#response.raise_for_status()    
#tiles_gdf = GeoDataFrame.from_features(
#    response.json()["features"],
#    crs=4326
#)

# Determine which zips to download
#zips_to_download= get_intersect_tile(tiles_gdf, cities_gdf)

zips_to_download = set(["N10E110-N20E120_FABDEM_V1-2.zip", "N30E140-N40E150_FABDEM_V1-2.zip"])
s3_client = boto3.client('s3')
bucket_name = 'fabdem.download.test'
download(zips_to_download)