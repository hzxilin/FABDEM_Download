from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile
import requests
from geopandas import GeoDataFrame
import geopandas as gpd
import shapely
import shapely.geometry
from shapely.geometry import box
from tqdm import tqdm
import rasterio, rasterio.merge
from pyproj import CRS
import pandas




def __download_file(url, destination_path, show_progress):
    # First, send a HEAD request to get the total size of the file
    response = requests.head(url)
    total_size = int(response.headers.get('content-length', 0))

    # Stream the download
    response = requests.get(url, stream=True)
    response.raise_for_status()

    # Open the file to write to
    with open(destination_path, "wb") as file:
        if show_progress:
            # Setup the progress bar if requested
            with tqdm(
                desc=destination_path.name,
                total=total_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
            ) as bar:
                for data in response.iter_content(chunk_size=1024):
                    size = file.write(data)
                    bar.update(size)
        else:
            # Download without progress bar
            for data in response.iter_content(chunk_size=1024):
                file.write(data)



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




def download(zips_to_download, output_path, show_progress=True):
    
    # Convert output path to a pathlib.Path object
    output_path = Path(output_path)

    # FABDEM base url
    base_url = "https://data.bris.ac.uk/datasets/s5hqmjcdj8yo2ibzi9b4ew3sn"

    # Download tiles
    download_folder = output_path
    for zipfile_name in zips_to_download:
        # Fetch a tile
        tile_url = f"{base_url}/{zipfile_name}"

        zip_path = Path(download_folder) / zipfile_name
        if not zip_path.exists():
            __download_file(tile_url, zip_path, show_progress)
        elif zip_path.exists() and show_progress:
            print(f"{zip_path} loaded from cache")

        # Unzip its contents
        with ZipFile(zip_path, 'r') as zip_archive:
            zip_archive.extractall(download_folder)


############################################################
############################################################

# Prepare City Boundary Data
city_gpkg = "/Users/xilin/Dropbox/Sealevel-Infrastructure/Data/Input/boundary_10.gpkg"
cities_gdf = gpd.read_file(city_gpkg) 
cities_gdf['centroid'] = cities_gdf.geometry.centroid
cities_gdf['x'] = cities_gdf['centroid'].x.astype(int)
cities_gdf['y'] = cities_gdf['centroid'].y.astype(int)
cities_gdf['bounds'] = cities_gdf.apply(
    lambda row: box(row['x'] - 1, row['y'] - 1, row['x'] + 1, row['y'] + 1),
    axis=1
)

# Download tiles Shapefile to prepare for extraction 
base_url = "https://data.bris.ac.uk/datasets/s5hqmjcdj8yo2ibzi9b4ew3sn"
tiles_info_url = f"{base_url}/FABDEM_v1-2_tiles.geojson"
response = requests.get(tiles_info_url, stream=True)
response.raise_for_status()    
tiles_gdf = GeoDataFrame.from_features(
    response.json()["features"],
    crs=4326
)

# Determine which zips to download
zips_to_download, cities_gdf_file = get_intersect_tile(tiles_gdf, cities_gdf)
cities_gdf_file = cities_gdf_file.drop(columns=["centroid","bounds"])
cities_gdf_file.to_file("/Users/xilin/Dropbox/Sealevel-Infrastructure/Data/Output/boundary_file_10.gpkg", driver="GPKG")


output_path = "/Users/xilin/Dropbox/Sealevel-Infrastructure/Data/Raw/17_FABDEM"
download(zips_to_download, output_path, show_progress=True)