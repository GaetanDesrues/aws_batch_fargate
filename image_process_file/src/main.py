import os
from datetime import datetime
from pprint import pprint
import json
import boto3
import pygrib
import geojson
import gatools as ga

from selected_grib_messages import GRIB_MSG


s3_client = boto3.client("s3")

input_bucket_name = "noaa-gfs-bdp-pds"

# Atlantic + Europe
lon_min, lon_max = -85, 35
lat_min, lat_max = -26, 60

# Small
# lon_min, lon_max = -10, 10
# lat_min, lat_max = 40, 50

def compute_features(grbs):
    """Extract selected grib messages to a geo json table"""
    features = {}

    # Iterate over all messages in the GRIB2 file
    for grb in grbs:
        # Get the parameter name
        param_name = grb.name

        # Filter based on the specified parameters
        if param_name not in GRIB_MSG:
            continue

        param = GRIB_MSG[param_name]

        if ["level_type"] != grb.typeOfLevel or param["level"] != int(grb.level):
            continue

        short_name = param["sn"]
        print(f"Found {param_name} ({short_name})")
        f = param.get("convert", lambda x: x)

        # Get the data values and coordinates
        lats, lons = grb.latlons()
        lats, lons = lats[:, 0], lons[0, :] - 180
        values = grb.values

        # Iterate over the coordinates and filter region
        for i, lat in enumerate(lats):
            for j, lon in enumerate(lons):
                if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
                    k_ = f"{i}_{j}"
                    if k_ in features:
                        features[k_]["properties"][
                            short_name
                        ] = f"{f(values[i][j]):.3f}"
                    else:
                        # Create a GeoJSON feature for this coordinate
                        feature = geojson.Feature(
                            geometry=geojson.Point((lon, lat)),
                            properties={short_name: f"{f(values[i][j]):.3f}"},
                        )
                        features[k_] = feature

    return geojson.FeatureCollection(list(features.values()))


def main():
    date = datetime.now().strftime("%Y%m%d")
    job_i = int(os.getenv("AWS_BATCH_JOB_ARRAY_INDEX"))

    s3_uri = os.getenv("INPUT_FILE")
    uri_parts = s3_uri.replace("s3://", "").split("/", 1)
    bucket_name = uri_parts[0]
    s3_key = uri_parts[1]

    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    data = json.loads(response["Body"].read().decode("utf-8"))
    # pprint(data)
    files_batch = data[str(job_i)]

    for fname in files_batch:
        print(f"s3://{input_bucket_name}/{fname}")

        with ga.TmpFile(suffix=".grib") as f:
            s3_client.download_file(input_bucket_name, fname, f.name)
            grbs = pygrib.open(f.name)
        geo_fts = compute_features(grbs)

        new_fname = fname.split("/")[-1].replace(".", "_")
        try:
            response = s3_client.put_object(
                Bucket="awstests-lavkge",
                Key=(s3key := f"lambda_slices/{date}/geojson_{new_fname}.json"),
                Body=geojson.dumps(geo_fts),
                ContentType="application/json",
            )

            print("Données envoyées avec succès à S3:", s3key)
        except Exception as e:
            print(f"Erreur lors de l'envoi des données à S3 : {str(e)}")
            raise


# os.environ["INPUT_FILE"] = "s3://awstests-lavkge/lambda_slices/tasks_20241003.json"
# os.environ["AWS_BATCH_JOB_ARRAY_INDEX"] = "0"
main()
