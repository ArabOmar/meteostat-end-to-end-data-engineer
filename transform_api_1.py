import json
import boto3
import pandas as pd
from io import StringIO

def lambda_handler(event, context):


    client = boto3.client('s3')
    meta_station = client.get_object(
        Bucket="meteo-project-omar",
        Key="data/raw_data/metadata/metadata_stations.json"
    )

    meta_station = json.loads(meta_station['Body'].read().decode('utf-8'))
    
    station_list=[]

    for station in meta_station:  # Parcours des éléments dans la clé 'data'
        station_id = station['data']['id']
        station_name = station['data']['name']['en']
        station_country = station['data']['country']
        station_latitude = station['data']['location']['latitude']
        station_longitude = station['data']['location']['longitude']
        station_elevation = station['data']['location']['elevation']

        # Créer un dictionnaire avec les informations de la station
        station_element = {
            'station_id': station_id,
            'name': station_name,
            'country': station_country,
            'latitude': station_latitude,
            'longitude': station_longitude,
            'elevation': station_elevation
        }

        # Ajouter l'élément à la liste des stations
        station_list.append(station_element)

    station_df = pd.DataFrame.from_dict(station_list)
    station_df = station_df.drop_duplicates(subset=['station_id'])
    # Convertie la DataFrame en format CSV format en memory
    csv_buffer = StringIO()
    station_df.to_csv(csv_buffer, index=False)
    
    # Insert le fichier dans le bucket S3
    client.put_object(
    Bucket="meteo-project-omar",
    Key="data/processed_data/metadata_stations/station_data.csv",
    Body=csv_buffer.getvalue()
    )

    #Delete raw metadata after the processing
    s3 = boto3.resource('s3')
    prefix = "data/raw_data/metadata/"  # Folder path
    bucket = s3.Bucket("meteo-project-omar")

    # List and delete all objects inside the folder
    bucket.objects.filter(Prefix=prefix).delete()
