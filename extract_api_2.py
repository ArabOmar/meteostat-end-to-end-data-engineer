import httpx
import asyncio
import boto3
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
import json


def lambda_handler(event, context):

    # On obtient la liste des stations proches de Nîmes à partir des metadata cleans
    client = boto3.client('s3')
    meta_station = client.get_object(
        Bucket="meteo-project-omar",
        Key="data/processed_data/metadata_stations/station_data.csv"
        )

    meta_station = pd.read_csv(StringIO(meta_station['Body'].read().decode('utf-8')),dtype={'station_id': str})
    station_ids = meta_station['station_id'].tolist()
    
    async def rate_limiter(queue):
    #Fonction qui contrôle la cadence des requêtes à 10/s.
        while True:
            await asyncio.sleep(0.1)  # Pause de 100ms entre chaque requête
            await queue.put(None)  # Débloque une requête

    # Get the current date of the day 1 week ago
    this_day = datetime.today().strftime("%Y-%m-%d")
    

    # Fonction générique pour récupérer les données mensuelles des stations
    async def fetch_data_hourly(endpoint, params, queue, station_id):
        headers = {
            "x-rapidapi-key": "8c44331d4dmsh35896668e18eba5p137b7fjsn8ed95e97d509",
            "x-rapidapi-host": "meteostat.p.rapidapi.com"
        }

        await queue.get()
        async with httpx.AsyncClient() as client:
            response = await client.get(endpoint, headers=headers, params=params)
            return { "station_id": station_id, "data": response.json() }   # Ici je return aussi la station_id associé pour ne pas perdre son association avec les données reçues simultanément. 
            
            



    # Fonction principale pour orchestrer l'appel des données des stations et des données quotidiennes
    async def main():

        results_1 = []
        queue = asyncio.Queue()
        asyncio.create_task(rate_limiter(queue))  # Lancer le régulateur de débit en arrière-plan
        
        # Créer les paramètres pour récupérer les métadonnées et les données mensuelles

        params_hourly = [{"station": station_id, "start": this_day, "end": this_day} for station_id in station_ids]

        # Créer les tâches pour récupérer les données des stations (métadonnées et mensuelles)

        tasks_1 = []
    
        for i, param in enumerate(params_hourly):
            tasks_1.append(fetch_data_hourly("https://meteostat.p.rapidapi.com/stations/hourly", param, queue, station_ids[i]))
        results_1 = await asyncio.gather(*tasks_1)

        
        return(results_1)

    results_1 = asyncio.run(main())
    this_day = datetime.today()
    week_number = (this_day.day - 1) // 7 + 1
    
    print((results_1))
    df_1= json.loads(json.dumps(results_1))
    metric_list=[]

    for station in df_1:
        station_id = station['station_id']
        
        # Vérifier si 'data' existe dans l'objet de la station
        if 'data' in station:
            # Parcourir chaque élément dans 'data' pour récupérer les métriques
            for data_entry in station['data']['data']:
                date_item = data_entry['time']  # Extraction de la date
                temp_item = data_entry['temp']
                prcp_item = data_entry['prcp']
                wspd_item = data_entry['wspd']
                
                # Créer un dictionnaire avec les métriques
                metric_element = {
                    'station_id': station_id,
                    'date': date_item,
                    'temp': temp_item,
                    'prcp_mm': prcp_item,
                    'wind_speed': wspd_item
                }
                # Ajouter l'élément à la liste metric_list
                metric_list.append(metric_element)

    metric_df = pd.DataFrame.from_dict(metric_list)
    csv_buffer = StringIO()
    metric_df.to_csv(csv_buffer, index=False)
        #Enregistrer les metadatas bruts des stations dans un bucket S3 
    client = boto3.client('s3')
    client.put_object(
        Bucket="meteo-project-omar",
        Key="data/raw_data/stations_data/year_"+str(this_day.year)+"/month_"+str(this_day.month) +"/week_"+str(week_number)+"/day_"+str(this_day.day)+"/data_"+str(datetime.now()) +".csv",
        Body=csv_buffer.getvalue()
    )

