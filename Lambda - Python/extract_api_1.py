import boto3
import httpx
import asyncio
from datetime import datetime
import json

def lambda_handler(event, context):
    
    coords = [["43.833328","4.35"]] # Coordonnées geographiques de Nîmes 
    param_coords = []

    for x in coords :
        param_coords.append({"lat": x[0], 
                    "lon": x[1],  
                    "radius": "50000" })


    async def rate_limiter(queue):
        """Fonction qui contrôle la cadence des requêtes à 10/s."""
        while True:
            await asyncio.sleep(0.1)  # Pause de 100ms entre chaque requête
            await queue.put(None)  # Débloque une requête

    # Fonction générique pour récupérer les données des stations proches de Nîmes par coordonnées et Radius de 50km
    async def fetch_station(endpoint, params,queue):
        headers = {
            "x-rapidapi-key": "8c44331d4dmsh35896668e18eba5p137b7fjsn8ed95e97d509",
            "x-rapidapi-host": "meteostat.p.rapidapi.com"
        }
        await queue.get()
        async with httpx.AsyncClient() as client:
            response = await client.get(endpoint, headers=headers, params=params)
            return response.json()

    # Fonction générique pour récupérer les métadonnées des stations
    async def fetch_data(endpoint, params, queue):
        headers = {
            "x-rapidapi-key": "8c44331d4dmsh35896668e18eba5p137b7fjsn8ed95e97d509",
            "x-rapidapi-host": "meteostat.p.rapidapi.com"
        }
        await queue.get()
        async with httpx.AsyncClient() as client:
            response = await client.get(endpoint, headers=headers, params=params)
            return response.json()

    # Fonction principale pour orchestrer l'appel des données des stations
    async def main():

        station_results = []
        results_0 = []
        station_ids = []

        queue = asyncio.Queue()
        asyncio.create_task(rate_limiter(queue))  # Lancer le régulateur de débit en arrière-plan

        tasks = []
        # Récupérer les données des stations proches des coordonnées de chaque ville declarées
        for coord in param_coords:
            tasks.append(fetch_station("https://meteostat.p.rapidapi.com/stations/nearby", coord, queue))

        # Exécuter toutes les requêtes pour récupérer les stations proches
        station_results = await asyncio.gather(*tasks)
        
        # Récupérer les identifiants des stations depuis les résultats
        station_ids = [result['data'][i]['id'] for result in station_results if 'data' in result and len(result['data']) > 0 for i in range(len(result['data']))]

        # Créer les paramètres pour récupérer les métadonnées et les données mensuelles
        params_meta = [{"id": station_id} for station_id in station_ids]

        # Créer les tâches pour récupérer les données des stations (métadonnées et mensuelles)
        tasks_0 = []
        for param in params_meta :
            tasks_0.extend([fetch_data("https://meteostat.p.rapidapi.com/stations/meta", param, queue)])
        results_0 = await asyncio.gather(*tasks_0)

        return results_0
        

    # Lancer la fonction principale avec asyncio
    results_0 = asyncio.run(main())

    #Enregistrer les metadatas bruts des stations dans un bucket S3 
    client = boto3.client('s3')
    client.put_object(
        Bucket="meteo-project-omar",
        Key="data/raw_data/metadata/metadata_stations.json",
        Body=json.dumps(results_0)
    )