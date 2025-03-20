import json
import boto3
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
import calendar

def lambda_handler(event, context):

    # Get this_day's date
    this_day = datetime.today()
    week_number = (this_day.day - 1) // 7 + 1
    
    Bucket = "meteo-project-omar"
    s3 = boto3.client('s3')

    # Lists to store station metrics and corresponding file keys
    metric_station = []
    metric_key = []

    # Iterate over the raw files of the current month
    for i in range(1, week_number+1):
        for j in range(1,this_day.day+1):

            Key = "data/raw_data/stations_data/year_" + str(this_day.year) + "/month_" + str(this_day.month) + "/week_" + str(i)+"/day_"+str(j) + "/"
            
            # List objects in the specified S3 folder
            response = s3.list_objects(Bucket=Bucket, Prefix=Key)
            
            # Check if the 'Contents' key exists in the response
            if 'Contents' in response:
                for file in response['Contents']:
                    file_key = file['Key']
                    
                    # If the file is a CSV file, download and read its content
                    if file_key.endswith('.csv'):
                        obj = s3.get_object(Bucket=Bucket, Key=file_key)
                        
                        # Read the content of the CSV file
                        csv_content = obj['Body'].read().decode('utf-8')

                        # Convert CSV content into a pandas DataFrame and appending to the metric_station list
                        df = pd.read_csv(StringIO(csv_content),dtype={'station_id': str})
                        metric_station.append(df)
                        metric_key.append(file_key)

    # Concatenate the dataframe of the month and store them in S3 Bucket
    all_data = pd.concat(metric_station, ignore_index=True) 
    all_data['date'] = pd.to_datetime(all_data['date'])
    all_data['station_id'] = all_data['station_id'].astype(str)

    csv_buffer = StringIO()
    all_data.to_csv(csv_buffer, index=False)

    s3.put_object(
        Bucket=Bucket,
        Key="data/processed_data/stations_data/year_"+str(this_day.year)+"/month_"+str(this_day.month)+"/"+str(this_day.month)+str(this_day.year)+"_station_data.csv",
        Body=csv_buffer.getvalue()
    )
 