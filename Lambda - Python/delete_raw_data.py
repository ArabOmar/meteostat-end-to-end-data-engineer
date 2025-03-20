import boto3

def lambda_handler(event, context):
    
    s3 = boto3.resource('s3')
    prefix = "data/raw_data/stations_data/"  # Folder path

    # Get the bucket object
    bucket = s3.Bucket("meteo-project-omar")

    # List and delete all objects inside the folder
    bucket.objects.filter(Prefix=prefix).delete()
