import os
import boto3
from botocore.client import Config
import datetime
from dotenv import load_dotenv
load_dotenv()

today = (datetime.datetime.today()).strftime("%Y-%m-%d")
prev_date = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
print("Today :" , today)
print ("Cleaning for : " ,prev_date)


def main():

    s3 = boto3.resource('s3',
                    endpoint_url=os.getenv('ENDPOINT_URL'),
                    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                    aws_secret_access_key=os.getenv('AWS_ACCESS_SECRET'),
                    config=Config(signature_version='s3v4'),
                    region_name=os.getenv('REGION_NAME'))

    
    database_backup = s3.Bucket(os.getenv('AWS_BUCKET'))
    all_objects = []
    objects_to_delete = [];
    for folder in database_backup.objects.all():
        all_objects.append(folder.key)

    print("All Objects" , all_objects)
    for storage_object in all_objects:
        path,file = storage_object.split("/")
        todays_file = path + "/" + today
        if file.startswith(prev_date,0,10) and any(item.startswith(todays_file) for item in all_objects):
                print(folder.key , "Is eligible for delete.")
                objects_to_delete.append({'Key' : path + "/" + file})

    print("Eligible Objects for delete" , objects_to_delete)
    if objects_to_delete:
        database_backup.delete_objects(
            Delete={
                'Objects': objects_to_delete
            })



if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("error occurred.",e)