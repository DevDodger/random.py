"""
Automatic audio transcription job using AWS Transcribe service https://aws.amazon.com/transcribe/
@author yohanes.gultom@gmail.com
"""

import configparser, boto3, os, time, json
from pprint import pprint

bucket_name = 'yohanesgultom-transcribe-test'
file_path = '/home/yohanesgultom/Downloads/Pidato-Kenegaraan-Presiden-Joko-Widodo-2019-Part-1.mp3'
# source: Pidato Kenegaraan Presiden Joko Widodo (2:21-3:42) https://www.youtube.com/watch?v=yDdQ9pEfcnw&t=155s

config = configparser.ConfigParser()        
config.read(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'aws.conf'))

# init AWS session
session = boto3.session.Session(
    aws_access_key_id=config['default']['aws_access_key_id'], 
    aws_secret_access_key=config['default']['aws_secret_access_key'],
    region_name=config['default']['region']
)
s3 = session.client('s3')
transcribe = session.client('transcribe')

# create bucket to store transcribe input/output file if not exists
res = s3.list_buckets()
buckets = [b['Name'] for b in res['Buckets']]
if bucket_name not in buckets:
    print(f'Creating new bucket: {bucket_name}...')
    res = s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={'LocationConstraint': session.region_name}
    )

# upload audio input file if not exist
file_name = os.path.basename(file_path)
res = s3.list_objects(Bucket=bucket_name)
contents = res['Contents'] if 'Contents' in res else []
file_names = [c['Key'] for c in contents]
if file_name not in file_names:
    print(f'Uploading input file: {file_name}...')
    res = s3.upload_file(file_path, bucket_name, file_name)

# create new job if not exist
res = transcribe.list_transcription_jobs()
job_name = file_name
jobs = [j['TranscriptionJobName'] for j in res['TranscriptionJobSummaries']]
if job_name not in jobs:
    print(f'Starting transcribe job: {job_name}...')
    s3_file = f's3://{bucket_name}/{file_name}'
    res = transcribe.start_transcription_job(
        TranscriptionJobName=job_name, 
        LanguageCode='id-ID', 
        Media={'MediaFileUri': s3_file}, 
        OutputBucketName=bucket_name
    )

# wait until job to complete
completed = False
while not completed:
    res = transcribe.list_transcription_jobs(
        JobNameContains=job_name, 
        MaxResults=1
    )  
    if 'TranscriptionJobSummaries' in res:
        if len(res['TranscriptionJobSummaries']) > 0:
            job = res['TranscriptionJobSummaries'][0]
            completed = job['TranscriptionJobStatus'] == 'COMPLETED'
            print(f'Job has completed')
    if not completed:
        print(f'Waiting for job to complete...')
        time.sleep(5)

# download transcription result        
result_file = f'{file_name}.json'
if completed and not os.path.isfile(result_file):
    res = s3.list_objects(Bucket=bucket_name)
    contents = res['Contents'] if 'Contents' in res else []
    for c in contents:
        content_name = c['Key']
        if content_name == result_file:
            print(f'Downloading transcription result...')
            s3.download_file(bucket_name, content_name, content_name)
            print(f'File downloaded {content_name}')

# print transcription result
if os.path.isfile(result_file):
    with open(result_file, 'r') as f:
        res_file = json.load(f)
        print(res_file['results']['transcripts'][0]['transcript'])
