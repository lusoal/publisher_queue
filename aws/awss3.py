import boto3

s3 = boto3.resource('s3')

def download_s3_file(bucket_s3, bucket_file):
    retorno = True
    if 'csv' in str(bucket_file):
        #download object from s3
        try:
            s3.meta.client.download_file(bucket_s3, bucket_file, 'newcsv.csv')
        except Exception as e:
            retorno = False
            print (e)
    return retorno

def remove_from_s3(bucket_s3, bucket_file):
    client = boto3.client('s3')
    response = client.delete_object(Bucket=bucket_s3, Key=bucket_file)

def upload_file_s3(bucket_s3, bucket_file, my_file):
    if '/' in bucket_file:
        bucket_file = (bucket_file.split('/'))[1]

    s3.meta.client.upload_file(my_file, bucket_s3, bucket_file)
