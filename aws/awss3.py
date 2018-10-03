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

