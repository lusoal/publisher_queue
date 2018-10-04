# -*- coding: utf-8 -*-
import yaml

from arquivo.files import *
from aws.awss3 import *
from fila.testingfila import * 

def main():
    #carregar configuracoes arquivo yaml
    configs = yaml.load(open('config.yml'))
    file_path = configs['file']['path']
    bucket_name = configs['aws']['bucket']
    bucket_processed = configs['aws']['bucket_dest']
    
    #exemplo de evento
    event = {'Records': [{'eventVersion': '2.0', 'eventSource': 'aws:s3', 'awsRegion': 'us-east-1', 'eventTime': '2018-10-04T20:23:07.559Z', 'eventName': 'ObjectCreated:Put', 'userIdentity': {'principalId': 'A1P17X70ZF03J0'}, 'requestParameters': {'sourceIPAddress': '179.191.118.22'}, 'responseElements': {'x-amz-request-id': 'D14932A56A23EDFC', 'x-amz-id-2': 'DNYl9FHyholAgbBsvjNUCeBjgcIFfntKmSdkSgF0BkWRRoctiW0dSM4twAxhS9aTg+B7Er9NlIs='}, 's3': {'s3SchemaVersion': '1.0', 'configurationId': '80da5261-151f-4e8a-9c7d-a8d6b4a9320e', 'bucket': {'name': 'bucket-projeto-integrado-usjt', 'ownerIdentity': {'principalId': 'A1P17X70ZF03J0'}, 'arn': 'arn:aws:s3:::bucket-projeto-integrado-usjt'}, 'object': {'key': 'clientes/meuarquivo.csv', 'size': 4123652, 'eTag': '121739544ae9764eccd5e92088a06aeb', 'sequencer': '005BB676AB524984F4'}}}]}

    if '/' in event['Records'][0]['s3']['object']['key']:
        file_name = ((event['Records'][0]['s3']['object']['key']).split('/'))[1]
        folder_name = ((event['Records'][0]['s3']['object']['key']).split('/'))[0]
    else:
        file_name = event['Records'][0]['s3']['object']['key']
        folder_name = "raiz_s3"

    s3_return = download_s3_file(bucket_name, event['Records'][0]['s3']['object']['key'])
    if s3_return:
        lines = ler_arquivo(file_path)
        dict_list = csv_parser_publisher(lines, folder_name)
        
        upload_file_s3(bucket_processed, file_name, 'newcsv.csv')
        remove_from_s3(bucket_name, event['Records'][0]['s3']['object']['key'])
        os.remove('newcsv.csv')
        #mover do bucket 1 para o bucket 2

if __name__ == "__main__":
    main()