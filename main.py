# -*- coding: utf-8 -*-
import yaml

from arquivo.files import *
from aws.awss3 import download_s3_file
from fila.testingfila import * 

def main():
    #carregar configuracoes arquivo yaml
    configs = yaml.load(open('config.yml'))
    file_path = configs['file']['path']
    bucket_name = configs['aws']['bucket']
    
    #file name sera passado como evento na lambda
    file_name = 'newcsv.csv'
    s3_return = download_s3_file(bucket_name, file_name)
    if s3_return:
        lines = ler_arquivo(file_path)
        dict_list = csv_parser(lines)
        for item in dict_list:
            print (item)
            publish_on_queue(str(item))
            #mover aquivo de bucket 1 para bucket 2

if __name__ == "__main__":
    main()