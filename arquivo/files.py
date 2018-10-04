import os
import re
import multiprocessing

from fila.testingfila import * 

def ler_arquivo(path):
    lines = False
    try:
        a = open(path)
        if a:
            lines = a.readlines()
            return lines
    except Exception as e:
        print (e)

def csv_parser_publisher(lines, table):
    #adicionar ao final do dicionario o nome da tabela
    header = lines[0]
    header = header.split(',')
    for linhas in range(1,len(lines)):

        items = (lines[linhas].split(','))
        last_value = items[len(items)-1].strip()
        items[len(items)-1] = last_value
        dict_new = {}
        for item, head in zip(items, header):
            dict_new[head] = item
        #adiciona nome da tabela baseado na pasta
        dict_new['table_name'] = table
        print (dict_new)
        p = multiprocessing.Process(target=publisher_multiprocess, args=(dict_new,))
        p.start()
    return True

def publisher_multiprocess(dict_new):
    print (dict_new)
    publish_on_queue(str(dict_new))
    return True
