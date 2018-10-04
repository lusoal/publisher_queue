import os
import re

from fila.testingfila import * 

def ler_arquivo(path):
    #ira receber como parametro o nome da pasta de onde ele veio (baseado no nome da pasta ira fazer o upload na tabela)
    lines = False
    try:
        a = open(path)
        if a:
            lines = a.readlines()
            os.remove(path)
            return lines
    except Exception as e:
        print (e)

def csv_parser_publisher(lines):
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
        publish_on_queue(str(dict_new))
    return True

# def csv_parser_publisher(lines):
#     #adicionar ao final do dicionario o nome da tabela
#     final_list = []
#     #remover quebra de linha do codigo
#     for i in lines:
#         final_list.append(i.strip())
#     #pegar cabecalho dinamicamente no codigo
#     header = final_list[0]
#     header = header.split(',')

#     for linhas in range(1,len(final_list)):
#         print (final_list[linhas])
#         items = (final_list[linhas].split(','))
#         dict_new = {}
#         for item, head in zip(items, header):
#             dict_new[head] = item
#         publish_on_queue(str(dict_new))
#     return True