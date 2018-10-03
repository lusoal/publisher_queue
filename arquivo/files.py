import os
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

def csv_parser(lines):
    #adicionar ao final do dicionario o nome da tabela
    final_list = []
    #remover quebra de linha do codigo
    for i in lines:
        final_list.append(i.strip())
    #pegar cabecalho dinamicamente no codigo
    header = final_list[0]
    header = header.split(',')
    dict_list = []

    #verificar possibilidade de fazer o push para a fila nesse momento
    for linhas in range(1,len(final_list)):
        items = (final_list[linhas].split(','))
        dict_new = {}
        for item, head in zip(items, header):
            dict_new[head] = item
        #realizar o push para a fila nesse momento do codigo
        dict_list.append(dict_new)
    return dict_list