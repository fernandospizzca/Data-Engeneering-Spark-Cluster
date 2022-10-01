# path = '/usr/local/spark/shareTemp/asdsaasas/sasasw'
# indexPath = path.find('shareTemp')

# print(indexPath)
# print(path[indexPath::1])

import urllib
from smb.SMBHandler import SMBHandler
import pandas as pd

# opener = urllib.request.build_opener(SMBHandler)
# src_path = 'shareTemp/teste/NotasTCC.txt'
# fh = opener.open(f'smb://ferhs:master@192.168.56.112/{src_path}')
# print(fh)
# data = fh.read()
# print(data)
# fh.close()

opener = urllib.request.build_opener(SMBHandler)
src_path = 'shareTemp/teste/NotasTCC.txt'
# fh = opener.open(f'smb://ferhs:master@192.168.56.112/shareTemp/Dados-covid-19-estado.csv')
# data =  fh.read()
# df = pd.read_csv(fh.read())

# print(data)

# import smbclient
# smbclient.register_session("192.168.56.112", username="ferhs", password="master")
# teste = smbclient.open_file("//192.168.56.112/shareTemp/Dados-covid-19-estado.csv")
# print(teste)

# fh.close()


# src_path2 = 'shareTemp/teste/NotasTCC.txt'
# fh2 = opener.open(f'smb://ferhs:master@192.168.56.112/{src_path2}')
# data2 = fh2.read()
# print(data2)


import subprocess
subprocess.run(["scp", 'testes.py', "ferhs@worker2:/usr/local/spark2/shareTemp"])

# indexPath = src_path.find('shareTemp')
# src_path = src_path[indexPath::1]
# fh = opener.open(f'smb://ferhs:master@192.168.56.112/{src_path}')
# data = fh.read()
# dfVac = spark.read.csv(data, inferSchema=True, header=True, sep=';')
# fh.close()