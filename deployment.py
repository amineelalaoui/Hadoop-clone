# -*- coding: utf-8 -*-

import os
import json
import time
import argparse
from time import sleep
import random
config = open('config.json', 'r')
config = json.load(config)

## Parametre
parser = argparse.ArgumentParser("Deployment de HDFS et d'Hidoop")
parser.add_argument('--n', default=3, type=int, help="Nombre de machines a utiliser")
parser.add_argument('--path', help="Chemin jusqu'a la racine de Hidoop")
arguments = parser.parse_args()
n = arguments.n
path = arguments.path

hdfs = config.get("hdfs")
hdfs_host = hdfs.get("host")
hdfs_port = hdfs.get("port")

hosts = config.get("hosts")
try:
    hosts.remove(hdfs_host)
    hosts.remove(config.get("hidoop").get("host"))
except:
    None
assert len(hosts) >= n ,"Il n'y a pas assez d'hosts de disponible"
hosts = random.sample(hosts, n)

data_path = "{}/data/".format(path) if config.get('data_path') is "" else config.get('data_path')
if not config.get('local'):
    # Lancement du NameNode 
    print('Lancement du NameNode sur {}...'.format(hdfs_host))
    os.system("ssh {} killall java".format(hdfs_host))
    os.system("ssh {} java -classpath {}/bin/ hdfs.NodeNameImpl &".format(hdfs_host, path))
    sleep(1)
    print('Lancement du NameNode sur {}... done.\n'.format(hdfs_host))

    # Lancement des Daemon utilise par HDFS et Hidoop sur les hosts distants
    for host in hosts:
        print('Lancement des Daemon sur {}...'.format(host))
        os.system("ssh {} mkdir -p /tmp/{}".format(host, config.get("tmp_dir")))
        os.system("ssh {} killall java".format(host))
        os.system("ssh {} java -classpath {}/bin/ ordo.DaemonImpl &".format(host, path))
        sleep(1)
        os.system("ssh {} java -classpath {}/bin/ hdfs.HdfsServer {} {} &".format(host, path, host, hdfs_port))
        sleep(1)
        print('Lancement des Daemon sur {}... done\n'.format(host))

    # Attente du deploiement de tous les Daemons
    sleep(3)

    # HdfsWrite les donnees
    print('Ecriture des donnees via HDFS...')
    ti = time.time()
    for file in os.listdir('{}/data/'.format(path)):
        if not os.path.isdir(file) and file == config.get("file_test_name"):
            tii = time.time()
            os.system("java -classpath {}/bin/ hdfs.HdfsClient write line {}/{}".format(path, data_path, file))
            print("HdfsClient.HdfsWrite {} ({}s)".format(file, round(time.time()-tii,2)))
    print('Ecriture des donnees via HDFS... done ({}s)'.format(round(time.time()-ti,2)))

# Lancer Hidoop IHM
# print("java -classpath {}/bin/ config.{}".format(path, "LocalRun {}/data/{}".format(path, config.get('file_test_name')) if config.get("debug") else "Project"))
os.system("java -classpath {}/bin/ config.{}".format(path, "{} {}/{}".format("LocalRun" if config.get('local') else "Run", data_path, config.get('file_test_name'))))
