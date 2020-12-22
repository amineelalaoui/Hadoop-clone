# -*- coding: utf-8 -*-

import sys
import json
import argparse

## Parametre
parser = argparse.ArgumentParser("Installation d'Hidoop")
parser.add_argument('--path', help="Chemin jusqu'à la racine de Hidoop")
arguments = parser.parse_args()
path = arguments.path

# Generation du fichier Path.java
sys.stdout.write('\rGenerate ./src/config/Path.java...')
config = open('config.json', 'r')
config = json.load(config)
hidoop_config = config.get("hidoop")
hdfs_config = config.get("hdfs")
with open("./src/config/Settings.java", 'w') as file:
    file.write("/* Automatically generated */\n")
    file.write("package config;\n")
    file.write("\n")
    file.write("public class Settings {\n")
    file.write("\n")
    file.write('\tpublic static final String HIDOOP_PATH = "{}/";\n'.format(path))
    file.write('\tpublic static final String TMP_PATH = "/tmp/{}/";\n'.format(config.get("tmp_dir")))
    file.write('\tpublic static final String DATA_PATH = {};\n'.format('HIDOOP_PATH+"data/"' if config.get('data_path') is '' else '"' +config.get('data_path') + '"'))
    file.write('\tpublic static final String BIN_PATH = HIDOOP_PATH+"bin/";\n')
    file.write('\n')
    file.write('\tpublic static final String MOTIF = "{}";\n'.format(config.get('result_motif')))
    file.write('\n')
    # Hidoop settings
    file.write("\tpublic static final int HIDOOP_PORT = {};\n".format(hidoop_config.get("port")))
    file.write('\tpublic static final String HIDOOP_HOST = "{}";\n'.format(hidoop_config.get("host")))
    file.write('\tpublic static final String HIDOOP_DAEMON_NAME = "{}";\n'.format(hidoop_config.get("daemon_name")))
    file.write('\n')
    # HDFS settings
    file.write("\tpublic static final int HDFS_PORT = {};\n".format(hdfs_config.get("port")))
    file.write('\tpublic static final String HDFS_HOST = "{}";\n'.format(hdfs_config.get("host")))
    file.write('\tpublic static final String HDFS_NAME = "{}";\n'.format(hdfs_config.get("name")))
    file.write('\tpublic static final long FRAGMENT_SIZE = {};\n'.format(hdfs_config.get("fragment_size")))
    file.write("\n")
    # 
    file.write('\tpublic static final boolean IHM = {};\n'.format("true" if config.get('ihm') else "false"))
    file.write("\tpublic static final boolean DEBUG = {};\n".format("true" if config.get('debug') else "false"))
    file.write("\n")
    file.write("}")
sys.stdout.write('\rGenerate ./src/config/Settings.java... done\n')