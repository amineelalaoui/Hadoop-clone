#!/bin/bash

# argument 1 : n nombre d'hosts (optionel)

if [ $# -eq 0 ]
  then
    echo "Deploiement avec 3 hosts..."
    python deployment.py --n=3 --path=$(pwd)
  else
    echo "Deploiement avec $1 hosts..."
    python deployment.py --n=$1 --path=$(pwd)
fi

