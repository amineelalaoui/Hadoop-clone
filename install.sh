#!/bin/bash
python install.py --path=$(pwd)

echo "Compilation des sources..."
mkdir -p bin
javac -d bin --class-path bin src/application/*.java src/config/*.java src/formats/*.java src/hdfs/*.java src/map/*.java src/ordo/*.java src/ihm/*.java
echo "\rCompilation des sources... done"
