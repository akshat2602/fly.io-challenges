#!/bin/bash

# Check if debug flag is passed to script, if yes then append --log-stderr flag to maelstrom command
if [ "$1" == "debug" ]; then
    echo "Running in debug mode"
    debug="--log-stderr"
else
    debug=""
fi


cwd=$(pwd)
go build -o bin
MAELSTROM_PATH=$cwd/../maelstrom
cd $MAELSTROM_PATH
# Pass the debug flag to maelstrom test command
./maelstrom test -w broadcast --bin $cwd/bin --node-count 25 --time-limit 20 --rate 100 --latency 100 $debug
cd $cwd