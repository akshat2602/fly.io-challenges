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
./maelstrom test -w g-counter --bin $cwd/bin --node-count 3 --rate 100 --time-limit 20 --nemesis partition $debug
cd $cwd