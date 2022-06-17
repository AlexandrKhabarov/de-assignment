#!/bin/bash

set -e

poetry build

JOB="entrypoints/signals.py"
INPUT_FILE_PATH="./resources/signals/"
OUTPUT_PATH="./output"

rm -rf $OUTPUT_PATH

poetry run spark-submit \
    --master local \
    --py-files dist/de_assignment-*.whl \
    $JOB \
    $INPUT_FILE_PATH \
    $OUTPUT_PATH