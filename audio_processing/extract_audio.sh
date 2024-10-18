#!/bin/bash

cd /data/babyview/
mkdir -p audio

FILES=$(find . -type f | grep "MP4")
for f in $FILES
do
    DIR="$(dirname "$f}")"
    FNAME="audio/${f%.MP4}.mp3"
    if ! [ -f $FNAME ]; then
        mkdir -p audio/$DIR
        ffmpeg -i $f -ar 16000 -vn $FNAME
    fi
done