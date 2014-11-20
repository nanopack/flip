#!/bin/bash

FILES=`find $PWD/test -name '*.lua'`
./modules/tape/bin/tape $FILES