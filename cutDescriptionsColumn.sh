#!/bin/bash
cut -d"," -f2 github-big-data.csv | tail -n +2 > descriptions.txt