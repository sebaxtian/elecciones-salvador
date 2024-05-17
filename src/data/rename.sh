#!/bin/bash

#
# Script to rename files name using regular expresion
#
# Unofficial Documentation: https://stackoverflow.com/questions/76514831/how-to-change-files-name-under-linux-regular-expression
#                           https://mywiki.wooledge.org/BashFAQ/100#Substituting_part_of_a_string
#
# Date: May 16, 2014
# Author: Sebastian Rios Sabogal - sebaxtianrioss@gmail.com
#
# Run: ./rename.sh <file_extension>
#      ./rename.sh .csv
#

FILE_EXTENSION=$1

echo "Rename files name, with $FILE_EXTENSION extension ..."

if [ -z "$FILE_EXTENSION" ]
then
  echo "File extension required"
else
  for name in *:*$FILE_EXTENSION; do
    # Target Files Name: marzo_2024-03-07T19:31:28.884+00:00.csv
    # Rename to: marzo_2024-03-07T19-31-28-884-00-00.csv
    newname=${name//:/-}
    newname=${newname//+/-}
    newname=${newname/./-}
    mv -v -n -- "$name" "$newname"
  done
fi

echo "END"
