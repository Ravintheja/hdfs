#!/usr/bin/env python
"""mapper.py"""
#Author -> Ravin.H

import sys
import csv

# with open('/resources/test.csv') as csv_file:
#     csv_reader = csv.reader(csv_file, delimiter=',')
for line in sys.stdin:
    #for row in csv_reader:
# remove leading and trailing whitespace
    line = line.strip()

    # split the line into words
    h_score = line.split(",")[3]
    a_score = line.split(",")[4]

    #Match Count
    if h_score == a_score:
        outcome = "draw"
    
    else:
        outcome = "result"

    #For City Count
    #outcome = line.split(",")[6]

    print '%s\t%s' % (outcome, 1)
