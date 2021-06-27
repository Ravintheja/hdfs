football_results = LOAD 'results.csv' USING PigStorage(',');
DUMP football_results;
