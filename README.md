## Setup
establish a src folder and a data folder, put the source code into the src folder, and the parsing results into the data folder

## Format file generation
run main.py in dynamic_info_fix folder to generate analysis-ready dynamic information:
 -The input file are the structured file and template file from parsig result
 -The output files and the pickle files store the intermediate data

## Label file generation for log parsers
run sample_file_gen.sh in label_file_gen to generate files for labeling of LibreLog and PILAR
run label_file_gen.sh in label_file_gen to generate files for labeling of Preprocess-Drain

## Label file generation for Ready-D
run fix_file_gen.py in fix_label_file_gen folder to generate files for labeling of Ready-D

## Data
The data for the replication test is available at: https://zenodo.org/records/19320772
You can use the parsing result for analysis-ready dynamic information generation

## Date
This version is updated on Marth 30th
