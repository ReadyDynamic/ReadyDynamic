## Requirements

Install dependencies with:

```bash
pip install -r requirements.txt
```

## Setup
establish a src folder and a data folder, put the source code into the src folder, and the parsing results into the data folder.
Put your api key in the format_extract.py in dynamic_info_fix/dynamic_format_generation.

## Format file generation
run main.py or main_fix.py in dynamic_info_fix folder to generate analysis-ready dynamic information:

 ```bash
python src/dynamic_info_fix/main_fix.py --dataset Spark
```

 The input file are the structured file and template file from parsig result
 
 The output files and the pickle files store the intermediate data

By default, the script reads:

- `data/Drain_result/<DATASET>_full.log_structured.csv`
- `data/Drain_result/<DATASET>_full.log_templates.csv`

and writes outputs under:

- `Output/dynamic_extract/structured_file/`
- `Output/dynamic_extract/preprocessed/`
- `Output/dynamic_extract/grouped/`
- `Output/dynamic_extract/format/`
- `Output/dynamic_extract/pickle/`

### Common options

```bash
python src/dynamic_info_fix/main_fix.py \
  --dataset Spark \
  --sample-ratio 0.05 \
  --split-prefixes rdd_,broadcast_,mesos-slave-,mesos-master-
```

```bash
python src/dynamic_info_fix/main_fix.py \
  --dataset BGL \
  --special-vars X-,X+,Y-,Y+,Z-,Z+,x+,x-,y+,y-,z+,z-
```

```bash
python src/dynamic_info_fix/main_fix.py \
  --dataset Spark \
  --structured-file data/Drain_result/Spark_full.log_structured.csv \
  --template-file data/Drain_result/Spark_full.log_templates.csv \
  --output-root Output/dynamic_extract \
  --skip-format-extraction
```

### Arguments

- `--dataset`: Dataset name used for default input and output naming.
- `--structured-file`: Optional override for the structured CSV input path.
- `--template-file`: Optional override for the template CSV input path.
- `--output-root`: Root directory for generated artifacts.
- `--sample-ratio`: Sampling ratio used during dynamic format extraction.
- `--split-prefixes`: Comma-separated prefixes used in format extraction.
- `--special-vars`: Comma-separated special dynamic-value markers used in preprocessing.
- `--skip-format-extraction`: Skip the LLM-based format extraction stage and only produce grouped intermediate results.

## Main outputs

- `*_dynamic_structured.csv`: Structured records after template alignment and dynamic token extraction.
- `*_dynamic_preprocessed.csv`: Preprocessed dynamic-variable candidates with token and component formats.
- `*_dynamic_grouped.csv`: Dynamic-variable records after related-value grouping.
- `*_dynamic_format.csv`: Final extracted format information.
- `*.pkl`: Intermediate serialized DataFrames for downstream analysis.

## Notes

- `main.py` preserves the original hard-coded execution style.
- `main_fix.py` is the recommended entry point if you want a cleaner CLI workflow.
- If you only want to validate the preprocessing and grouping stages, use `--skip-format-extraction` to avoid the LLM call.

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
