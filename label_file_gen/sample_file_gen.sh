input_dir=../../data/Label_files
output_dir=../../data/Label_files/Sample_files

for dataset in Proxifier Linux Apache Zookeeper Hadoop HealthApp OpenStack HPC Mac OpenSSH Spark Thunderbird BGL HDFS
do
  python sample_label_file.py --d ${dataset} --i ${input_dir} --o ${output_dir}
done