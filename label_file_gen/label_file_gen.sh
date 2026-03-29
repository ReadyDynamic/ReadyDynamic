input_dir=../../data/Drain_result
output_dir=../../data/Label_files

for dataset in Proxifier Linux Apache Zookeeper Hadoop HealthApp OpenStack HPC Mac OpenSSH Spark Thunderbird BGL HDFS
do
  python label_file_generator.py --d ${dataset} --i ${input_dir} --o ${output_dir}
done