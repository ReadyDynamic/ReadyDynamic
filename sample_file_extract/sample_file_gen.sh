parser=LibreLog
output_dir=../../Output/sample_files/sample_files

for dataset in Apache BGL Hadoop HDFS HealthApp HPC Linux Mac OpenSSH OpenStack Proxifier Spark Thunderbird Zookeeper
do
  python sample_file_extract.py --d ${dataset} --p ${parser} --o ${output_dir}
done