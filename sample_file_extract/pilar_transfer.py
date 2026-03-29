import pandas as pd

datasets = ['Apache', 'BGL', 'Hadoop', 'HDFS', 'HealthApp', 'HPC', 'Linux', 'Mac', 'OpenSSH', 'OpenStack', 'Proxifier', 'Spark', 'Thunderbird', 'Zookeeper']

for dataset in datasets:
    structured_file = '../../data/PILAR_result/old/' + dataset + '_full.log_structured.csv'
    new_structured_file = '../../data/PILAR_result/' + dataset + '_full.log_structured.csv'
    structured_df = pd.read_csv(structured_file)
    structured_df.columns = ['LineId', 'EventId', 'Content', 'EventTemplate']
    structured_df['LineId'] = structured_df['LineId'].astype(int) + 1
    structured_df.to_csv(new_structured_file, index=False)