import pandas as pd
import re
def regex_template_to_event_template(s: str) -> str:
    s = re.sub(r'\(\.\*\?\)', '<*>', s)

    s = re.sub(r'\\([\\()\[\]{}.+*?|^$:/\-])', r'\1', s)

    s = re.sub(r'\$$', '', s)

    return s

datasets = ['Apache', 'BGL', 'Hadoop', 'HDFS', 'HealthApp', 'HPC', 'Linux', 'Mac', 'OpenSSH', 'OpenStack', 'Proxifier', 'Spark', 'Thunderbird', 'Zookeeper']

for dataset in datasets:
    print(dataset)
    structured_file = '../../data/LibreLog_result/' + dataset + '/3_sorted.csv'
    new_structured_file = '../../data/LibreLog_result/' + dataset + '_full.log_structured.csv'
    structured_df = pd.read_csv(structured_file)
    structured_df = structured_df.rename(columns={'RegexTemplate':'EventTemplate'})
    structured_df['LineId'] = structured_df.index + 1
    structured_df['EventTemplate'] = structured_df['EventTemplate'].apply(regex_template_to_event_template)
    structured_df.to_csv(new_structured_file, index=False)
