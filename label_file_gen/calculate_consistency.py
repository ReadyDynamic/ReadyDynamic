import pandas as pd

def calculate_similarity(label_0, label_1):
    pass

file_0_prefix = '../../data/Label_result_sample/part1_new/tingxv/'
file_1_prefix = '../../data/Label_result_sample/part1_new/xiaohui/'
result_prefix = '../../data/Label_result_sample/part1_new/comp_result/'

file_suffix = '_labels.xlsx'

datasets_part1 = ['Apache', 'Linux', 'OpenSSH', 'OpenStack', 'Proxifier', 'Spark', 'Thunderbird', 'Zookeeper']
datasets_part2 = ['BGL', 'Hadoop', 'HDFS', 'HealthApp', 'HPC', 'Mac']
# datasets_part2 = ['BGL']

for dataset in datasets_part1:
    print(dataset)
    file_0 = file_0_prefix + dataset + file_suffix
    file_1 = file_1_prefix + dataset + file_suffix
    result_file = result_prefix + dataset + file_suffix

    label_0_df = pd.read_excel(file_0)
    label_1_df = pd.read_excel(file_1)

    mask_na = label_0_df['Type'].isna() & label_1_df['Type'].isna()
    n_total = len(label_0_df) - len(label_0_df[mask_na])

    mask = ~(
            (label_0_df['Type'] == label_1_df['Type']) |
            (label_0_df['Type'].isna() & label_1_df['Type'].isna())
    )
    result_0_df = label_0_df[mask]
    result_1_df = label_1_df[mask]

    n_inconsistency = len(result_0_df)

    result_0_df = result_0_df.rename(columns={'Type': 'type_tingxv'})
    result_1_df = result_1_df.rename(columns={'Type': 'type_xiaohui'})
    combined_df = pd.concat([result_0_df, result_1_df['type_xiaohui']], axis=1)
    combined_df.to_excel(result_file)
    print(((n_total - n_inconsistency)/n_total)*100)