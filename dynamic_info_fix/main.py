import pandas as pd
import time
import pickle
import ast

from dynamic_info_combine import generate_combined_dynamic_template, generate_combined_dynamic_structured_df, get_template_with_dynamic
from dynamic_info_fix import dynamic_based_df_generate, preprocess, value_format_generate, dynamic_based_df_generate_v2, find_same_value_group, find_contain_value_group, update_dynamic_based_df_with_group_info, value_format_generate_with_group

prefix_config = {
    'Hadoop':['attempt_', 'task_'],
    'Spark':['rdd_', 'broadcast_', 'mesos-slave-', 'mesos-master-'],
    'BGL':[]
}

special_var_config = {
    'BGL':['X-', 'X+', 'Y-', 'Y+', 'Z-', 'Z+', 'x+', 'x-', 'y+', 'y-', 'z+', 'z-'],
}

if __name__ == "__main__":
    dataset = 'Spark'

    structured_file = '../../data/Drain_result/' + dataset + '_full.log_structured.csv'
    template_file = '../../data/Drain_result/' + dataset + '_full.log_templates.csv'

    output_file_structured = '../../Output/dynamic_extract/structured_file/' + dataset + '_dynamic_structured.csv'
    output_file_preprocessed = '../../Output/dynamic_extract/preprocessed/' + dataset + '_dynamic_preprocessed.csv'
    output_file_grouped = '../../Output/dynamic_extract/grouped/' + dataset + '_dynamic_grouped.csv'
    output_file_format = '../../Output/dynamic_extract/format/' + dataset + '_dynamic_format.csv'
    pickle_file = '../../Output/dynamic_extract/pickle/' + dataset + '.pkl'
    pickle_file_format = '../../Output/dynamic_extract/format/' + dataset + '_format.pkl'

    begin_time = time.time()

    template_df = pd.read_csv(template_file)
    structured_df = pd.read_csv(structured_file)

    template_list = template_df['EventTemplate'].tolist()
    new_old_template_dict, template_dynamic_dict= generate_combined_dynamic_template(template_list)
    new_structured_df, new_old_template_dict, template_dynamic_dict = generate_combined_dynamic_structured_df(structured_df, new_old_template_dict, template_dynamic_dict)
    template_with_dynamic = get_template_with_dynamic(template_list)

    print('Finish original data fix')
    data_fix_time = time.time()
    print(data_fix_time - begin_time)
    new_structured_df.to_csv(output_file_structured, index=False)

    """Use new template as index in the following part"""
    new_template_list_with_dynamic = []
    for template in template_with_dynamic:
        new_template = new_old_template_dict[template]
        new_template_list_with_dynamic.append(new_template)
    dynamic_based_df = dynamic_based_df_generate_v2(new_structured_df, new_template_list_with_dynamic, extract_from_file=False)

    print('Finish dynamic based data generation')
    dynamic_based_time = time.time()
    print(dynamic_based_time - begin_time)

    dynamic_based_updated_df = preprocess(dynamic_based_df, special_var_config[dataset], template_dynamic_dict)
    dynamic_based_updated_df['DynamicID'] = dynamic_based_updated_df.index
    print('Finish dynamic based data preprocess')
    preprocess_time = time.time()
    print(preprocess_time - begin_time)
    dynamic_based_updated_df[['DynamicID','Template', 'ParameterIndex', 'Tokens', 'TokenFormat', 'ComponentFormat', 'ValueList', 'ValueType']].to_csv(output_file_preprocessed, index=False)

    """Check the format and find the related dynamic variables"""
    print('Begin related dynamic variable finding process')
    same_value_group = find_same_value_group(dynamic_based_updated_df)
    dynamic_based_updated_df = update_dynamic_based_df_with_group_info(dynamic_based_updated_df, same_value_group)
    dynamic_based_updated_df.to_csv(output_file_grouped, index=False)

    print('Finish related dynamic variable finding process')
    group_time = time.time()
    print(group_time - begin_time)
    with open(pickle_file, 'wb') as f:
        pickle.dump(dynamic_based_updated_df, f)

    """Generate dynamic variable format for undecided type value"""
    print('Begin dynamic format extraction')
    dynamic_based_updated_df = pickle.load(open(pickle_file, 'rb'))
    print(dynamic_based_updated_df)
    begin_time = time.time()
    dynamic_format_df = value_format_generate_with_group(dynamic_based_updated_df, sample_ratio=0.05, split_prefixes=prefix_config[dataset])
    dynamic_format_df.to_csv(output_file_format, index=False)

    with open(pickle_file_format, 'wb') as f:
        pickle.dump(dynamic_format_df, f)

    print('Finish dynamic format extraction')
    extract_time = time.time()
    print(extract_time - begin_time)