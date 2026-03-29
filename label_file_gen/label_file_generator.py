import pandas as pd
import ast

from utils.common import common_args

def label_info_extractor(template, structured_df, n):
    info_df = structured_df.loc[[template]]
    if len(info_df) < 2:
        n = 1
    sample_df = info_df.sample(n=n, random_state=19970411)
    info_list = []
    for row in sample_df.itertuples(index=False):
        content = row.Content
        para_str = row.ParameterList
        para_list = ast.literal_eval(para_str)
        info_list.append([content, para_list])
    return info_list

if __name__ == "__main__":
    args = common_args()
    dataset = args.dataset
    input_dir = args.input
    output_dir = args.output
    n_sample = args.sample

    structured_file = input_dir + '/' + dataset + '_full.log_structured.csv'
    template_file = input_dir + '/' + dataset + '_full.log_templates.csv'
    output_file = output_dir + '/' + dataset + '_labels.xlsx'

    template_list = pd.read_csv(template_file)['EventTemplate'].tolist()
    structured_df = pd.read_csv(structured_file)
    structured_df = structured_df.set_index('EventTemplate')

    label_info_list = []
    for template in template_list:
        if '<*>' in template:
            info_list = label_info_extractor(template, structured_df, n_sample)
            index = 0
            while index < len(info_list):
                info = info_list[index]
                log = info[0]
                para_list = info[1]
                p_index = 0
                for para in para_list:
                    row = [template, log, p_index, para]
                    label_info_list.append(row)
                    p_index += 1
                index += 1
        else:
            pass

    label_info_df = pd.DataFrame(label_info_list, columns=['Template', 'Log', 'Index', 'Value'])
    label_info_df.to_excel(output_file, index=False)