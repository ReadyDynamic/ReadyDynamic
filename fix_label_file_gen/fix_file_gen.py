import pandas as pd
import ast

format_file = '../../Output/dynamic_extract/format/BGL_dynamic_format.csv'
new_structured_file = '../../Output/dynamic_extract/structured_file/BGL_dynamic_structured.csv'

label_file = '../../Output/label_files/BGL.xlsx'

def replace_nth_placeholder(template: str, token: str, n: int) -> str:
    count = 0
    result = []
    i = 0
    while i < len(template):
        if template.startswith("<*>", i):
            count += 1
            if count == n:
                result.append(token)
            else:
                result.append("<*>")
            i += 3
        else:
            result.append(template[i])
            i += 1
    return "".join(result)

new_structured_df = pd.read_csv(new_structured_file)
format_df = pd.read_csv(format_file)

new_old_dict = {}
groups_by_old_templates = new_structured_df.groupby('NewTemplate')
for new_template, group_df in groups_by_old_templates:
    old_template = group_df['EventTemplate'].iloc[0]
    new_old_dict[new_template] = old_template

groups_by_templates = format_df.groupby('Template')
label_df_list = []
for new_template, group_df in groups_by_templates:
    for row in group_df.itertuples(index=False):
        t_format = row.TokenFormat
        token_rep = ast.literal_eval(row.Tokens)[0]
        c_format = row.ComponentFormat
        value_list_repre = ast.literal_eval(row.ValueList)[0]
        p_index = int(row.ParameterIndex)
        content_rep = replace_nth_placeholder(new_template, token_rep, p_index+1)
        old_template = new_old_dict[new_template]
        label_df_list.append([old_template, new_template, content_rep, p_index, t_format, token_rep, c_format, value_list_repre])
label_df = pd.DataFrame(label_df_list, columns=['OldTemplate', 'NewTemplate', 'Content', 'ParameterIndex', 'TokenFormat', 'Token', 'ComponentFormat', 'ValueList'])
label_df.to_excel(label_file, index=False)

# for row in format_df.itertuples(index=False):
#     t_format = row.TokenFormat
#     tokens = ast.literal_eval(row.Tokens)
#     c_format = row.ComponentFormat
#     value_list = ast.literal_eval(row.ValueList)
#     p_index = int(row.ParameterIndex)
#     new_template = row.Template


# old_template_list = list(dict.fromkeys(new_structured_df['EventTemplate'].tolist()))
# for old_template in old_template_list:
#     pass