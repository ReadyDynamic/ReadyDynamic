import pandas as pd
import pickle
import ast
from tqdm import tqdm

def get_analysis_ready_value(token, format_rows):
    for row in format_rows.itertuples(index=False):
        if token in row.Tokens:
            tokenformat = row.TokenFormat.replace('<C>', row.ComponentFormat)
            return row.ValueList[row.Tokens.index(token)], tokenformat, row.ValueType

def update_unit_value(new_para_list, unit_pos):
    for index in unit_pos:
        value_index = index-1
        unit = new_para_list[index][0]
        value = float(new_para_list[value_index][0])

        if unit == 'KB':
            pass
        elif unit == 'MB':
            value = value*1024
        elif unit == 'GB':
            value = value*1024*1024
        elif unit == 'B':
            value = 0
        else:
            pass
        temp = list(new_para_list[value_index])
        temp[0] = value
        new_para_list[value_index] = tuple(temp)
    return new_para_list

pickle_file_format = '../../Output/dynamic_extract/format/Spark_format.pkl'
analysis_list_file = 'analysis_list.pkl'
dynamic_format_df = pickle.load(open(pickle_file_format, 'rb'))
broadcast_structured_df = pickle.load(open('../../Output/downstream/broadcast.pkl', 'rb'))

broadcast_format_df = dynamic_format_df[
    dynamic_format_df['Template'] == "<*> <*> in memory on <*> (size: <*> <*> free: <*> <*>"]
broadcast_structured_df = broadcast_structured_df

format_lookup = {
    key: group
    for key, group in dynamic_format_df.groupby(["Template", "ParameterIndex"])
}

analysis_list = []

for row in tqdm(broadcast_structured_df.itertuples(index=False),
                total=len(broadcast_structured_df),
                desc="Processing"):
    para_list = ast.literal_eval(row.ParameterList)
    template = row.NewTemplate

    index = 0
    new_para_list = []
    unit_pos = []

    for index, param in enumerate(para_list):
        format_rows = format_lookup.get((template, index))

        if format_rows is None:
            value_list = param
            token_format = '<D>'
        else:
            value_list, token_format, value_type = get_analysis_ready_value(param, format_rows)
            if value_type == 'Unit':
                unit_pos.append(index)

        new_para_list.append((value_list, token_format))

    new_para_list = update_unit_value(new_para_list, unit_pos)
    analysis_list.append(new_para_list)

with open(analysis_list_file, 'wb') as f:
    pickle.dump(analysis_list, f)