import ast

def rewrite_parameter_list(param_list, template, format_df):
    format_group_lookup = {
        key: group
        for key, group in format_df.groupby(["Template", "ParameterIndex"])
    }
    param_list = ast.literal_eval(param_list)
    print(len(param_list))

    for index in range(len(param_list)):
        key = (template, index)
        format_rows = format_group_lookup[key]
        if format_rows is None:
            print('Error')