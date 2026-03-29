import pandas as pd

from common import check_unit

def generate_combined_dynamic_template(templates):
    """Use new template as key for index of dynamic information"""
    old_new_dict = {}
    temp_index_dict = {}
    for template in templates:
        new_token_list = []
        dynamic_index = []
        ori_template_tokens = template.split(' ')
        token_index = 0
        for token in ori_template_tokens:
            if '<*>' in token:
                new_token_list.append('<*>')
                dynamic_index.append(token_index)
            else:
                new_token_list.append(token)
            token_index = token_index + 1
        new_template = ' '.join(new_token_list)
        old_new_dict[template] = new_template
        temp_index_dict[new_template] = dynamic_index
    return old_new_dict, temp_index_dict

def dynamic_token_extract(content_tokens, new_template, temp_index_dict):
    dynamic_token_list = []

    dynamic_index = temp_index_dict[new_template]
    tokens = content_tokens

    for index in dynamic_index:
        dynamic_token_list.append(tokens[index])

    return dynamic_token_list

def update_dynamic_token_extract_info(content_tokens, new_template_tokens):
    anchors = [token for token in new_template_tokens if token != "<*>"]
    update_template_tokens = []
    update_dynamic_index_list = []

    content_token_index = 0
    for anchor in anchors:
        while content_token_index < len(content_tokens) and content_tokens[content_token_index] != anchor:
            if content_tokens[content_token_index] == '':
                update_template_tokens.append('')
                content_token_index += 1
            else:
                update_template_tokens.append('<*>')
                update_dynamic_index_list.append(content_token_index)
                content_token_index += 1
        if content_token_index < len(content_tokens):
            update_template_tokens.append(anchor)
            content_token_index += 1

    while content_token_index < len(content_tokens):
        if content_tokens[content_token_index] == '':
            update_template_tokens.append('')
            content_token_index += 1
        else:
            update_template_tokens.append('<*>')
            update_dynamic_index_list.append(content_token_index)
            content_token_index += 1

    update_new_template = ' '.join(update_template_tokens)
    return update_new_template, update_dynamic_index_list

def generate_combined_dynamic_structured_df(structured_df, old_new_dict, temp_index_dict):
    new_df_info = []
    for row in structured_df.itertuples(index=False):
        new_row = []
        template = row.EventTemplate
        new_template = old_new_dict[template]
        if '<*>' in template:
            content = row.Content
            content_tokens = content.split(' ')
            new_template_tokens = new_template.split(' ')
            if len(content_tokens) == len(new_template_tokens):
                para_list = dynamic_token_extract(content_tokens, new_template, temp_index_dict)
                new_row = new_row + list(row[:-1]) + [new_template, para_list]
                new_df_info.append(new_row)
            else:
                """need to update new template, old new template dict, temp index dict"""
                update_new_template, update_dynamic_index_list = update_dynamic_token_extract_info(content_tokens, new_template_tokens)
                old_new_dict[template] = update_new_template
                temp_index_dict[update_new_template] = update_dynamic_index_list
                para_list = dynamic_token_extract(content_tokens, update_new_template, temp_index_dict)
                new_row = new_row + list(row[:-1]) + [update_new_template, para_list]
                new_df_info.append(new_row)
        else:
            pass
            # new_row = new_row + list(row[:-1]) + [new_template, []]

    headers_old = list(structured_df.columns)[:-1]
    headers_new = headers_old + ['NewTemplate', 'ParameterList']
    new_structured_df = pd.DataFrame(new_df_info, columns=headers_new)
    return new_structured_df, old_new_dict, temp_index_dict

def get_template_with_dynamic(templates):
    template_with_dynamic = []
    for template in templates:
        if '<*>' in template:
            template_with_dynamic.append(template)
    return template_with_dynamic
