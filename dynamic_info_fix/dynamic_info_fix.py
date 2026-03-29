import random
import pandas as pd
import json
import re
from collections import defaultdict

from common import preprocess_punc, value_type_verify, unit_type_verify, host_type_verify, path_type_verify, pattern_to_regex, ip_format_verify, path_format_verify, macro_type_verify, suffix_dot_digit_verify
from dynamic_format_generation.format_memory import ValidatedFormat, RejectedFormat, RejectOutput
from dynamic_format_generation.format_extract import FormatExtractor

dynamic_types = ['Unit', 'Value', 'Identifier', 'Path', 'Undecided']

# def get_dynamic_based_info(sampled_representative_info):
#     dynamic_representative_info = []
#     n_para = len(sampled_representative_info[0])
#     p_index = 0
#     while p_index < n_para:
#         para_list = []
#         for para_info in sampled_representative_info:
#             if para_info[p_index] in para_list:
#                 pass
#             else:
#                 para_list.append(para_info[p_index])
#         dynamic_representative_info.append(para_list)
#         p_index += 1
#     return dynamic_representative_info

def get_dynamic_based_info(sampled_representative_info):
    dynamic_representative_info = []
    n_para = len(sampled_representative_info[0])
    for p_index in range(n_para):
        seen = set()
        para_list = []
        for para_info in sampled_representative_info:
            v = para_info[p_index]
            if v not in seen:
                seen.add(v)
                para_list.append(v)
        dynamic_representative_info.append(para_list)
    return dynamic_representative_info

def dynamic_based_df_generate_v2(new_structured_df, templates, extract_from_file):
    dynamic_based_df_list = []
    grouped = new_structured_df.groupby('NewTemplate', sort=False)

    for template, group in grouped:
        group_unique = group.drop_duplicates(subset='Content', keep='first')
        ori_template = group['EventTemplate'].iloc[0]

        para_info_list = group_unique['ParameterList'].tolist()
        dynamic_based_info = get_dynamic_based_info(para_info_list)
        content_list = group_unique['Content'].tolist()

        for para_index, para_value_list in enumerate(dynamic_based_info):
            dynamic_based_df_list.append([
                ori_template,
                template,
                content_list,
                para_index,
                para_value_list
            ])

    return pd.DataFrame(dynamic_based_df_list,
                        columns=['Template', 'NewTemplate', 'ContentList', 'ParameterIndex', 'ParameterValue'])

def dynamic_based_df_generate(new_structured_df, templates, extract_from_file):
    dynamic_based_df_list = []
    for template in templates:
        selected_df = new_structured_df[new_structured_df['NewTemplate'] == template]
        selected_unique_df = selected_df.drop_duplicates(subset='Content', keep='first')
        ori_template = selected_df['EventTemplate'].iloc[0]

        para_info_list = selected_unique_df['ParameterList'].tolist()
        dynamic_based_info = get_dynamic_based_info(para_info_list)
        content_list = selected_unique_df['Content'].tolist()

        para_index = 0
        while para_index < len(dynamic_based_info):
            para_value_list = dynamic_based_info[para_index]
            dynamic_based_df_list.append([ori_template, template, content_list, para_index, para_value_list])
            para_index += 1

    headers = ['Template', 'NewTemplate', 'ContentList', 'ParameterIndex', 'ParameterValue']
    dynamic_based_df = pd.DataFrame(dynamic_based_df_list, columns=headers)
    return dynamic_based_df

def representative_df_extract(new_structured_df, templates, n_representative, extract_from_file):
    representative_df_list = []
    for template in templates:
        selected_df = new_structured_df[new_structured_df['NewTemplate'] == template]
        selected_unique_df = selected_df.drop_duplicates(subset='Content', keep='first')
        ori_template = selected_df['EventTemplate'].iloc[0]

        dynamic_representative_info = []
        content_list = []
        if len(selected_unique_df) < n_representative:
            sampled_df = selected_unique_df.sample(n=len(selected_unique_df), random_state=19970411)
            para_info_list = sampled_df['ParameterList'].tolist()
            dynamic_representative_info = get_dynamic_based_info(para_info_list)
            content_list = sampled_df['Content'].tolist()
        else:
            sampled_df = selected_unique_df.sample(n=n_representative, random_state=19970411)
            para_info_list = sampled_df['ParameterList'].tolist()
            dynamic_representative_info = get_dynamic_based_info(para_info_list)
            content_list = sampled_df['Content'].tolist()

        """The following is construct the structure of new dataframe for dynamic information fix"""
        para_index = 0
        while para_index < len(dynamic_representative_info):
            para_value_list = dynamic_representative_info[para_index]
            representative_df_list.append([ori_template, template, content_list, para_index, para_value_list])
            para_index += 1

    headers = ['Template', 'NewTemplate', 'ContentList', 'ParameterIndex', 'ParameterValue']
    representative_df = pd.DataFrame(representative_df_list, columns=headers)
    return representative_df

IP_REGEX = re.compile(r'(?:\b(?:\d{1,3}\.){3}\d{1,3})$')
HEX_REGEX = re.compile(r'(?:0x[0-9a-fA-F]+|[0-9a-fA-F]{8,})$')
EQ_DIGIT_END_REGEX = re.compile(r'=(\d+)$')
def normalize_sub_components_for_list(value_list):
    if all(v.startswith("url=") for v in value_list):
        extracted = [v[len("url="):] for v in value_list]
        return "url=<C>", extracted
    def try_pattern(regex, replace_func):
        extracted = []
        normalized = []
        for v in value_list:
            m = regex.search(v)
            if not m:
                return None, None
            extracted.append(m.group(0))
            normalized.append(replace_func(v, m))
        if len(set(normalized)) == 1:
            return normalized[0], extracted
        return None, None

    fmt, vals = try_pattern(IP_REGEX, lambda v, m: v[:m.start()] + "<C>")
    if fmt:
        return fmt, vals

    fmt, vals = try_pattern(HEX_REGEX, lambda v, m: v[:m.start()] + "<C>")
    if fmt:
        return fmt, vals

    fmt, vals = try_pattern(EQ_DIGIT_END_REGEX, lambda v, m: v[:m.start(1)] + "<C>")
    if fmt:
        return fmt, vals

    return "<C>", value_list


def preprocess(dynamic_based_df, special_var_list, temp_index_dict):
    dynamic_based_updated_df_list = []
    for row in dynamic_based_df.itertuples(index=False):
        para_values = row.ParameterValue
        content_list = row.ContentList[0]
        template = row.NewTemplate
        para_index = row.ParameterIndex
        """Preprocess parameter"""
        if value_type_verify(para_values, special_var_list):
            token_format = '<C>'
            component_format = '<D>'
            value_type = 'Value'
            para_list = para_values
            dynamic_based_updated_df_list.append(
                [template, content_list, para_index, para_values, token_format, component_format, para_list, value_type])
        else:
            token_format, para_list = preprocess_punc(para_values)
            fmt, para_list = normalize_sub_components_for_list(para_list)
            token_format = token_format.replace('<C>', fmt)

            value_type = 'Undecided'

            dot_digit_format_dict = suffix_dot_digit_verify(para_list)
            if dot_digit_format_dict:
                value_type = 'Value'
                for fmt in dot_digit_format_dict.keys():
                    extracted_list = dot_digit_format_dict[fmt]
                    dynamic_based_updated_df_list.append(
                        [template, content_list, para_index, para_values,
                        token_format, fmt, extracted_list, value_type]
                    )
                continue

            if value_type_verify(para_list, special_var_list):
                value_type = 'Value'
                component_format = '<D>'
                dynamic_based_updated_df_list.append(
                    [template, content_list, para_index, para_values, token_format, component_format, para_list, value_type])
            elif unit_type_verify(para_list):
                value_type = 'Unit'
                component_format = '<D>'
                dynamic_based_updated_df_list.append(
                    [template, content_list, para_index, para_values, token_format, component_format, para_list,
                     value_type])
            elif host_type_verify(para_list):
                value_type = 'Identifier'
                component_format = '<D>:<D>'
                dynamic_based_updated_df_list.append(
                    [template, content_list, para_index, para_values, token_format, component_format, para_list, value_type])
            elif path_type_verify(para_list):
                value_type = 'Path'
                component_format = '<D>'
                dynamic_based_updated_df_list.append(
                    [template, content_list, para_index, para_values, token_format, component_format, para_list,
                     value_type])
            elif macro_type_verify(para_list):
                value_type = 'Macro'
                component_format = '<D>'
                dynamic_based_updated_df_list.append(
                    [template, content_list, para_index, para_values, token_format, component_format, para_list,
                     value_type])
            elif 'url=' in token_format:
                value_type = 'Path'
                component_format = '<D>'
                dynamic_based_updated_df_list.append(
                    [template, content_list, para_index, para_values, token_format, component_format, para_list,
                     value_type])
            else:
                component_format = '<D>'
                dynamic_based_updated_df_list.append(
                    [template, content_list, para_index, para_values, token_format, component_format, para_list,
                     value_type])
    headers = ['Template', 'ContentList', 'ParameterIndex', 'Tokens', 'TokenFormat', 'ComponentFormat', 'ValueList', 'ValueType']
    dynamic_based_updated_df = pd.DataFrame(dynamic_based_updated_df_list, columns=headers)
    return dynamic_based_updated_df

"""TODO: connect value with its corresponding unit, generated a new dataframe"""
def combine_value_unit(structured_df, dynamic_based_df):
    pass

"""TODO: generate token format and extract value with LLM"""
def pattern0_match_pattern1(tmp0, tmp1):
    regex0 = pattern_to_regex(tmp0)
    match = re.search(regex0, tmp1)
    if (match):
        return True
    else:
        return False

def insert_pattern(tmp, tmplist):
    index = 0
    for t in tmplist:
        if ((pattern0_match_pattern1(t, tmp) == True) and (pattern0_match_pattern1(tmp, t) == False)):
            return index
        else:
            index = index + 1
    return -1

def fix_fault_format(pattern_list):
    new_pattern_list = []
    for pattern in pattern_list:
        fixed_pattern = pattern
        fixed_pattern = re.sub(r'(?:<D>\s*){2,}', '<D>', fixed_pattern)
        new_pattern_list.append(fixed_pattern)
    return new_pattern_list

def refine_pattern_list(pattern_list):
    pattern_list = fix_fault_format(pattern_list)
    new_pattern_list = []
    for index in range(0, len(pattern_list)):
        if (pattern_list[index] in new_pattern_list):
            pass
        else:
            pos = insert_pattern(pattern_list[index], new_pattern_list)
            if (pos == -1):
                new_pattern_list.append(pattern_list[index])
            else:
                new_pattern_list.insert(pos, pattern_list[index])
    return new_pattern_list

def get_unmatched_value(value_list, matched_value_list):
    unmatched_value_list = []
    for value in value_list:
        if value in matched_value_list:
            pass
        else:
            unmatched_value_list.append(value)
    return unmatched_value_list

def output_validate(llm_output):
    try:
        return json.loads(llm_output)
    except json.JSONDecodeError:
        return None

def match_value_with_format(value_list, format_list):
    format_value_dict = {}
    format_separate_value_dict = {}
    unmatched_format_list = []
    unmatched_values = set(value_list)

    format_list = refine_pattern_list(format_list)

    for fmt in format_list:
        format_value_dict[fmt] = []
        format_separate_value_dict[fmt] = []

        regex = re.compile(pattern_to_regex(fmt))
        matched_values = set()

        for value in unmatched_values:
            m = regex.match(value)
            if not m:
                continue

            matched_values.add(value)
            format_value_dict[fmt].append(value)

            groups = list(m.groups()) if m.groups() else []
            format_separate_value_dict[fmt].append(groups)

        if not matched_values:
            unmatched_format_list.append(fmt)
        else:
            unmatched_values -= matched_values

    return unmatched_format_list, list(unmatched_values), format_value_dict, format_separate_value_dict,

def match_value_with_format_old(value_list, format_list):
    format_value_dict = {}
    format_seperate_value_dict = {}
    unmatched_format_list = []
    unmatched_value_list = value_list[:]
    format_list = refine_pattern_list(format_list)
    for format in format_list:
        format_value_dict[format] = []
        format_seperate_value_dict[format] = []
        format_regex = pattern_to_regex(format)
        matched_value_list = []
        for value in unmatched_value_list:
            if re.match(format_regex, value):
                matched_value_list.append(value)
                format_value_dict[format].append(value)
                para_list = re.findall(format_regex, value)[0]
                if isinstance(para_list, tuple):
                    para_list = list(para_list)
                    format_seperate_value_dict[format].append(para_list)
                else:
                    para_list = [para_list]
                    format_seperate_value_dict[format].append(para_list)
        if len(format_value_dict[format]) == 0:
            unmatched_format_list.append(format)
        unmatched_value_list = [v for v in unmatched_value_list if v not in matched_value_list]
    return unmatched_format_list, unmatched_value_list, format_value_dict, format_seperate_value_dict

def verify_format_with_value(value_list, format_list):
    unmatched_value_list = value_list[:]
    verified_format_list = []
    unmatched_format_list = []

    format_list = refine_pattern_list(format_list)
    for fmt in format_list:
        format_regex = pattern_to_regex(fmt)

        matched_values = [
            v for v in unmatched_value_list
            if re.match(format_regex, v)
        ]

        if not matched_values:
            unmatched_format_list.append(fmt)
        else:
            verified_format_list.append(fmt)
        if matched_values:
            matched_set = set(matched_values)
            unmatched_value_list = [
                v for v in unmatched_value_list
                if v not in matched_set
            ]

    return unmatched_value_list, verified_format_list, unmatched_format_list

def fix_format(format_seperate_value_dict, format_value_dict):
    keys = list(format_value_dict.keys())
    for format in keys:
        if ip_format_verify(format) or path_format_verify(format):
            vals_from_value = format_value_dict.pop(format, [])
            vals_from_separate = format_seperate_value_dict.pop(format, [])

            format_value_dict.setdefault('<D>', [])
            format_seperate_value_dict.setdefault('<D>', [])

            format_value_dict['<D>'].extend(vals_from_value)
            format_seperate_value_dict['<D>'].extend(vals_from_value)
    return format_seperate_value_dict, format_value_dict

def llm_generated_format_repair(format_list):
    new_format_list = []
    for format in format_list:
        new_format = format
        new_format = re.sub(r'<D(?!>)', '<D>', new_format)
        new_format = re.sub(r'(?<!<)D>', '<D>', new_format)
        new_format_list.append(new_format)
    return new_format_list

def seperate_merged_list(merged_value_list, split_prefixes=None):
    extract_lists = []

    if split_prefixes:
        prefix_buckets = {p: [] for p in split_prefixes}
        other_bucket = []

        for v in merged_value_list:
            matched = False
            for p in split_prefixes:
                if v.startswith(p):
                    prefix_buckets[p].append(v)
                    matched = True
                    break
            if not matched:
                other_bucket.append(v)

        # 只对「至少有 2 个 value」的 bucket 做 extract
        for bucket in prefix_buckets.values():
            if len(bucket) >= 2:
                extract_lists.append(bucket)

        if len(other_bucket) >= 2:
            extract_lists.append(other_bucket)

        # fallback：全都没拆开
        if not extract_lists:
            extract_lists = [merged_value_list]

    else:
        extract_lists = [merged_value_list]
    return extract_lists

def estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)

def token_aware_sample(values: list[str], max_tokens=30000, min_samples: int = 5):
    sorted_values = sorted(values, key=len)

    sampled = []
    token_sum = 0

    for v in sorted_values:
        t = estimate_tokens(v)
        if token_sum + t > max_tokens:
            break
        sampled.append(v)
        token_sum += t

    if len(sampled) < min_samples:
        sampled = sorted_values[:min_samples]

    return sampled

def extract_formats_from_merged(merged_value_list, format_extactor, sample_ratio, min_size,
                        rejected_format_memory, rejected_output_memory,
                        split_prefixes=None):
    extract_lists = seperate_merged_list(merged_value_list, split_prefixes)
    all_formats = []

    for value_list in extract_lists:
        sample_size = int(len(value_list) * sample_ratio)
        if sample_size < min_size:
            sampled_list = (
                value_list if len(value_list) < min_size
                else token_aware_sample(value_list)
            )
        else:
            sampled_list = token_aware_sample(value_list)

        llm_output = format_extactor.extract(sampled_list, use_retry=1)
        format_list = output_validate(llm_output)

        while format_list is None:
            rejected_output_memory.add_output(llm_output)
            llm_output = format_extactor.extract(sampled_list, use_retry=2)
            print(llm_output)
            format_list = output_validate(llm_output)

        rejected_output_memory.clean_outputs()
        format_list = llm_generated_format_repair(format_list)
        unmatched_value_list, verified_format_list, unmatched_format_list = verify_format_with_value(value_list, format_list)
        all_formats.extend(verified_format_list)

        for format in unmatched_format_list:
            rejected_format_memory.add_format(format)
        while (len(unmatched_value_list) > 0):
            sampled_list = token_aware_sample(unmatched_value_list)
            llm_output = format_extactor.extract(sampled_list, use_retry=0)
            print(unmatched_value_list)
            print(llm_output)
            format_list = output_validate(llm_output)
            while (format_list == None):
                rejected_output_memory.add_output(llm_output)
                llm_output = format_extactor.extract(sampled_list, use_retry=2)
                format_list = output_validate(llm_output)
            format_list = llm_generated_format_repair(format_list)
            unmatched_value_list, verified_format_list, unmatched_format_list = verify_format_with_value(unmatched_value_list, format_list)
            all_formats.extend(verified_format_list)
            for format in unmatched_format_list:
                rejected_format_memory.add_format(format)
    return list(set(all_formats))

def fill_values_for_row(row, format_list, group_id):
    new_rows = []
    unmatched_format_list, unmatched_value_list, format_value_dict, format_seperate_value_dict = match_value_with_format(row.ValueList, format_list)

    format_seperate_value_dict = {k: v for k, v in format_seperate_value_dict.items() if v}
    format_value_dict = {k: v for k, v in format_value_dict.items() if v}

    format_seperate_value_dict, format_value_dict = fix_format(format_seperate_value_dict, format_value_dict)
    for fmt in format_seperate_value_dict:
        seperated_value_list = format_seperate_value_dict[fmt]
        value_list = format_value_dict[fmt]
        token_list = [
            row.TokenFormat.replace('<C>', v)
            for v in value_list
        ]
        new_rows.append([
            row.Template,
            row.ParameterIndex,
            row.TokenFormat,
            token_list,
            fmt,
            seperated_value_list,
            row.ValueType,
            group_id
        ])
    return new_rows

def value_format_generate_with_group(dynamic_based_updated_df, sample_ratio = 0.25, min_size = 5, split_prefixes=None):
    validated_format_memory = ValidatedFormat()
    rejected_format_memory = RejectedFormat()
    rejected_output_memory = RejectOutput()
    format_extactor = FormatExtractor(validated_format_memory, rejected_format_memory, rejected_output_memory)
    dynamic_format_df_info = []
    grouped = dynamic_based_updated_df.groupby('GroupID', sort=False)

    for group_id, group_df in grouped:
        group_id = int(group_id)
        if (group_df['ValueType'] != 'Undecided').all():
            for row in group_df.itertuples(index=False):
                dynamic_format_df_info.append([
                    row.Template,
                    row.ParameterIndex,
                    row.TokenFormat,
                    row.Tokens,
                    row.ComponentFormat,
                    row.ValueList,
                    row.ValueType
                ])
            continue

        merged_value_list = []
        for vl in group_df['ValueList']:
            merged_value_list.extend(vl)
        merged_value_list = list(dict.fromkeys(merged_value_list))

        format_list = extract_formats_from_merged(merged_value_list, format_extactor, sample_ratio, min_size,
            rejected_format_memory, rejected_output_memory,
            split_prefixes
        )

        for row in group_df.itertuples(index=False):
            if row.ValueType == "Undecided":
                new_rows = fill_values_for_row(row, format_list, group_id)
                dynamic_format_df_info.extend(new_rows)
            else:
                dynamic_format_df_info.append([
                    row.Template,
                    row.ParameterIndex,
                    row.TokenFormat,
                    row.Tokens,
                    row.ComponentFormat,
                    row.ValueList,
                    row.ValueType,
                    group_id
                ])
    headers = ['Template', 'ParameterIndex', 'TokenFormat', 'Tokens', 'ComponentFormat', 'ValueList', 'ValueType', 'GroupID']
    dynamic_format_df = pd.DataFrame(data=dynamic_format_df_info, columns=headers)
    return dynamic_format_df

def value_format_generate(dynamic_based_updated_df, sample_ratio = 0.25, min_size = 5):
    validated_format_memory = ValidatedFormat()
    rejected_format_memory = RejectedFormat()
    rejected_output_memory = RejectOutput()
    format_extactor = FormatExtractor(validated_format_memory, rejected_format_memory, rejected_output_memory)
    dynamic_format_df_info = []
    index = 0
    num = len(dynamic_based_updated_df)

    for row in dynamic_based_updated_df.itertuples(index=False):
        print((index/num)*100)
        index=index+1
        if row.ValueType == 'Undecided':
            value_list = row.ValueList
            print(value_list)
            sampled_list = []
            sample_size = int(len(value_list) * sample_ratio)
            if sample_size < min_size:
                if len(value_list) < min_size:
                    sampled_list = value_list
                else:
                    sampled_list = random.sample(value_list, min_size)
            else:
                sampled_list = random.sample(value_list, sample_size)
            llm_output = format_extactor.extract(sampled_list, use_retry=1)
            print(llm_output)
            format_list = output_validate(llm_output)
            while (format_list == None):
                rejected_output_memory.add_output(llm_output)
                llm_output = format_extactor.extract(sampled_list, use_retry=2)
                print(llm_output)
                format_list = output_validate(llm_output)
            rejected_output_memory.clean_outputs()
            format_list = llm_generated_format_repair(format_list)
            unmatched_format_list, unmatched_value_list, format_value_dict, format_seperate_value_dict = match_value_with_format(
                value_list, format_list)
            for format in unmatched_format_list:
                rejected_format_memory.add_format(format)
            while (len(unmatched_value_list) > 0):
                llm_output = format_extactor.extract(unmatched_value_list, use_retry=0)
                print(llm_output)
                format_list = output_validate(llm_output)
                while (format_list == None):
                    rejected_output_memory.add_output(llm_output)
                    llm_output = format_extactor.extract(sampled_list, use_retry=2)
                    print(llm_output)
                    format_list = output_validate(llm_output)
                format_list = llm_generated_format_repair(format_list)
                unmatched_format_list, unmatched_value_list, format_value_dict_tmp, format_seperate_value_dict_tmp = match_value_with_format(
                    unmatched_value_list, format_list)
                for format in unmatched_format_list:
                    rejected_format_memory.add_format(format)
                format_value_dict.update(format_value_dict_tmp)
                format_seperate_value_dict.update(format_seperate_value_dict_tmp)
            format_seperate_value_dict = {k: v for k, v in format_seperate_value_dict.items() if v}
            format_value_dict = {k: v for k, v in format_value_dict.items() if v}
            format_seperate_value_dict, format_value_dict = fix_format(format_seperate_value_dict, format_value_dict)
            for format in format_seperate_value_dict.keys():
                seperated_value_list = format_seperate_value_dict[format]
                value_list = format_value_dict[format]
                token_format = row.TokenFormat
                token_list = [token_format.replace('<D>', value) for value in value_list]
                token_format = token_format.replace('<D>', format)
                new_row = [row.Template, row.ParameterIndex, token_format, token_list, seperated_value_list, row.ValueType]
                dynamic_format_df_info.append(new_row)
        else:
            new_row = [row.Template, row.ParameterIndex, row.TokenFormat, row.Tokens, row.ValueList, row.ValueType]
            dynamic_format_df_info.append(new_row)

    headers = ['Template', 'ParameterIndex', 'TokenFormat', 'Tokens', 'ValueList', 'ValueType']
    dynamic_format_df = pd.DataFrame(dynamic_format_df_info, columns=headers)
    return dynamic_format_df

# def value_format_generate(dynamic_based_updated_df, sample_ratio = 0.25, min_size = 5):
#     validated_format_memory = ValidatedFormat()
#     rejected_format_memory = RejectedFormat()
#     rejected_output_memory = RejectOutput()
#     format_extactor = FormatExtractor(validated_format_memory, rejected_format_memory, rejected_output_memory)
#     dynamic_format_df_info = []
#     index = 0
#     num = len(dynamic_based_updated_df)
#
#     for row in dynamic_based_updated_df.itertuples(index=False):
#         print((index/num)*100)
#         index=index+1
#         if row.ValueType == 'Undecided':
#             value_list = row.ValueList
#             print(value_list)
#             sampled_list = []
#             sample_size = int(len(value_list) * sample_ratio)
#             if sample_size < min_size:
#                 if len(value_list) < min_size:
#                     sampled_list = value_list
#                 else:
#                     sampled_list = random.sample(value_list, min_size)
#             else:
#                 sampled_list = random.sample(value_list, sample_size)
#             llm_output = format_extactor.extract(sampled_list, use_retry=1)
#             print(llm_output)
#             format_list = output_validate(llm_output)
#             while (format_list == None):
#                 rejected_output_memory.add_output(llm_output)
#                 llm_output = format_extactor.extract(sampled_list, use_retry=2)
#                 print(llm_output)
#                 format_list = output_validate(llm_output)
#             rejected_output_memory.clean_outputs()
#             format_list = llm_generated_format_repair(format_list)
#             unmatched_format_list, unmatched_value_list, format_value_dict, format_seperate_value_dict = match_value_with_format(
#                 value_list, format_list)
#             for format in unmatched_format_list:
#                 rejected_format_memory.add_format(format)
#             while (len(unmatched_value_list) > 0):
#                 llm_output = format_extactor.extract(unmatched_value_list, use_retry=0)
#                 print(llm_output)
#                 format_list = output_validate(llm_output)
#                 while (format_list == None):
#                     rejected_output_memory.add_output(llm_output)
#                     llm_output = format_extactor.extract(sampled_list, use_retry=2)
#                     print(llm_output)
#                     format_list = output_validate(llm_output)
#                 format_list = llm_generated_format_repair(format_list)
#                 unmatched_format_list, unmatched_value_list, format_value_dict_tmp, format_seperate_value_dict_tmp = match_value_with_format(
#                     unmatched_value_list, format_list)
#                 for format in unmatched_format_list:
#                     rejected_format_memory.add_format(format)
#                 format_value_dict.update(format_value_dict_tmp)
#                 format_seperate_value_dict.update(format_seperate_value_dict_tmp)
#             format_seperate_value_dict = {k: v for k, v in format_seperate_value_dict.items() if v}
#             format_value_dict = {k: v for k, v in format_value_dict.items() if v}
#             format_seperate_value_dict, format_value_dict = fix_format(format_seperate_value_dict, format_value_dict)
#             for format in format_seperate_value_dict.keys():
#                 seperated_value_list = format_seperate_value_dict[format]
#                 value_list = format_value_dict[format]
#                 token_format = row.TokenFormat
#                 token_list = [token_format.replace('<D>', value) for value in value_list]
#                 token_format = token_format.replace('<D>', format)
#                 new_row = [row.Template, row.ParameterIndex, token_format, token_list, seperated_value_list, row.ValueType]
#                 dynamic_format_df_info.append(new_row)
#         else:
#             new_row = [row.Template, row.ParameterIndex, row.TokenFormat, row.Tokens, row.ValueList, row.ValueType]
#             dynamic_format_df_info.append(new_row)
#
#     headers = ['Template', 'ParameterIndex', 'TokenFormat', 'Tokens', 'ValueList', 'ValueType']
#     dynamic_format_df = pd.DataFrame(dynamic_format_df_info, columns=headers)
#     return dynamic_format_df

class UnionFind:
    def __init__(self):
        self.parent = {}
        self.sub_info = defaultdict(list)

    def find(self, x):
        if x not in self.parent:
            self.parent[x] = x
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x, y):
        px, py = self.find(x), self.find(y)
        if px != py:
            self.parent[py] = px

def find_same_value_group(dynamic_based_updated_df):
    value_to_dynamic_ids = defaultdict(set)
    for row in dynamic_based_updated_df.itertuples(index=False):
        if row.ValueType == 'Value' or row.ValueType == 'Unit':
            pass
        else:
            for v in row.ValueList:
                value_to_dynamic_ids[v].add(row.DynamicID)

    uf = UnionFind()
    for dynamic_ids in value_to_dynamic_ids.values():
        dynamic_ids = list(dynamic_ids)
        first = dynamic_ids[0]
        for other in dynamic_ids[1:]:
            uf.union(first, other)

    components = defaultdict(list)
    for row in dynamic_based_updated_df.itertuples(index=False):
        root = uf.find(row.DynamicID)
        components[root].append(row.DynamicID)

    groups = []
    for group_id, dynamic_id_list in enumerate(components.values(), start=1):
        groups.append({
            "group_id": group_id,
            "dynamic_ids": dynamic_id_list
        })
    return groups

def update_dynamic_based_df_with_group_info(dynamic_based_updated_df, groups):
    dynamic_based_updated_df['GroupID'] = 0

    id_to_group = {}
    for group in groups:
        group_id = group["group_id"]
        for dynamic_id in group["dynamic_ids"]:
            id_to_group[dynamic_id] = group_id

    dynamic_based_updated_df['GroupID'] = (
        dynamic_based_updated_df['DynamicID'].map(id_to_group).fillna(0).astype(int)
    )
    return dynamic_based_updated_df

PUNCT = set("._:-/\\,;|[](){}<>@#=+?")
def match_with_punct_boundary(short_v: str, long_v: str, punct=PUNCT):
    if short_v.isdigit():
        return None
    start = long_v.find(short_v)
    if start == -1:
        return None
    end = start + len(short_v) - 1
    n = len(long_v)
    if start == 0:
        if end < n - 1 and long_v[end + 1] in punct:
            return (start, end)
        return None
    if end == n - 1:
        if long_v[start - 1] in punct:
            return (start, end)
        return None
    if long_v[start - 1] in punct and long_v[end + 1] in punct:
        return (start, end)
    return None

def find_contain_value_group(dynamic_based_updated_df):
    dynamic_id_values = {}
    for row in dynamic_based_updated_df.itertuples(index=False):
        if row.ValueType == 'Undecided':
            dynamic_id_values[row.DynamicID] = row.ValueList

    value_to_dynamic_ids = defaultdict(list)
    all_values = set()
    for dynamic_id, values in dynamic_id_values.items():
        for v in values:
            value_to_dynamic_ids[v].append(dynamic_id)
            all_values.add(v)

    sorted_values = sorted(all_values, key=len)
    uf = UnionFind()
    n = len(sorted_values)

    for i in range(n):
        v_short = sorted_values[i]
        for j in range(i+1, n):
            v_long = sorted_values[j]
            if len(v_long) < len(v_short):
                continue
            res = match_with_punct_boundary(v_short, v_long)
            if res:
                sub_info = v_long.replace(v_short, "<sub>")
                for dynamic_id_short in value_to_dynamic_ids[v_short]:
                    for dynamic_id_long in value_to_dynamic_ids[v_long]:
                        uf.union(dynamic_id_short, dynamic_id_long)
                        uf.sub_info[dynamic_id_long].append(sub_info)
            # if v_short in v_long:
            #     sub_info = v_long.replace(v_short, "<sub>")
            #     for dynamic_id_short in value_to_dynamic_ids[v_short]:
            #         for dynamic_id_long in value_to_dynamic_ids[v_long]:
            #             uf.union(dynamic_id_short, dynamic_id_long)
            #             uf.sub_info[dynamic_id_long].append(sub_info)

    components = defaultdict(lambda: {
        "dynamic_ids": [],
        "sub_info": defaultdict(list)
    })

    for row in dynamic_based_updated_df.itertuples(index=False):
        root = uf.find(row.DynamicID)
        components[root]["dynamic_ids"].append(row.DynamicID)

    for dynamic_id, sub_info in uf.sub_info.items():
        root = uf.find(dynamic_id)
        components[root]["sub_info"][dynamic_id].extend(sub_info)

    groups = []
    for group_id, comp in components.items():
        groups.append({
            "group_id": group_id,
            "dynamic_ids": comp["dynamic_ids"],
            "sub_info": dict(comp["sub_info"])
        })
    for group in groups:
        if len(group["dynamic_ids"]) > 1:
            print(group['dynamic_ids'])
            print(group['sub_info'])
            print('-----')
    return groups