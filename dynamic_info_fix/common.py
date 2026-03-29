import re
from collections import defaultdict

punc_list = [',', '.', ':', ';', ')', '(', '[', ']', '{', '}', '\'', '/', '\"', '<', '>']
# punc_list = [',', ':', ';', ')', '(', '[', ']', '{', '}', '<', '>', '\'', '/', '\"']
unit_list = ['kb', 'mb', 'gb', 'ms']
common_tlds = ['.com', '.net', '.cn', '.hk', '.edu', '.io']

ip_pattern = re.compile(r"^\d{1,3}(\.\d{1,3}){3}$")
domain_pattern = re.compile(r"^[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*$")

def preprocess_punc(value_list):
    new_value_list = []
    for value in value_list:
        new_value_list.append(value)

    value_rep = new_value_list[0]
    endwithpunc = True
    beginwithpunc = True
    end_punc_list = []
    begin_punc_list = []

    while(endwithpunc or beginwithpunc):
        endwithpunc = False
        beginwithpunc = False
        for punc in punc_list:
            if value_rep.endswith(punc):
                endwithpunc = True
                value_rep = value_rep[:-1]
                end_punc_list.insert(0,punc)
                for i, value in enumerate(new_value_list):
                    new_value_list[i] = value[:-1]
                break
        for punc in punc_list:
            if value_rep.startswith(punc):
                beginwithpunc = True
                value_rep = value_rep[1:]
                begin_punc_list.insert(0,punc)
                for i, value in enumerate(new_value_list):
                    new_value_list[i] = value[1:]
                break

    token_format = '<C>'
    for punc in end_punc_list:
        token_format = token_format + punc
    for punc in begin_punc_list:
        token_format = punc + token_format

    return token_format, new_value_list

def preprocess_punc_unit(token):
    endwithpunc = True
    beginwithpunc = True
    unit_token = token

    while (endwithpunc or beginwithpunc):
        endwithpunc = False
        beginwithpunc = False
        for punc in punc_list:
            if unit_token.endswith(punc):
                endwithpunc = True
                unit_token = token[:-1]
                break
        for punc in punc_list:
            if unit_token.startswith(punc):
                beginwithpunc = True
                unit_token = unit_token[1:]
                break
    return unit_token

def check_unit(token):
    token = preprocess_punc_unit(token)
    check_token = token.lower()
    for unit in unit_list:
        if check_token == unit:
            return unit
    return 'None'

# def value_type_verify(value_list):
#     verified_list = []
#     unmatched_list = []
#
#     hex_pattern = re.compile(r'[+-]?0[xX][0-9a-fA-F]+')
#
#     for value in value_list:
#         if hex_pattern.fullmatch(value):
#             verified_list.append(value)
#             continue
#         try:
#             float(value)  # 可处理 int, float, "3.14", "-2", "1e-3"
#             verified_list.append(value)
#         except (ValueError, TypeError):
#             unmatched_list.append(value)
#
#     return verified_list, unmatched_list

def value_type_verify(value_list, special_var_list):
    hex_pattern = re.compile(r'[+-]?0[xX][0-9a-fA-F]+')
    for value in value_list:
        if value in special_var_list:
            continue
        if hex_pattern.fullmatch(value):
            continue
        try:
            float(value)  # 可处理 int, float, "3.14", "-2", "1e-3"
        except (ValueError, TypeError):
            return False
    return True

def is_ip(host):
    if not ip_pattern.match(host):
        return False
    parts = host.split('.')
    return all(0 <= int(p) <= 255 for p in parts)

def is_domain(host):
    if domain_pattern.match(host):
        if '.' in host:
            return True
        for t in common_tlds:
            if host.endswith(t):
                return True
    return False

_MACRO_RE = re.compile(r'^[A-Z]+(?:_[A-Z]+)*$')
def is_macro_value(v: str) -> bool:
    if not isinstance(v, str):
        return False
    return bool(_MACRO_RE.fullmatch(v))

def macro_type_verify(value_list) -> bool:
    if not value_list:
        return False
    for v in value_list:
        if not is_macro_value(v):
            return False
    return True

# def host_type_verify(value_list):
#     verified_list = []
#     unmatched_list = []
#     for value in value_list:
#         if ':' not in value:
#             unmatched_list.append(value)
#             continue
#         host, port =  value.rsplit(':', 1)
#         if not port.isdigit():
#             unmatched_list.append(value)
#             continue
#         if not (is_ip(host) or is_domain(host)):
#             unmatched_list.append(value)
#             continue
#         verified_list.append(value)
#     return verified_list, unmatched_list

def host_type_verify(value_list):
    flag = True
    for value in value_list:
        if ':' not in value:
            flag = False
            break
        host, port =  value.rsplit(':', 1)
        if not port.isdigit():
            flag = False
            break
        if not (is_ip(host) or is_domain(host)):
            flag = False
            break
    return flag

# def unit_type_verify(value_list):
#     verified_list = []
#     unmatched_list = []
#     for value in value_list:
#         value_check = value.lower()
#         if value_check in unit_list:
#             verified_list.append(value)
#         else:
#             unmatched_list.append(value)
#     return verified_list, unmatched_list

def unit_type_verify(value_list):
    flag = True
    for value in value_list:
        value_check = value.lower()
        if value_check in unit_list:
            pass
        else:
            flag = False
            break
    return flag

# def path_type_verify(value_list, strict_level = 2):
#     verified_list = []
#     unmatched_list = []
#     for value in value_list:
#         if not (re.match(r"^[A-Za-z]:[\\/]", value) or
#                 (value.count('/') >= strict_level or value.count('\\\\') >= strict_level)
#         ):
#             unmatched_list.append(value)
#             continue
#         verified_list.append(value)
#     return verified_list, unmatched_list

def path_type_verify(value_list, strict_level = 2):
    flag = True
    for value in value_list:
        if not (re.match(r"^[A-Za-z]:[\\/]", value) or
                (value.count('/') >= strict_level or value.count('\\\\') >= strict_level)
        ):
            flag = False
            break
    return flag

def pattern_to_regex(pattern):
    regex = pattern
    regex = regex.replace("\\", "\\\\")
    regex = regex.replace(".", "\.")
    regex = regex.replace("*", "\*")

    regex = regex.replace("(", "\(")
    regex = regex.replace(")", "\)")

    regex = regex.replace("<D>", "(.*)")

    regex = regex.replace("[", "\[")
    regex = regex.replace("]", "\]")

    regex = regex.replace("|", "\|")

    regex = regex.replace("+", "\+")
    regex = regex.replace("?", "\?")
    regex = regex.replace("$", "\$")
    regex = regex.replace("@", "\@")
    regex = regex.replace("^", "\^")

    # regex = regex.replace(":", "\:")
    # regex = regex.replace("\"", "\\\"")

    regex = regex + '$'

    return regex

def ip_format_verify(format):
    ip_pattern = re.compile(r'(<D>\.){3}<D>')
    if ip_pattern.fullmatch(format):
        return True
    return False

def path_format_verify(format, strict_level=2):
    if (re.match(r"^[A-Za-z]:[\\/]", format) or
            (format.count('/') >= strict_level or format.count('\\\\') >= strict_level)
    ):
        return True
    return False

DOT_DIGIT_END_REGEX = re.compile(
    r'^(\(?[A-Za-z]+\)?)(\.{3,})(\d+)$'
)
def suffix_dot_digit_verify(value_list):
    if not value_list:
        return {}

    formats = defaultdict(list)

    for v in value_list:
        m = DOT_DIGIT_END_REGEX.match(v)
        if not m:
            return {}

        words = m.group(1)
        dots = m.group(2)
        digits = m.group(3)

        pattern = f"{words}{dots}<D>"
        formats[pattern].append(digits)

    return dict(formats)