class ValidatedFormat():
    def __init__(self):
        self.format_value_dict = {}
        self.format_info_list = []

    def generate_format_key(self, format_list):
        return hash(','.join(format_list))

    def add_format(self, format_list, value_list):
        self.format_info_list.append(format_list)
        format_key = self.generate_format_key(format_list)
        self.format_value_dict[format_key] = value_list

    def get_context(self):
        context = ''
        for format_list in self.format_info_list:
            format_key = self.generate_format_key(format_list)
            value_list = self.format_value_dict[format_key]
            context = context + ' '.join(format_list) + ' : ' + ' '.join(value_list) + '\n'
        return context

class RejectedFormat():
    def __init__(self):
        self.rejected_formats = []

    def add_format(self, format):
        if format not in self.rejected_formats:
            self.rejected_formats.append(format)

    def clean_formats(self):
        self.rejected_formats.clear()

    def get_context(self):
        context = ''
        for format in self.rejected_formats:
            context = context + format + '\n'
        return context

class RejectOutput():
    def __init__(self):
        self.rejected_output = []

    def add_output(self, output):
        self.rejected_output.append(output)

    def clean_outputs(self):
        self.rejected_output.clear()

    def get_context(self):
        context = ''
        for output in self.rejected_output:
            context = context + output + '\n'
        return context