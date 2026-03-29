import os

from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnableParallel, RunnablePassthrough

class FormatExtractor:
    def __init__(self, validatedformat, rejectedformat, rejectedoutput):
        self.validatedformat = validatedformat
        self.rejectedformat = rejectedformat
        self.rejectedoutput = rejectedoutput
        self.llm = ChatOpenAI(model="gpt-4o")
        self.first_prompt = PromptTemplate.from_template("""
You are a professional log dynamic-variable format extractor.

Given the following values for a dynamic variable:
{dynamic_values}
Please provide a list generalized format for the dynamic variable.

The following are the requirements for this task:
(1) The output of the prompt should only be a JSON-style Python list of format strings, like:: [\"format_1\", \"format_2\", ... , \"format_n\"]. Do not include ```json or ``` markers.
(2) Use as few formats as possible; ideally, one format should cover all values.
(3) All digit should be marked as <D>.
(4) Any URL, domain, or IP address should be treated as a single entity and represented as <D>. Examples: 
   - IP: "86.99.222.235" -> "<D>"
   - URL/host: "proxy.cse.cuhk.edu.hk", "ping3.teamviewer.com" -> "<D>"
(5) If a value contains a port number after a host (like "host:port" or "IP:port"), represent the host and port as "<D>:<D>".
(6) Any filesystem-like path (segments separated by /) should be treated as one entity and replaced with <D>.
(7) Any Java-style or package-style identifier made of segments separated by '.', '/', '_', '-' should also be treated as one entity and replaced with <D>.
(8) If such identifiers are combined with other tokens via separators like '@', ':', '=', '$' or '#', treat each part as a separate entity:
e.g., "org.apache.hadoop.yarn.util.WindowsBasedProcessTree@5d61bed0" → "<D>@<D>".
(9) Any MAC address in standard formats (colon-separated or dash-separated hex bytes) should be treated as a single entity and replaced with <D>. Examples:
    - "FF:F2:9F:16:BF:44:00:0D:60:E9:40:BB" -> "<D>"
(10) Do not create duplicate formats in the list.

Examples:
- ['65020ms', '48957ms'] -> ["<D>ms"]
- ["'attempt_1445087491445_0005_m_000000_0'", "'attempt_1445087491445_0005_m_000001_0'"] -> ["attempt_<D>_<D>_<D>_<D>_<D>"]
- ['183.62.156.108:22', '137.189.89.156:22', 'linux13.cse.cuhk.edu.hk:22'] -> ["<D>:<D>"]
- ['proxy.cse.cuhk.edu.hk', 'ping3.teamviewer.com'] -> ["<D>"]  
- ['org.apache.hadoop.mapreduce.v2.api.MRClientProtocolPB'] -> ["<D>"]
- ['org.apache.hadoop.yarn.util.WindowsBasedProcessTree@abcds'] -> ["<D>@<D>"]
- ['FF:F2:9F:16:BF:44:00:0D:60:E9:40:BB'] -> ["<D>"]
- ['R02-M1-N0-C'] -> ["<D>-<D>-<D>-<D>"]
- ['VPAUAggregateAudioDevice-0x7f9d41d6f640][VPAUAggregateAudioDevice-0x7f9d41d6f640]>'] -> ["VPAUAggregateAudioDevice-<D>][VPAUAggregateAudioDevice-<D>]>"]

Previously validated format for other dynamic variables are listed below:
{validated_format_info}
        """)

        self.retry_prompt = PromptTemplate.from_template("""
You are a professional log dynamic-variable format extractor.

Given the following values for a dynamic variable:
{dynamic_values}
Please provide a list generalized format for the dynamic variable.

The following are the requirements for this task:
(1) The output of the prompt should only be a JSON-style Python list of format strings, like:: [\"format_1\", \"format_2\", ... , \"format_n\"]. Do not include ```json or ``` markers.
(2) Use as few formats as possible; ideally, one format should cover all values.
(3) All digit should be marked as <D>.
(4) Any URL, domain, or IP address should be treated as a single entity and represented as <D>. Examples: 
   - IP: "86.99.222.235" -> "<D>"
   - URL/host: "proxy.cse.cuhk.edu.hk", "ping3.teamviewer.com" -> "<D>"
(5) If a value contains a port number after a host (like "host:port" or "IP:port"), represent the host and port as "<D>:<D>".
(6) Any filesystem-like path (segments separated by /) should be treated as one entity and replaced with <D>.
(7) Any Java-style or package-style identifier made of segments separated by '.', '/', '_', '-' should also be treated as one entity and replaced with <D>.
(8) If such identifiers are combined with other tokens via separators like '@', ':', '=', '$' or '#', treat each part as a separate entity:
e.g., "org.apache.hadoop.yarn.util.WindowsBasedProcessTree@5d61bed0" → "<D>@<D>".
(9) Any MAC address in standard formats (colon-separated or dash-separated hex bytes) should be treated as a single entity and replaced with <D>. Examples:
    - "FF:F2:9F:16:BF:44:00:0D:60:E9:40:BB" -> "<D>"
(10) Do not create duplicate formats in the list.
(11) If you are not certain, output ["<D>"].

Examples:
- ['65020ms', '48957ms'] -> ["<D>ms"]
- ["'attempt_1445087491445_0005_m_000000_0'", "'attempt_1445087491445_0005_m_000001_0'"] -> ["attempt_<D>_<D>_<D>_<D>_<D>"]
- ['183.62.156.108:22', '137.189.89.156:22', 'linux13.cse.cuhk.edu.hk:22'] -> ["<D>:<D>"]
- ['proxy.cse.cuhk.edu.hk', 'ping3.teamviewer.com'] -> ["<D>"]  
- ['org.apache.hadoop.mapreduce.v2.api.MRClientProtocolPB'] -> ["<D>"]
- ['org.apache.hadoop.yarn.util.WindowsBasedProcessTree@abcds'] -> ["<D>@<D>"]
- ['FF:F2:9F:16:BF:44:00:0D:60:E9:40:BB'] -> ["<D>"]
- ['2017-07-01_08,57,48.779252]-io80211Family-002.pcapng', '2017-07-01_08,57,48.782711]-CCIOReporter-002.xml'] -> ["<D>]-<D>"]
- ['v4(en0-:10.105.160.95', 'v4(en0:10.105.160.95'] -> ["<D>(<D>:<D>"]

This is not your first attempt. The following formats were previously rejected as invalid:
{rejected_formats}
You MUST NOT generate Any format identical to the rejected ones.
You MUST generate a format that is clearly distinct from all rejected formats.

Previously validated format for other dynamic variables are listed below:
{validated_format_info}
        """)

        self.format_prompt =  PromptTemplate.from_template("""
You are a professional log dynamic-variable format extractor.

Given the following values for a dynamic variable:
{dynamic_values}
Please provide a list generalized format for the dynamic variable.

The following are the requirements for this task:
(1) The output of the prompt should only be a JSON-style Python list of format strings, like:: [\"format_1\", \"format_2\", ... , \"format_n\"]. Do not include ```json or ``` markers.
(2) Use as few formats as possible; ideally, one format should cover all values.
(3) All digit should be marked as <D>.
(4) Any URL, domain, or IP address should be treated as a single entity and represented as <D>. Examples: 
   - IP: "86.99.222.235" -> "<D>"
   - URL/host: "proxy.cse.cuhk.edu.hk", "ping3.teamviewer.com" -> "<D>"
(5) If a value contains a port number after a host (like "host:port" or "IP:port"), represent the host and port as "<D>:<D>".
(6) Any filesystem-like path (segments separated by /) should be treated as one entity and replaced with <D>.
(7) Any Java-style or package-style identifier made of segments separated by '.', '/', '_', '-' should also be treated as one entity and replaced with <D>.
(8) If such identifiers are combined with other tokens via separators like '@', ':', '=', '$' or '#', treat each part as a separate entity:
e.g., "org.apache.hadoop.yarn.util.WindowsBasedProcessTree@5d61bed0" → "<D>@<D>".
(9) Any MAC address in standard formats (colon-separated or dash-separated hex bytes) should be treated as a single entity and replaced with <D>. Examples:
    - "FF:F2:9F:16:BF:44:00:0D:60:E9:40:BB" -> "<D>"
(10) Do not create duplicate formats in the list.
(11) If you are not certain, output ["<D>"].

Examples:
- ['65020ms', '48957ms'] -> ["<D>ms"]
- ["'attempt_1445087491445_0005_m_000000_0'", "'attempt_1445087491445_0005_m_000001_0'"] -> ["attempt_<D>_<D>_<D>_<D>_<D>"]
- ['183.62.156.108:22', '137.189.89.156:22', 'linux13.cse.cuhk.edu.hk:22'] -> ["<D>:<D>"]
- ['proxy.cse.cuhk.edu.hk', 'ping3.teamviewer.com'] -> ["<D>"]  
- ['org.apache.hadoop.mapreduce.v2.api.MRClientProtocolPB'] -> ["<D>"]
- ['org.apache.hadoop.yarn.util.WindowsBasedProcessTree@abcds'] -> ["<D>@<D>"]
- ['FF:F2:9F:16:BF:44:00:0D:60:E9:40:BB'] -> ["<D>"]

The output of the prompt should only be a JSON-style Python list of format strings, like: [\"format_1\", \"format_2\", ... , \"format_n\"]. Do not include ```json or ``` markers.
The generated output is wrong in format, the prior generated output is:
{prior_output}
    """)

    def extract(self, value_list, use_retry):
        if use_retry == 0:
            prompt = self.retry_prompt
            rejected_formats = self.rejectedformat.get_context()
            validated_format_info = self.validatedformat.get_context()
            payload = {
                "dynamic_values": value_list,
                "rejected_formats": rejected_formats,
                "validated_format_info": validated_format_info
            }
        elif use_retry == 1:
            prompt = self.first_prompt
            validated_format_info = self.validatedformat.get_context()
            payload = {
                "dynamic_values": value_list,
                "validated_format_info": validated_format_info
            }
        elif use_retry == 2:
            prompt = self.format_prompt
            rejected_llm_output = self.rejectedoutput.get_context()
            payload = {
                "dynamic_values": value_list,
                "prior_output": rejected_llm_output
            }

        chain = prompt | self.llm
        result = chain.invoke(payload)

        return result.content