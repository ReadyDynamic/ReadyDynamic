from dataclasses import dataclass
from typing import List, Optional

import ast

class ValidationResult:
    ok: bool
    formats: Optional[List[str]] = None
    reason: Optional[str] = None
    level: Optional[int] = None
    unmatched_values: Optional[List[str]] = None

class FormatValidatorAgent:
    def validate(self, llm_output: str) -> ValidationResult:
        formats, error = self._validate_output_structure(llm_output)

        if error:
            return ValidationResult(ok=False, reason=error)
        return ValidationResult(ok=True, formats=formats)
    def _validate_output_structure(self, llm_output: str):
        try:
            parsed = ast.literal_eval(llm_output)
        except Exception:
            return None, "Output is not a valid Python Literal"

        if not isinstance(parsed, list):
            return None, "Output is not a list"
        # Do not know whether to use the following part
        # if len(parsed) == 0:
        #     return None, "Empty format list"
        #
        # for f in parsed:
        #     if not isinstance(f, str):
        #         return None, "Non-string format detected"
        #     if f.strip() == "":
        #         return None, "Empty format string detected"
        #
        # if len(set(parsed)) != len(parsed):
        #     return None, "Duplicate formats detected"
        return parsed, None

class Level2ValidationResult:
    ok: bool
    need_repair: bool
    reason: Optional[str] = None