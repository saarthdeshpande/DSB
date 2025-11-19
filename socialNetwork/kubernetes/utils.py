import re

def interval_string_to_seconds(input: str) -> int:
    SUFFIX_MULTIPLES = {
        'h': 60 * 60,
        'm': 60,
        's': 1
    }
    total = 0
    pattern = re.compile(r'(\d+)([hms])')
    for match in pattern.finditer(input):
        amount = int(match.group(1))
        suffix = match.group(2)
        multiple = SUFFIX_MULTIPLES[suffix]
        total += amount * multiple
    return total
