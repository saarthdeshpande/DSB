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

def parse_hpa_output(parts, metric_names, version):
    metric_values = []
    multiple_metric_values = {}
    thresholds = []

    if version == "old":
        if len(parts) == 7: # single metric
            metric_match = re.search(r'(\d+)%/(\d+)%|(<unknown>)/(\d+)%', parts[2])
            if metric_match:
                metric_value = metric_match.group(1) or metric_match.group(3)
                metric_value = int(metric_value) if metric_value and metric_value.isdigit() else None
                if len(thresholds) == 0:
                    threshold = metric_match.group(2) or metric_match.group(4)
                    if len(thresholds) == 0:
                        thresholds = [int(threshold)] if threshold else []
            else:
                metric_value = None
            metric_values.append(metric_value)

        else:
            n_metrics = len(parts) - 6
            thresholds = [None] * n_metrics
            for metric_number in range(n_metrics):
                metric_match = re.search(r'(\d+)%/(\d+)%|(<unknown>)/(\d+)%', parts[2 + metric_number])

                if metric_match:
                    metric_value = metric_match.group(1) or metric_match.group(3)
                    metric_value = int(metric_value) if metric_value and metric_value.isdigit() else None
                    if thresholds[n_metrics - 1] is None:
                        threshold = metric_match.group(2) or metric_match.group(4)
                        thresholds[metric_number] = int(threshold) if threshold else None
                else:
                    metric_value = None
                if metric_names[metric_number] in multiple_metric_values.keys():
                    multiple_metric_values[metric_names[metric_number]].append(metric_value)
                else:
                    multiple_metric_values[metric_names[metric_number]] = [metric_value]
    elif version == "new":
        n_metrics = (len(parts) - 6) // 2  # Calculate metrics based on new structure
        thresholds = [None] * n_metrics

        for metric_number in range(n_metrics):
            # Extract metric name and value from paired elements
            name_index = 2 + (2 * metric_number)
            value_index = 3 + (2 * metric_number)

            metric_name = parts[name_index].rstrip(':')  # Remove trailing colon
            value_part = parts[value_index]

            # Pattern matching for metric values
            metric_match = re.search(r'(\d+)%/(\d+)%|(<unknown>)/(\d+)%', value_part)
            if metric_match:
                metric_value = metric_match.group(1) or metric_match.group(3)
                metric_value = int(metric_value) if metric_value and metric_value.isdigit() else None
                threshold = metric_match.group(2) or metric_match.group(4)
                thresholds[metric_number] = int(threshold) if threshold else None
            else:
                metric_value = None

            metric_names.append(metric_name)
            metric_values.append(metric_value)
            # Store in metric dictionary
            if metric_name in multiple_metric_values:
                multiple_metric_values[metric_name].append(metric_value)
            else:
                multiple_metric_values[metric_name] = [metric_value]

    replica = int(parts[len(parts) - 2])
    timestamp = parts[len(parts) - 1]

    return metric_values, multiple_metric_values, thresholds, replica, timestamp

