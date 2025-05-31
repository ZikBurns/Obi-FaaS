import json
import numpy as np
import json
import os
import pandas as pd
import numpy as np


class OptimizerCPR:
    def __init__(self, coef_json_path, price_per_second):
        with open(coef_json_path, 'r') as f:
            coefs = json.load(f)
        self.init_coeficient_f = coefs['init']['coef']            # alpha_1
        self.init_intercept = coefs['init']['intercept']          # alpha_0
        self.acc_coeficient_i = coefs['acc']['coef_i']            # beta_2
        self.acc_coeficient_f = coefs['acc']['coef_f']            # beta_1
        self.acc_intercept = coefs['acc']['intercept']            # beta_3
        self.price_per_second = price_per_second

    def predict_cpr(self, num_functions, num_images):
        log_num_images = np.log(num_images)
        log_num_functions = np.log(num_functions)
        log_term_a = log_num_images * self.acc_coeficient_i + log_num_functions * self.acc_coeficient_f + self.acc_intercept
        term_a = np.exp(log_term_a)
        log_num_functions = np.log(num_functions)
        log_term_i = log_num_functions * self.init_coeficient_f + self.init_intercept
        term_i = np.exp(log_term_i)
        total = term_a + term_i
        cpr = total * self.price_per_second / num_images
        return cpr

    def predict_num_functions(self, target_cpr, num_images, batch_size, min_functions=1, max_functions=1000, epsilon=0.05):
        low = min_functions
        high = min(max_functions, num_images / batch_size)
        high = int(high) if high == int(high) else int(high) + 1
        num_workers = -1

        while low <= high:
            w = (low + high) // 2
            predicted_cpr = self.predict_cpr(w, num_images)

            lower_bound = target_cpr * (1 - epsilon)
            upper_bound = target_cpr

            if lower_bound < predicted_cpr <= upper_bound:
                num_workers = w
                break
            elif predicted_cpr > upper_bound:
                high = w - 1
            else:
                low = w + 1


        num_workers = 1 if num_workers < 1 else num_workers
        return num_workers


def sample_grid_search(
    max_num_requests=20001,
    batch_size=32,
    step_requests=1,
    step_num_functions=1,
    n_samples=30
):
    combinations = []
    for num_requests in range(0, max_num_requests, step_requests):
        if num_requests == 0:
            num_requests = 1
        max_functions = num_requests // batch_size
        for num_functions in range(0, max_functions + 1, step_num_functions):
            if num_functions == 0:
                num_functions = 1
            combinations.append((num_requests, num_functions))
    df = pd.DataFrame(combinations, columns=['num_requests', 'num_functions'])
    df = df.sort_values(by=['num_requests', 'num_functions']).reset_index(drop=True)
    indices = np.linspace(0, len(df) - 1, num=n_samples, dtype=int)
    sampled_combinations = df.iloc[indices]
    requests = sampled_combinations['num_requests'].values[:-1]
    functions = sampled_combinations['num_functions'].values[:-1]
    return requests, functions


def sample_grid_search_logspace(
        max_num_requests=20001,
        batch_size=32,
        step_requests=1,
        step_num_functions=1,
        n_samples=30
):
    combinations = []
    for num_requests in range(0, max_num_requests, step_requests):
        if num_requests == 0:
            num_requests = 1
        max_functions = num_requests // batch_size
        for num_functions in range(0, max_functions + 1, step_num_functions):
            if num_functions == 0:
                num_functions = 1
            combinations.append((num_requests, num_functions))
    df = pd.DataFrame(combinations, columns=['num_requests', 'num_functions'])
    df = df.sort_values(by=['num_requests', 'num_functions']).reset_index(drop=True)

    # Logarithmic sampling of indices
    log_indices = np.logspace(0, np.log10(len(df) - 1), num=n_samples, dtype=int)
    log_indices = np.unique(log_indices)
    while len(log_indices) < n_samples:
        log_indices = np.append(log_indices, len(df) - 1)
    log_indices = log_indices[:n_samples]  # Just in case we over-shoot

    sampled_combinations = df.iloc[log_indices]
    requests = sampled_combinations['num_requests'].values[:-1]
    functions = sampled_combinations['num_functions'].values[:-1]
    return requests, functions


def read_result_files(base_dir, max_dir=100):
    """
    Reads JSON files from subdirectories named 0..max_dir (inclusive),
    in numeric order, and appends their contents to faas_executions.
    """
    faas_executions = []
    for i in range(max_dir + 1):
        dir_path = os.path.join(base_dir, str(i))
        if not os.path.isdir(dir_path):
            # Skip missing or non-directory entries
            continue

        # List all .json files in this directory, sorted lexically (or numerically if names allow)
        json_files = sorted(
            [f for f in os.listdir(dir_path) if f.endswith('.json')]
        )
        for fname in json_files:
            file_path = os.path.join(dir_path, fname)
            print(f"Reading file: {file_path}")
            with open(file_path, 'r') as f:
                data = json.load(f)
            faas_executions.append(data)

    return faas_executions


def calculate_price_per_function(functions, price_per_1ms):
    billed_durations = []
    durations = []
    init_durations = []
    for enu, function in enumerate(functions):
        try:
            log_message = function['log_message']
            # Find Billed Duration in log message
            billed_duration = int(log_message.split('Billed Duration: ')[1].split(' ms')[0])
            billed_durations.append(int(billed_duration))
            duration = float(log_message.split('Duration: ')[1].split(' ms')[0])
            durations.append(duration)
            if "Init Duration" in log_message:
                init_duration = float(log_message.split('Init Duration: ')[1].split(' ms')[0])
            else:
                init_duration = 0
            init_durations.append(init_duration)
        except Exception as e:
            print(f"Error in function {enu}: {e}")
            billed_durations.append(0)
            durations.append(0)
            init_durations.append(0)

    duration_acc = sum(durations) / 1000
    init_duration_acc = sum(init_durations) / 1000
    billed_price = sum(billed_durations) * price_per_1ms
    total_billed_duration = sum(billed_durations) / 1000
    return billed_price, total_billed_duration, duration_acc, init_duration_acc


def generate_latency_costs(faas_executions, cost_per_1ms_1mb = 0.0000000000163, memory=1769):
    price_per_1ms = cost_per_1ms_1mb * memory
    summary = []
    for faas_execution in faas_executions:
        num_task_managers = faas_execution['num_task_managers']
        try:
            lithops_stats = faas_execution['output']['lithops_stats']
        except Exception as e:
            lithops_stats = faas_execution['invoke_output']['lithops_stats']
        billed_price, billed_duration, duration_acc, init_duration_acc = calculate_price_per_function(lithops_stats, price_per_1ms)
        start_times = [function['host_submit_tstamp'] for function in lithops_stats]
        end_times = [function['host_result_done_tstamp'] for function in lithops_stats]
        first_start = min(start_times)
        last_end = max(end_times)
        total_time = last_end - first_start
        num_images = len(faas_execution['input'])
        summary.append({
            'num_task_managers': num_task_managers,
            "num_images": num_images,
            "split_size": faas_execution["split_size"],
            "cost": billed_price,
            "latency": total_time,
            "throughput": num_images / total_time,
            "throughput/$": (num_images / total_time) / billed_price,
            "duration_billed": billed_duration,
            "duration_function": duration_acc,
            "duration_init": init_duration_acc,
        })
    return summary

def best_task_manager(file_best_worker):
    """
    Reads the best worker configuration from a JSON file.
    """
    with open(file_best_worker, 'r') as f:
        best_worker = json.load(f)
    memory = best_worker['memory']
    batch_size = best_worker['batch_size']
    interop = best_worker['interop']
    intraop = best_worker['intraop']
    forks = best_worker['forks']

    task_manager_config = {
        'load': {'batch_size': batch_size, 'max_concurrency': batch_size},
        'preprocess': {'batch_size': batch_size, 'num_cpus': 2},
        'predict': {'batch_size': batch_size, 'interop': interop, 'intraop': intraop, 'n_models': forks}
    }
    return memory, task_manager_config