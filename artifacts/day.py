import json

import pandas as pd
import logging
from obifaas.obifaas import Obi_FaaS, Job
import os
import time
from obifaas.optimizer import OptimizerCPR,best_task_manager, read_result_files, generate_latency_costs


memory, task_manager_config = best_task_manager('optimizer_model/best_worker.json')
BATCH_SIZE = task_manager_config['predict']['batch_size']
with open(f"../datasets/imagenet_keys/10kds.txt") as f:
    urls_general = f.read().splitlines()

df = pd.read_csv('../datasets/day/day.csv')
df['STARTED'] = pd.to_datetime(df['STARTED'])
df = df.sort_values(by='STARTED')

obi = Obi_FaaS(fexec_args={'runtime': f'obifaas_311_{memory}', 'runtime_memory': memory},
                        ec2_host_machine=True, max_task_managers=1000,
                        initialize=False, logging_level=logging.INFO)

CPR_1M = 8
MAX_WORKERS = 1000
cpr_predictor = OptimizerCPR(coef_json_path='optimizer_model/cpr_model.json', price_per_second=0.0000000500 * 1000)

def run_job(index, ds_id, annots):
    w = cpr_predictor.predict_num_functions(target_cpr=CPR_1M / 1_000_000,
                                            num_images=annots,
                                            batch_size=BATCH_SIZE,
                                            min_functions=1,
                                            max_functions=MAX_WORKERS,
                                            epsilon=0.05)

    urls = urls_general[:annots]
    job = Job(input=urls, job_name=f'{index}_{ds_id}', orchestrator_backend="aws_lambda", dynamic_batch=True,
              num_task_managers=w, split_size=BATCH_SIZE, output_storage="local",
              output_location=f"day_results/{index}/{ds_id}", speculation_enabled=True,
              max_split_retries=3, speculation_add_task_manager=False,
              task_manager_config=task_manager_config)


    start_timestamp = time.time()
    print(f"Job {index} started")

    obi.run_job(job)

    print(f"Job {index} finished")
    end_timestamp = time.time()
    stats = {
        'start_timestamp': start_timestamp,
        'end_timestamp': end_timestamp,
    }
    return stats


MAX_WAITING_TIME = 600
last_end_job = df.iloc[0]['STARTED']

for index, row in df.iterrows():
    ds_id = row['ds_id']
    annots = row['n_images_ds']
    if not os.path.exists(f"traces/{index}/"):
        os.makedirs(f"traces/{index}/")

    start_time = row['STARTED']
    wait_time = (start_time - last_end_job).total_seconds()
    if wait_time > 0:
        if wait_time < MAX_WAITING_TIME:
            print(f"Waiting for {wait_time} seconds before starting the next job at {start_time}.")
            time.sleep(wait_time)
        else:
            print(f"Waiting for {MAX_WAITING_TIME} seconds before starting the next job at {start_time}.")
            time.sleep(MAX_WAITING_TIME)


    stats = run_job(index, ds_id, annots)

    if stats:
        duration = stats['end_timestamp'] - stats['start_timestamp']
        last_end_job = start_time + pd.Timedelta(seconds=duration)

faas_executions = read_result_files('day_results')
summary = generate_latency_costs(faas_executions, memory = memory)

with open('day_summary.json', 'w') as f:
    json.dump(summary, f, indent=4)
