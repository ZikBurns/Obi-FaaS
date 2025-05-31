import json
import logging
import sys
import time
from obifaas.obifaas import Obi_FaaS, Job
import numpy as np
from obifaas.optimizer import sample_grid_search, sample_grid_search_logspace, read_result_files, generate_latency_costs, best_task_manager

requests_1, functions_1 = sample_grid_search(max_num_requests=1000, batch_size=32,step_requests=1,step_num_functions=1,n_samples=30)
requests_2, functions_2 = sample_grid_search_logspace(max_num_requests=1000, batch_size=32,step_requests=1,step_num_functions=1,n_samples=30)
DATASET_SIZES = np.concatenate((requests_1, requests_2))
TASK_MANAGER_LIST = np.concatenate((functions_1, functions_2))
TASK_MANAGER_LIST = TASK_MANAGER_LIST.astype(int).tolist()
print(DATASET_SIZES)
print(TASK_MANAGER_LIST)

def no_policy(job):
    return job

memory, task_manager_config = best_task_manager('optimizer_model/best_worker.json')
BATCH_SIZE = task_manager_config['predict']['batch_size']
obi = Obi_FaaS(fexec_args ={'runtime': f'obifaas_311_{memory}', 'runtime_memory': memory},
                        ec2_host_machine=False, job_policy=no_policy, max_task_managers=1000,
                        initialize=False, logging_level=logging.INFO)
dataset = "10kds"
REPETITIONS = 4
with open(f"../datasets/imagenet_keys/{dataset}.txt") as f:
    urls_general = f.read().splitlines()

for dataset_size, task_managers in zip(DATASET_SIZES,TASK_MANAGER_LIST):
    urls = urls_general[:dataset_size]
    date_time = time.strftime("%Y-%m-%d_%Hh%Mm%Ss", time.localtime())
    job = Job(input=urls, job_name=f'{dataset}_{date_time}', orchestrator_backend="aws_lambda", dynamic_batch=True,
              num_task_managers=task_managers, split_size=BATCH_SIZE, output_storage="local",
              output_location=f"results_cpr/ds_{dataset_size}/tm_{task_managers}/{date_time}", speculation_enabled=True,
              max_split_retries=3, speculation_add_task_manager=True,
              task_manager_config=task_manager_config)
    result =obi.run_job(job)
    time.sleep(720)

faas_executions = read_result_files('results_cpr')
summary = generate_latency_costs(faas_executions, memory = memory)

with open('summary.json', 'w') as f:
    json.dump(summary, f, indent=4)


