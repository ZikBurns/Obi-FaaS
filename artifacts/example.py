import logging
import time
from obifaas.obifaas import Obi_FaaS, Job
from obifaas.optimizer import OptimizerCPR,best_task_manager

# Read best_worker.json
memory, task_manager_config = best_task_manager('optimizer_model/best_worker.json')
BATCH_SIZE = task_manager_config['predict']['batch_size']
DATASET_SIZE = 32
CPR_1M = 8
MAX_WORKERS = 1000
cpr_predictor = OptimizerCPR(coef_json_path='optimizer_model/cpr_model.json', price_per_second=0.0000000500 * 1000)
w = cpr_predictor.predict_num_functions(target_cpr = CPR_1M/1_000_000,
                                        num_images = DATASET_SIZE,
                                        batch_size = BATCH_SIZE,
                                        min_functions=1,
                                        max_functions=MAX_WORKERS,
                                        epsilon=0.05)

obi = Obi_FaaS(fexec_args ={'runtime': f'obifaas_311_{memory}', 'runtime_memory': memory},
                        ec2_host_machine=True, max_task_managers=MAX_WORKERS,
                        initialize=False, logging_level=logging.INFO)

with open(f"../datasets/imagenet_keys/10kds.txt") as f:
    urls_general = f.read().splitlines()
    urls = urls_general[:DATASET_SIZE]

date_time = time.strftime("%Y-%m-%d_%Hh%Mm%Ss", time.localtime())
job = Job(input=urls, job_name=f'{memory}_{date_time}', orchestrator_backend="aws_lambda", dynamic_batch=True,
          split_size=BATCH_SIZE, output_storage="local", num_task_managers=w,
          output_location=f"results/ds_{DATASET_SIZE}/{date_time}", speculation_enabled=True,
          max_split_retries=3, speculation_add_task_manager=False,
          task_manager_config=task_manager_config)
result =obi.run_job(job)

