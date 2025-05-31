import json
import logging
import sys
import time
from obifaas.optimizer import generate_latency_costs
from obifaas.obifaas import Obi_FaaS, Job
import optuna
import pandas as pd
from optuna.visualization import plot_optimization_history

class BayesianOptimization:
    """
    A class for performing Bayesian Optimization on a given DataFrame.
    Attributes:
        df (pd.DataFrame): The DataFrame containing the data to optimize.
        objective (str): The objective function to optimize. Can be "Cost/1MInputs", "Throughput", "Throughput/$", or "Energy/1MInputs".
        param_spaces (dict): The parameter spaces for optimization.
        n_trials (int): The number of trials for optimization.
        seed (int): Random seed for reproducibility.
        time_taken (list): List to store time taken for each trial.
        cost_acc (list): List to store cost accumulated for each trial.
        target_value (float): The target value for the objective function.
    """
    def __init__(self, param_spaces, inputs, objective="Throughput/$", direction='maximize' , sampler=None,
                 n_trials=100, seed=500, verbose=True,  task_manager_config=None):
        self.objective = objective
        self.param_spaces = param_spaces
        self.n_trials = n_trials
        self.seed = seed
        self.time_taken = []
        self.cost_acc = []
        self.energy_list = []
        self.emissions_list = []
        self.verbose = verbose
        self.task_manager_config = task_manager_config
        self.inputs = inputs

        if sampler is None:
            self.sampler = optuna.samplers.TPESampler(seed=self.seed)
        else:
            self.sampler = sampler

        self.direction = direction

        # Calculate total combinations in param_spaces
        self.total_combinations = self._calculate_total_combinations()

        self.trial_count = 0

    def _calculate_total_combinations(self):
        total_combinations = 0
        for memory, space in self.param_spaces.items():
            valid_space = 1
            for key, value in space.items():
                if isinstance(value, list):
                    valid_space *= len(value)
                elif isinstance(value, tuple):
                    valid_space *= value[1]
            total_combinations += valid_space
        return total_combinations

    def objective_function(self, trial):
        # Choose memory first (root parameter)
        memory = trial.suggest_categorical("memory", list(self.param_spaces.keys()))

        # Get valid space for chosen memory
        space = self.param_spaces[memory]

        # Sample only from valid values
        batch_size = trial.suggest_categorical(f"Batch Size (memory={memory}mb)", space["Batch Size"])
        interop = trial.suggest_int(f"Interop (memory={memory}mb)", *space["Interop"])
        intraop = trial.suggest_int(f"Intraop (memory={memory}mb)", *space["Intraop"])
        forks = trial.suggest_int(f"Forks (memory={memory}mb)", *space["Forks"])

        TASK_MANAGER_CONFIG = {
            'load': {'batch_size': batch_size, 'max_concurrency': 32},
            'preprocess': {'batch_size': batch_size, 'num_cpus': 2},
            'predict': {'batch_size': batch_size, 'interop': interop, 'intraop': intraop, 'n_models': forks}
        }
        obi = Obi_FaaS(fexec_args={'runtime': f'obifaas_311_{memory}', 'runtime_memory': memory},
                                ec2_host_machine=False, job_policy=no_policy, max_task_managers=1000,
                                initialize=False, logging_level=logging.INFO)

        # Filter valid data points
        urls = self.inputs[:batch_size]
        date_time = time.strftime("%Y-%m-%d_%Hh%Mm%Ss", time.localtime())
        job = Job(input=urls, job_name=f'{date_time}', orchestrator_backend="aws_lambda",
                  dynamic_batch=False,
                  num_task_managers=1, split_size=batch_size, output_storage="local",
                  output_location=f"results/bayesian_optimization/{self.trial_count}/{date_time}", speculation_enabled=False,
                  task_manager_config=TASK_MANAGER_CONFIG)
        result = obi.run_job(job)


        summary = generate_latency_costs([result], cost_per_1ms_1mb = 0.0000000000163, memory=memory)[0]
        latency = summary['latency']
        cost = summary['cost']
        throughput_per_dollar = summary['throughput/$']
        self.time_taken.append(latency)
        self.cost_acc.append(cost)
        self.trial_count += 1
        return throughput_per_dollar

    def optimize(self):
        # Run Bayesian Optimization
        if not self.verbose:
            optuna.logging.set_verbosity(optuna.logging.WARNING)  # Set logging to WARNING to suppress info logs
        else:
            optuna.logging.set_verbosity(optuna.logging.INFO)  # Default verbosity level

        study = optuna.create_study(direction=self.direction, sampler=self.sampler)
        study.optimize(self.objective_function, n_trials=self.n_trials)



        total_time = sum(self.time_taken)
        total_cost = sum(self.cost_acc)

        result = {
            "best_params": study.best_params,
            "best_value": study.best_value,
            "total_time": total_time,
            "total_cost": total_cost,
        }

        return study, result

    def plot_optimization_history(self, study, plot_file = 'optimizer_model/optimization_history.png'):
        plot = plot_optimization_history(study)
        plot.show()

        # Save the plot to a file
        if plot_file:
            plot.write_image(plot_file)


    def save_results(self, study, filename='optimizer_model/best_worker.json'):
        """
        Save the optimization results, including the best memory, batch size,
        interop, intraop, and forks, to a JSON file.
        """
        best_params = study.best_params

        # Extract the memory value
        best_memory = best_params.get("memory")

        # The keys for batch size, interop, intraop, and forks are formatted with memory,
        # so we need to find the right keys based on best_memory
        batch_size_key = f"Batch Size (memory={best_memory}mb)"
        interop_key = f"Interop (memory={best_memory}mb)"
        intraop_key = f"Intraop (memory={best_memory}mb)"
        forks_key = f"Forks (memory={best_memory}mb)"

        # Extract the values
        best_batch_size = best_params.get(batch_size_key)
        best_interop = best_params.get(interop_key)
        best_intraop = best_params.get(intraop_key)
        best_forks = best_params.get(forks_key)

        results = {
            "memory": best_memory,
            "batch_size": best_batch_size,
            "interop": best_interop,
            "intraop": best_intraop,
            "forks": best_forks,
        }

        with open(filename, 'w') as f:
            json.dump(results, f, indent=4)
        print(f"Results saved to {filename}")


def no_policy(job):
    return job

TASK_MANAGER_CONFIG = {
    'load': {'batch_size': 1, 'max_concurrency': 32},
    'preprocess': {'batch_size': 1, 'num_cpus': 2},
    'predict': {'batch_size': 32, 'interop': 3, 'intraop': 2, 'n_models': 4}
}



param_space = {
    1769: {"Batch Size": [8, 16, 32, 64], "Interop": (1, 2), "Intraop": (1, 2), "Forks": (1, 2)},
    3008: {"Batch Size":  [8, 16, 32, 64], "Interop": (1, 4), "Intraop": (1, 4), "Forks": (1, 4)},
    7076: {"Batch Size":  [8, 16, 32, 64, 128], "Interop": (1, 8), "Intraop": (1, 8), "Forks": (1, 8)},
    10240: {"Batch Size":  [8, 16, 32, 64, 128, 256, 512], "Interop": (1, 12), "Intraop": (1, 12), "Forks": (1, 12)},
}

with open(f"../datasets/imagenet_keys/10kds.txt") as f:
    urls_general = f.read().splitlines()

bayesian_optimization = BayesianOptimization(param_spaces=param_space, inputs=urls_general, objective="Throughput/$", n_trials=100)
study, result = bayesian_optimization.optimize()
bayesian_optimization.plot_optimization_history(study)

print("Best Parameters:", study.best_params)
print("Best Value:", study.best_value)
print("Total Time Taken:", result["total_time"])
print("Total Cost Accumulated:", result["total_cost"])


