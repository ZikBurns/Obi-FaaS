# Obi-FaaS

Obi-FaaS is an offline batch inference framework built on top of serverless functions. 

This repository contains the code for Obi-FaaS to run using a ResNet50 model for image classification tasks.

## Requirements
- Python 3.8+
- Docker

## Installation
Even though this project is compatible with Python 3.8+, it was developed and tested with Miniconda Python 3.11.
The deployment is done usinng a Dockerfile with Python 3.11 as the base image.

Asuming python is installed and activated, install the required packagesby by running:
```bash
pip install -r requirements.txt
```

Requirements include Lithops. However, it's not the standard Lithops package, but [a forked version](https://github.com/cIoudlab-urv/lithops-Obi-FaaS) with the enhancements to fully support execution under the Resource Manager:


### Lithops Configuration

To run the framework, you need to configure Lithops with your cloud provider credentials.
The experiments in this repository were run using AWS Lambda, so you need to set up your AWS credentials in the `.lithops_config` file.
Lithops also needs to be configured with a storage backend, such as S3, to store the model and datasets.
You can find more information on how to configure Lithops in the [Lithops documentation](https://lithops.readthedocs.io/en/latest/configuration.html).

The `.lithops_config` file used in the experiments used the following configuration:

```yaml
lithops:
    backend: aws_lambda
    storage: aws_s3
    log_level: DEBUG

aws:
    region: eu-west-1
    access_key_id: 
    secret_access_key: 
    session_token: 

aws_lambda:
    execution_role: 
    region_name: eu-west-1
    runtime_include_function: True

aws_s3:
    storage_bucket: 
    region_name : eu-west-1

```

The fields `access_key_id`, `secret_access_key`, `session_token`, `execution_role`, and `storage_bucket` should be filled with your AWS credentials and the S3 bucket you want to use for storage.
We ran on the `eu-west-1` region, but you can change it to your preferred region.
It's important to toggle the `runtime_include_function` to `True`, so that the serverless function is included in the runtime and can be executed by the framework.

## Project Structure

Find below the structure tree of the project:
```
artifacts/
├── optimizer_model/
│   ├── best_worker.json
│   ├── cpr_model.json
│   ├── model_linear_regression.ipynb
│   └── summary.json
├── obifaas/
│   ├── included_function/
│   │   ├── commons/
│   │   │   ├── init.py
│   │   │   ├── model.py
│   │   │   └── resources.py
│   │   ├── function.py
│   │   ├── grpc_assets/
│   │   └── task_manager
│   │   ├── constants.py
│   │   ├── queuewrapper.py
│   │   ├── task_executor.py
│   │   ├── task_manager.py
│   │   └── torchscript_forker.py
│   ├── constants.py
│   ├── optimizer.py
│   ├── job_manager.py
│   ├── job_policies.py
│   ├── job.py
│   ├── orchestrator.py
│   ├── resource_provisioner.py
│   └── split_enumerator.py
├── bayesian_optimization.py
├── day.py
├── deploy_function_configurations.py
├── Dockerfile
├── example.py
├── model.pt
└── sample_cpr_model.py

datasets/
├── imagenet_images/
├── imagenet_keys/
└── day/
```

Here is the structure of the artifacts directory, which contains the main code and data for the Obi-FaaS framework:

| File/Folder                          | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|--------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `optimize_model/`                    | Contains code and data for the CPR (Cost Per Request) model.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| ├─ `best_worker.json`                | Contains the best worker setup for the ResNet50 model. This file is produced by the bayesian optimization optimizer [optimizer_bayesian_optimization.py](resnet50/optimizer_bayesian_optimization.py).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| ├─ `summary.json`                    | Summary statistics of samples collected by [profiler_sample_cpr_model.py](resnet50/optimizer_sample_cpr_model.py)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ├─ `model_linear_regression.ipynb`   | Jupyter notebook to generate the regression coefficients from the samples in [summary.json](resnet50/optimizer_model/summary.json).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| └─ `cpr_model.json`            | Saved model regression coefficients.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `obifaas/`                           | Main Obi-FaaS framework code.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ├─ `included_function/`              | Function code to be deployed in the serverless runtime.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| │   ├─ `commons/`                    | Common utilities shared between functions.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| │   │   ├─ `model.py`                | Model loading and inference helpers.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| │   │   └─ `resources.py`            | Resource management utilities                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| │   ├─ `grpc_assets/`                | Auto-generated gRPC files for batch fetching.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| │   ├─ `task_manager/`               | Manager of ML pipelines, such as: loading, transformation and inference.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| │   └─ `function.py`                 | Entrypoint for serverless function execution.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ├─ `constants.py`                    | Framework-wide constant values.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| ├─ `optimizer.py`                    | Profiling code used by [optimizer_sample_cpr_model.py](resnet50/optimizer_sample_cpr_model.py) and [optimizer_bayesian_optimization.py](resnet50/optimizer_bayesian_optimization.py)[optimizer_bayesian_optimization.py](resnet50/optimizer_bayesian_optimization.py)[optimizer_bayesian_optimization.py](resnet50/optimizer_bayesian_optimization.py)[optimizer_bayesian_optimization.py](resnet50/optimizer_bayesian_optimization.py)  to get optimizer results. OptimizerCPR that uses the model such as [cpr_model.json](resnet50/optimizer_model/cpr_model.json)[cpr_model.json](resnet50/optimizer_model/cpr_model.json) to predict the number of workers to satisfy a CPR (Cost per Request) |
| ├─ `job_manager.py`                  | Job manager class.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| ├─ `job_policies.py`                 | Job policies as examples.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| ├─ `job.py`                          | Job object to be instantiated and passed onto the orchestrator.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| ├─ `obi_faas.py`                     | Obi FaaS main orchestration class.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| ├─ `resource_provisioner.py`         | Dynamic resource allocation logic.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| └─ `split_enumerator.py`             | Batch manager to distribute batches across workers.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `.lithops_config`                    | Lithops configuration file for serverless function deployment.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `upload_dataset.py`                  | Script to upload the dataset to the cloud storage bucket. It saves the keys to S3 in [imagenet_keys](datasets/imagenet_keys).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `optimizer_bayesian_optimization.py` | Code for hyperparameter optimization using Bayesian optimization.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `optimizer_sample_cpr_model.py`      | Sample code to run all samples needed to build the CPR model                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `deploy_function_configurations.py`  | Deploys serverless functions on the cloud provider                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `example.py`                         | Example script for running the framework on 1 job.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `day.py`                             | Example script for running the framework on all jobs of 1 day.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `Dockerfile`                         | Docker build instructions.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `model.pt`                           | Pre-trained Torchvision ResNet50 model in Torchscript format                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |

The datasets directory containes the datasets used in the experiments:

| File/Folder               | Description                                                |
|---------------------------|------------------------------------------------------------|
| `imagenet_images/10kds/`  | Contains a 10.000 subset of the ImageNet dataset images.   |
| `imagenet_keys/10kds.txt` | Contains the keys of the ImageNet dataset in object storage |
| `day/day.csv`             | Contains the jobs of a day in CSV format.                  |


## Usage

To run the framework, we provide `example.py`, which demonstrates how to run a single job using Obi-FaaS. 
However, make sure to do the following steps before running the example:
- Set up the `.lithops_config` file with your cloud provider credentials and storage bucket.
- Upload the dataset to your storage bucket (e.g., AWS S3), you may find some imagenet datasets in the [imagenet_images](datasets/imagenet_images) folder, but you can also use your own dataset. Use [upload_datasets.py](artifacts/upload_datasets.py) to upload the dataset to your cloud storage bucket. This script will save the keys of the dataset in [imagenet_keys](datasets/imagenet_keys/10kds.txt) so that the framework can access them.
- To run Obi-FaaS with dynamic batch allocation (`dynamic_batch=True`), Obi-FaaS must be running on the same VPC as the Lithops worker, since batches are fetched using gRPC. Experiments were running Obi-FaaS on an EC2 instance in the same VPC as the AWS Lambda. Set `ec2_host_machine=True` during deployment so that the runtime is deployed in the same VPC as the EC2 instance. With static batch allocation (`ec2_host_machine=False` and `dynamic_batch=False`), this step is not required.
- Run `deploy_function_configurations.py` to deploy the serverless functions to your cloud provider (e.g., AWS Lambda). It usually takes 5-10 minutes per memory configuration.
- Make sure to have the coefficients for the CPR model ready. We provide a sample model in [cpr_model.json](resnet50/optimizer_model/cpr_model.json) that can be used to predict the number of workers to invoke.

To run the example, execute the following command:
```bash
python example.py
```
This will run the framework on a single job, using the ResNet50 model for image classification tasks.

Also, you can run the framework on all jobs of a day using the `day.py` script:
```bash
python day.py
```
This will get all jobs of a day from [day.csv](datasets/day/day.csv).

### Optimizer 

The optimizer solves two optimization problems:
- Finding the worker setup that maximizes throughput per dollar. Run [optimizer_bayesian_optimization.py](resnet50/optimizer_bayesian_optimization.py) to find the best worker setup for the ResNet50 model. Results are saved in [best_worker.json](resnet50/optimizer_model/best_worker.json).
- Finding the number of workers that satisfies the CPR (Cost Per Request) constraint. Run [optimizer_sample_cpr_model.py](resnet50/optimizer_sample_cpr_model.py) to collect samples and build the CPR model. Results are saved in [summary.json](resnet50/optimizer_model/summary.json). Use [model_linear_regression.ipynb](resnet50/optimizer_model/model_linear_regression.ipynb) to generate the regression coefficients from the samples and save them in [cpr_model.json](resnet50/optimizer_model/cpr_model.json).

The resulting files were already uploaded to the repository and are optimized for the ResNet50 model. To use the optimizer for another model, follow the steps above to collect samples and build the CPR model.


### Processing data
Each job produces a json file with the results of the inference, which is stored locally or in the cloud storage bucket.
To get statistics of the results, such as the latency or costs, see some of the implemented methods in [optimizer.py](resnet50/obifaas/optimizer.py).

