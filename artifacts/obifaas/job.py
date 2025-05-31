import json
import logging
import time

from .constants import DEFAULT_IP, DYNAMIC_BATCH, DEFAULT_ORCHESTRATOR, \
    ORCHESTRATOR_BACKENDS, OUTPUT_STORAGE, DEFAULT_TASK_MANAGER_CONFIG, \
    SPECULATIVE_EXECUTION, SPECULATION_MULTIPLIER, SPECULATION_QUARTILE, SPECULATION_INTERVAL_MS, MAX_SPLIT_RETRIES, \
    SPECULATION_ADD_TASK_MANAGER, \
    EXCEPTION_PERCENTAGE, DELAY_PERCENTAGE, DELAY_MAX, KEEP_ALIVE_INTERVAL, KEEP_ALIVE, LOCAL_MODEL_PATH

logger = logging.getLogger()


class Job:
    '''
    Class that represents a job to be executed by the orchestrator
    :param input: list. The input data to be processed
    :param job_name: str. The name of the job
    :param bucket: str. The bucket where the input data is stored
    :param split_size: int. The size of the splits
    :param num_task_managers: int. The number of task managers to be used
    :param dynamic_batch: bool. True if the split is dynamic, False if it is static
    :param ip: str. The IP address of the orchestrator
    :param task_manager_config: dict. The configuration of the task manager
    :param orchestrator_backend: str. The backend to be used by the orchestrator
    :param output_storage: str. The storage to be used for the output
    :param output_location: str. The location where the output will be stored
    :param output_bucket: str. The bucket where the output will be stored
    :param speculation_enabled: bool. True if speculative execution is enabled
    :param speculation_multiplier: int. The multiplier for speculative execution
    :param speculation_quartile: float. The quartile for speculative execution
    :param speculation_interval_ms: int. The interval for speculative execution
    :param max_split_retries: int. The maximum number of retries for a split
    '''

    def __init__(self,
                 input: list,
                 job_name: str = None,
                 bucket: str = None,
                 split_size: int = None,
                 num_task_managers: int = None,
                 dynamic_batch: bool = DYNAMIC_BATCH,
                 ip: str = None,
                 task_manager_config: dict = DEFAULT_TASK_MANAGER_CONFIG,
                 orchestrator_backend: str = DEFAULT_ORCHESTRATOR,
                 output_storage: str = None,
                 output_location: str = None,
                 output_bucket: str = None,
                 speculation_enabled: bool = SPECULATIVE_EXECUTION,
                 speculation_multiplier: int = SPECULATION_MULTIPLIER,
                 speculation_quartile: float = SPECULATION_QUARTILE,
                 speculation_interval_ms: int = SPECULATION_INTERVAL_MS,
                 speculation_add_task_manager: bool = SPECULATION_ADD_TASK_MANAGER,
                 max_split_retries: int = MAX_SPLIT_RETRIES,
                 exception_percentage: int = EXCEPTION_PERCENTAGE,
                 delay_percentage: int = DELAY_PERCENTAGE,
                 delay_max: int = DELAY_MAX,
                 keep_alive: bool = KEEP_ALIVE,
                 keep_alive_interval: int = KEEP_ALIVE_INTERVAL,
                 local_model_path: str = LOCAL_MODEL_PATH,
                 get_cloudwatch_report=True,
                 local_function=None,
                 heterogeneous_splits = None
                 ):
        self.input = input
        if not job_name:
            self.job_name = str(time.strftime("%Y%m%d-%H%M%S"))
        else:
            self.job_name = job_name

        self.num_inputs = len(input)
        self.bucket = bucket
        self.split_size = split_size
        self.num_task_managers = num_task_managers
        self.output = None
        self.orchestrator_stats = {
            "created": time.time(),
            "assigned": None,
            "started": None,
            "finished": None
        }
        self.dynamic_batch = dynamic_batch
        self.ec2_metadata = None
        self.task_manager_config = task_manager_config

        if orchestrator_backend not in ORCHESTRATOR_BACKENDS:
            self.orchestrator_backend = DEFAULT_ORCHESTRATOR
        else:
            self.orchestrator_backend = orchestrator_backend
            if orchestrator_backend == "local" and not ip:
                ip = DEFAULT_IP
        if not dynamic_batch:
            ip = None
        self.ip = ip
        self.invoke_output = None

        if output_storage not in OUTPUT_STORAGE:
            self.output_storage = None
        else:
            self.output_storage = output_storage
        self.output_location = output_location
        self.output_bucket = output_bucket
        self.speculation_enabled = speculation_enabled
        self.speculation_multiplier = speculation_multiplier
        self.speculation_quartile = speculation_quartile
        self.speculation_interval = speculation_interval_ms / 1000
        self.speculation_add_task_manager = speculation_add_task_manager
        self.split_info = None
        self.max_split_retries = max_split_retries
        self.exception_percentage = exception_percentage
        self.delay_percentage = delay_percentage
        self.delay_max = delay_max
        self.extra_task_managers = 0
        self.extra_invoke_output = None
        self.local_model_path = local_model_path
        self.keep_alive = keep_alive
        self.keep_alive_interval = keep_alive_interval
        self.get_cloudwatch_report = get_cloudwatch_report
        # Check that if the orchestrator_backend is local, the local_function is not None
        self.local_function = local_function
        self.heterogeneous_splits = heterogeneous_splits

    def to_dict(self):
        '''
        Returns the job as a dictionary
        :return: dict. The job as a dictionary
        '''
        return {
            "input": self.input,
            "job_name": self.job_name,
            "bucket": self.bucket,
            "split_size": self.split_size,
            "num_task_managers": self.num_task_managers,
            "extra_task_managers": self.extra_task_managers,
            "output": self.output,
            "orchestrator_stats": self.orchestrator_stats,
            "dynamic_batch": self.dynamic_batch,
            "ec2_metadata": self.ec2_metadata,
            "task_manager_config": self.task_manager_config,
            "orchestrator_backend": self.orchestrator_backend,
            "ip": self.ip,
            "output_storage": self.output_storage,
            "output_location": self.output_location,
            "output_bucket": self.output_bucket,
            "speculation_enabled": self.speculation_enabled,
            "speculation_multiplier": self.speculation_multiplier,
            "speculation_quartile": self.speculation_quartile,
            "speculation_interval": self.speculation_interval,
            "split_info": self.split_info,
            "max_split_retries": self.max_split_retries,
            "invoke_output": self.invoke_output,
            "extra_invoke_output": self.extra_invoke_output
        }

    def to_json(self):
        return json.dumps(self.__dict__)

    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        return cls(**data)
