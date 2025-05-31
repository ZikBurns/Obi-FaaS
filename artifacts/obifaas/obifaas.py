import socket
import logging
import time
from concurrent.futures import ProcessPoolExecutor
import threading
import os
from concurrent import futures
from queue import Queue
import grpc
from lithops import FunctionExecutor
from .constants import MAX_JOB_MANAGERS, MAX_TASK_MANAGERS, SPLIT_ENUMERATOR_THREAD_POOL_SIZE, \
    ORCHESTRATOR_BACKENDS, EC2_HOST_MACHINE, LOGGING_FORMAT,  AWS_LAMBDA_BACKEND,LOCAL_BACKEND
from .job_manager import JobManager
from .batch_manager import SplitEnumerator, SplitController, ThreadSafeList
from .job import Job
from .resource_provisioner import ResourceProvisioner

logger = logging.getLogger()

def default_job_policy(job):
    if job.dynamic_batch:
        return job
    else:
        split_size = job.split_size
        num_inputs = len(job.input)
        num_task_managers = int(num_inputs / split_size)
        job.num_task_managers = int(num_task_managers) if num_task_managers == int(num_task_managers) else int(num_task_managers) + 1
        return job

class Obi_FaaS:
    """
    The Obi_FaaS is used to coordinate the Job Managers. It will enqueue the jobs and start the Job Managers.
    Important Parameters:
    :param fexec_args: dict. The arguments to be used in the FunctionExecutor7
    :param orchestrator_backends: list. The orchestrator backends to be used and initialized
    :param initialize: bool. If Obi_FaaS should be initialized
    :param job_policy: JobPolicy. The job policy to be used
    :param ec2_host_machine: bool. If the host machine is an EC2 instance
    :param max_job_managers: int. The maximum number of job managers to be used
    :param max_task_managers: int. The maximum number of task managers to be used

    Secondary Parameters:
    :param split_enumerator_thread_pool_size: int. The thread pool size for the split enumerator
    :param logging_level: int. The logging level
    :param job_pool_executor: Executor. The executor to be used in the job pool
    """

    def __init__(self,
                 fexec_args: dict = {},
                 orchestrator_backends: list = ORCHESTRATOR_BACKENDS,
                 initialize=True,
                 job_policy=default_job_policy,
                 ec2_host_machine: bool = EC2_HOST_MACHINE,
                 max_job_managers=MAX_JOB_MANAGERS,
                 max_task_managers=MAX_TASK_MANAGERS,
                 split_enumerator_thread_pool_size: int = SPLIT_ENUMERATOR_THREAD_POOL_SIZE,
                 logging_level=logging.INFO,
                 job_pool_executor=ProcessPoolExecutor(),
                 ):
        # Set the logger configuration
        logging.basicConfig(
            level=logging_level,
            format=LOGGING_FORMAT,
            handlers=[logging.StreamHandler()]
        )

        # check if orchestrator_backend is valid
        if all(backend not in ORCHESTRATOR_BACKENDS for backend in orchestrator_backends):
            raise ValueError(
                f"Invalid orchestrator_backend: {orchestrator_backends}. Must be of {ORCHESTRATOR_BACKENDS}")
        self.orchestrator_backends = orchestrator_backends

        # Check if the thread pool size is valid
        if split_enumerator_thread_pool_size < 1:
            raise ValueError(
                f"Invalid split_enumerator_thread_pool_size: {split_enumerator_thread_pool_size}. Must be greater than 0")
        self.split_enumerator_thread_pool_size = split_enumerator_thread_pool_size

        # Check if the max_job_managers is valid
        if max_job_managers < 1:
            raise ValueError(f"Invalid max_job_managers: {max_job_managers}. Must be greater than 0")

        # Check if the max_task_managers is valid
        if max_task_managers < 1:
            raise ValueError(f"Invalid max_task_managers: {max_task_managers}. Must be greater than 0")

        # Initialize the ResourceProvisioner
        self.resource_provisioner = ResourceProvisioner(job_policy, max_job_managers, max_task_managers)

        self.fexec_args = fexec_args
        self.ec_2_metadata = None
        if AWS_LAMBDA_BACKEND in orchestrator_backends:
            if ec2_host_machine:
                self.ec_2_metadata = self.resource_provisioner.get_ec2_metadata()

                logger.info(f"EC2 metadata: {self.ec_2_metadata}")
                self.fexec_args['vpc'] = {'subnets': [self.ec_2_metadata['subnet_id']],
                                          'security_groups': [self.ec_2_metadata['security_group_id']]}

        self.job_queue = Queue()
        self.stop_server_flag = threading.Event()

        self.keep_running = True
        self.job_pool_executor = job_pool_executor

        if initialize:
            if AWS_LAMBDA_BACKEND in orchestrator_backends:
                if self.check_runtime_status(fexec=None, double_check=True):
                    logger.info(f"Runtime is available")
                else:
                    logger.info(f"Runtime is not available")
                    self.redeploy_runtime(fexec=None)
            elif LOCAL_BACKEND in orchestrator_backends:
                self.copy_default_included_function()
        logger.info(f"Orchestrator initialized")

    def check_runtime_status(self, fexec: FunctionExecutor = None, double_check: bool = True):
        '''
        Checks if the runtime is available. If double_check is True, it will test the runtime by calling a function.
        :param fexec: FunctionExecutor. The FunctionExecutor to be used
        :param double_check: bool. If True, it will test the runtime by calling a function
        :return: bool. True if the runtime is available, False otherwise
        '''
        logger.info(f"Checking Lithops backend configuration...")
        if not fexec:
            fexec = FunctionExecutor(**self.fexec_args)
        runtime_meta = fexec.invoker.get_runtime_meta(fexec._create_job_id('A'),
                                                      fexec.config['aws_lambda']['runtime_memory'])
        if runtime_meta:
            if double_check:
                logger.info(f"Runtime {fexec.backend} found.")
                logger.info(f'Testing call')
                lithops_futures = self.resource_provisioner.lithops_call(fexec,
                                                                         payloads=[{'body': {'ping': True}}])
                lithops_results = self.resource_provisioner.wait_futures(fexec, futures=lithops_futures,
                                                                         timeout=30, exception_str=False)
                if isinstance(lithops_results, Exception):
                    logger.info(f"Runtime {fexec.backend} found but not working.")
                    return False
                logger.info(f"Runtime {fexec.backend} found and working.")
            return True
        else:
            logger.info(f"Runtime {fexec.backend} not found.")
            return False

    def redeploy_runtime(self, fexec: FunctionExecutor = None, initialize: bool = True):
        '''
        Redeploys the runtime by deleting it, creating it again and testing it.
        '''
        if not fexec:
            fexec = FunctionExecutor(**self.fexec_args)
        fexec.compute_handler.delete_runtime(fexec.config['aws_lambda']['runtime'],
                                             fexec.config['aws_lambda']['runtime_memory'])
        exists_included_function = self.copy_default_included_function()

        logger.info(f"Runtime {self.fexec_args['runtime']} not found. Creating runtime...")
        if initialize:
            lithops_futures = self.resource_provisioner.lithops_call(fexec, payloads=[{'body': {'ping': True}}])
            lithops_results = self.resource_provisioner.wait_futures(fexec, futures=lithops_futures, timeout=30,
                                                                     exception_str=False)
            logger.info(f"Runtime created and returned: {lithops_results}")

    def copy_default_included_function(self):
        if not os.path.isdir("included_function"):
            exists_included_function = False
            logger.info(f"Using default included_function")
            # Get directory of this file
            current_dir = os.path.dirname(os.path.realpath(__file__))
            # Get working directory
            cwd = os.getcwd()
            # Copy the current_dit/included_function to the current working directory using a python library
            import shutil
            shutil.copytree(f"{current_dir}/included_function", f"{cwd}/included_function")
        else:
            logger.info(f"Using included_function found in the current directory")
            exists_included_function = True
        return exists_included_function

    def delete_runtime(self):
        '''
        Deletes the runtime.
        '''
        fexec = FunctionExecutor(**self.fexec_args)
        fexec.compute_handler.delete_runtime(fexec.config['aws_lambda']['runtime'],
                                             fexec.config['aws_lambda']['runtime_memory'])

    def enqueue_job(self, job: Job):
        """
        Enqueues a job to later be processed by a JobManager.
        :param job: Job. The job to be enqueued
        """
        self.job_queue.put(job)

    def dequeue_job(self):
        """
        Dequeues a job from the job_queue.
        :return: Job. The job dequeued
        """
        job = self.job_queue.get()
        return job

    def get_available_port(self):
        """
        Gets an available port to be used by the Job Manager that is also valid for gRPC.
        """
        ip = self.ec_2_metadata["ip"] if self.ec_2_metadata else None

        while True:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                try:
                    sock.bind(('', 0))
                    assigned_port = sock.getsockname()[1]
                except Exception:
                    continue  # Try another port if binding fails

            # Now test the port with gRPC
            try:
                test_server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
                port_binding = test_server.add_insecure_port(f'{ip}:{assigned_port}')
                if port_binding == 0:
                    logger.info(f"Port {assigned_port} cannot be used by gRPC. Trying another.")
                    continue  # gRPC could not bind to the port
                else:
                    test_server.stop(0)  # Clean up the test server
                    return assigned_port
            except Exception as e:
                logger.info(f"Port {assigned_port} caused gRPC error ({e}). Trying another.")
                continue

    def run_orchestrator(self):
        '''
        Runs the orchestrator. Gets Jobs out of the queue and starts Job Managers if there are available resources.
        '''
        future_to_job = {}  # Dictionary to store future and corresponding job
        while (self.keep_running):
            if not self.job_queue.empty():
                job = self.dequeue_job()
                _job = self.resource_provisioner.apply_job_policy(job)
                _job = self.resource_provisioner.correct_resources(_job)
                if self.resource_provisioner.check_available_resources(_job):
                    self.resource_provisioner.increment_count(_job.num_task_managers)
                    future = self.start_job_manager(_job)
                    future_to_job[future] = _job  # Save future and job together
                else:
                    self.enqueue_job(job)

            # Check if is there any future that is done
            done_futures = [f for f in future_to_job if f.done()]
            for future in done_futures:
                job = future_to_job.pop(future)
                logger.info(f"Job {job.job_name} finished.")
                self.resource_provisioner.decrement_count(job.num_task_managers)
            time.sleep(1)

    def run_job(self, job):
        '''
        Runs one job. If there are available resources, it will start a Job Manager to process the job.
        This method is used to run a single job without starting the orchestrator.
        :param job: Job. The job to be processed
        :return: dict. The job as a dictionary
        '''
        job = self.resource_provisioner.apply_job_policy(job)
        job = self.resource_provisioner.correct_resources(job)
        if self.resource_provisioner.check_available_resources(job):
            self.resource_provisioner.increment_count(job.num_task_managers)
            future = self.start_job_manager(job)
            result = future.result()
            self.resource_provisioner.decrement_count(job.num_task_managers)
            return result
        else:
            raise ValueError(f"Job {job.job_name} could not be started due to lack of resources")

    def start_job_manager(self, job):
        '''
        Starts a Job Manager to process the job.
        :param job: Job. The job to be processed
        :return: future. The future of the Job Manager
        '''

        available_port = self.get_available_port()  if job.dynamic_batch else None
        job.ec2_metadata = self.ec_2_metadata
        job_manager = JobManager(fexec_args=self.fexec_args, job=job, resource_provisioner=self.resource_provisioner,
                                 split_enumerator_thread_pool_size=self.split_enumerator_thread_pool_size,
                                 split_enumerator_port=available_port)

        future = self.job_pool_executor.submit(job_manager.run)

        return future

    def force_cold_start(self, sleep_time=600):
        '''
        Forces a cold start by deleting the runtime and creating it again.
        '''
        fexec = FunctionExecutor(**self.fexec_args)
        runtime_name = fexec.config['aws_lambda']['runtime']
        runtime_memory = fexec.config['aws_lambda']['runtime_memory']
        fexec.compute_handler.backend.force_cold(runtime_name, runtime_memory)
        time.sleep(sleep_time)


