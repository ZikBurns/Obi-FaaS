import random
import logging
import time
import threading
from threading import Thread
import os
import shutil
from concurrent import futures
from .included_function.grpc_assets.split_grpc_pb2_grpc import SPLITRPCServicer, add_SPLITRPCServicer_to_server
import grpc
from lithops import FunctionExecutor
from .batch_manager import SplitEnumerator, SplitController, ThreadSafeList
from .job import Job
from .resource_provisioner import ResourceProvisioner

logger = logging.getLogger()


class JobManager:
    def __init__(self,
                 fexec_args: dict,
                 job: Job,
                 resource_provisioner: ResourceProvisioner,
                 split_enumerator_thread_pool_size: int,
                 split_enumerator_port: int,
                 ):
        self.fexec_args = fexec_args
        self.job = job
        self.resource_provisioner = resource_provisioner
        self.split_enumerator_thread_pool_size = split_enumerator_thread_pool_size
        self.split_enumerator_port = split_enumerator_port
        self.split_controller = None
        self.server_thread = None
        self.running_server_flag = None
        self.stop_server_flag = None
        self.extra_futures = None
        self.fexec = None
        self.alive_check_table = None

    def serve(self):
        """
        Starts the gRPC server and waits for the stop_server_flag to be set.
        This method is to be called by the start_server method, using a separate thread.
        """
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=self.split_enumerator_thread_pool_size))
        add_SPLITRPCServicer_to_server(SplitEnumerator(self), server)
        logger.info(f"Starting gRPC server at {self.job.ip}:{self.split_enumerator_port}")
        server.add_insecure_port(f'{self.job.ip}:{self.split_enumerator_port}')
        server.start()
        self.running_server_flag.set()
        self.stop_server_flag.wait()
        server.stop(0)

    def start_server(self):
        """
        Starts the gRPC server and waits for the server_thread to finish.
        """
        self.running_server_flag = threading.Event()
        self.stop_server_flag = threading.Event()
        self.server_thread = Thread(target=self.serve, args=())
        self.server_thread.start()
        self.running_server_flag.wait()

    def close_server(self):
        """ Stops the gRPC server and waits for the server_thread to finish."""
        self.stop_server_flag.set()
        self.server_thread.join()

    def build_dynamic_payload(self):
        """
        Builds the payload for the dynamic split.
        The payload is a list of dictionaries, each dictionary containing the information for a task manager.
        The information includes the task manager ID, the IP, the port, the configuration, the flag to raise an exception and the delay for the task manager.
        """
        tids = list(range(self.job.num_task_managers))
        num_trues = int(len(tids) * self.job.exception_percentage / 100)
        random_list = [True] * num_trues + [False] * (len(tids) - num_trues)
        random.shuffle(random_list)

        num_trues = int(len(tids) * self.job.delay_percentage / 100)
        delay_list = [random.randint(0, self.job.delay_max) for i in range(num_trues)] + [0] * (len(tids) - num_trues)
        random.shuffle(delay_list)

        payload_list = []
        for i in range(len(tids)):
            payload = {'body':
                {
                    'tid': str(tids[i] + 1),
                    'ip': self.job.ip,
                    'port': self.split_enumerator_port,
                    'keep_alive': self.job.keep_alive,
                    'keep_alive_interval': self.job.keep_alive_interval,
                    'config': self.job.task_manager_config,
                    'to_raise_exception': random_list[i],
                    'to_delay': delay_list[i]
                }
            }
            if self.job.orchestrator_backend != "local":
                payload['body']['bucket'] = self.job.bucket
            payload_list.append(payload)
        return payload_list

    def build_static_payload(self):
        """
        Builds the payload for the static split.
        The payload is a list of dictionaries, each dictionary containing the inputs for each task manager.
        """

        def divide_dict_into_chunks(items, num_chunks):
            chunk_size = len(items) // num_chunks
            remainder = len(items) % num_chunks
            chunks = []
            start = 0
            for i in range(num_chunks):
                end = start + chunk_size
                if i < remainder:
                    end += 1
                chunks.append(items[start:end])
                start = end
            return chunks

        dataset = self.job.input
        payload_list = []
        splits = divide_dict_into_chunks(dataset, self.job.num_task_managers)
        for split in splits:
            payload = {'body':
                {
                    'split': split,
                    'config': self.job.task_manager_config,
                }
            }
            if self.job.orchestrator_backend != "local":
                payload['body']['bucket'] = self.job.bucket
            payload_list.append(payload)
        return payload_list

    def build_payload(self):
        """
        Builds the payload for the job. If the job is dynamic, it builds the dynamic payload.
        If the job is static, it builds the static payload.
        """
        if self.job.dynamic_batch:
            return self.build_dynamic_payload()
        else:
            return self.build_static_payload()

    def splits_outputs(self):
        """
        Returns the outputs of the splits as a dictionary.
        """
        all_outputs = {}
        sids = self.split_controller.done_splits.to_list()
        logger.info(f"Done splits: {sids}")
        for sid in sids:
            split_outputs = self.split_controller.done_splits.get(sid).output
            for output in split_outputs:
                all_outputs.update({output: split_outputs[output]})
        # Assign the inputs that were not split to the output with None as value
        for input in self.job.input:
            if input not in all_outputs:
                all_outputs.update({input: None})
        return all_outputs

    def splits_times(self):
        """
        Returns the start and end times of the splits as a dictionary.
        """
        times = {}
        for sid in self.split_controller.done_splits.to_list():
            split = self.split_controller.done_splits.get(sid)
            times.update({split: {"start_time": split.start_time, "end_time": split.end_time}})
        return times

    def initialize_dynamic_job(self):
        """
        Initializes the dynamic job. It sets the IP of the job and initializes the split controller and the extra futures.
        """
        if self.job.ec2_metadata:
            self.job.ip = self.job.ec2_metadata["ip"]
        self.split_controller = SplitController(self.job.input, self.job.split_size, self.job.max_split_retries,  self.job.heterogeneous_splits)
        self.extra_futures = ThreadSafeList()
        self.split_controller.initialize_pending_queue()
        self.start_server()

    def close_dynamic_job(self, invoke_output):
        """
        Closes the dynamic job. It stops the server, gets the outputs of the splits, the times of the splits,
        and the split info.
        """
        self.close_server()
        self.job.output = invoke_output
        self.job.split_times = self.splits_times()
        self.job.split_info = [self.split_controller.done_splits.get(sid).to_dict_reduced() for sid in
                               self.split_controller.done_splits.to_list()]
        self.job.invoke_output = self.splits_outputs()

    def add_extra_task_executors(self, num_extra_task_managers):
        """
        Adds extra task managers to the job updates and adds the futures of the extra task managers to the extra futures.
        """
        logger.info(f"Adding {num_extra_task_managers} extra task managers.")
        logger.info(
            f"New TIDs: {list(range(self.job.num_task_managers + 1, self.job.num_task_managers + num_extra_task_managers + 1))}")
        payload_list = []
        for tid in range(self.job.num_task_managers, self.job.num_task_managers + num_extra_task_managers):
            payload = {'body':
                {
                    'tid': str(tid + 1),
                    'ip': self.job.ip,
                    'port': self.split_enumerator_port,
                    'config': self.job.task_manager_config
                }
            }
            if self.job.orchestrator_backend != "local":
                payload['body']['bucket'] = self.job.bucket
            payload_list.append(payload)
        futures = self.resource_provisioner.call(self.fexec, payload_list, self.job.local_function)
        self.job.num_task_managers += num_extra_task_managers
        self.extra_futures.extend(futures)
        logger.info(f"Extra task managers added.")

    def wait_extra_task_executors(self):
        logger.info(f"Waiting for extra task managers to finish.")
        list_of_futures = self.extra_futures.to_list()
        invoke_output = self.resource_provisioner.wait_futures(self.fexec, list_of_futures)
        logger.info(f"Extra task managers finished.")
        self.job.extra_task_managers = len(list_of_futures)
        return invoke_output

    def run(self):
        self.job.orchestrator_stats["assigned"] = time.time()

        if self.job.dynamic_batch: self.initialize_dynamic_job()

        self.fexec = FunctionExecutor(**self.fexec_args)
        if self.job.orchestrator_backend != "local":
            if not self.job.bucket:
                self.job.bucket = self.fexec.config['aws_s3']['storage_bucket']
        else:
            os.makedirs('/tmp/obifaas/', exist_ok=True)
            # split directory from file in local_model_path
            directory, filename = os.path.split(self.job.local_model_path)
            shutil.copyfile(self.job.local_model_path, f'/tmp/obifaas/{filename}')

        payload = self.build_payload()
        self.job.orchestrator_stats["started"] = time.time()
        futures = self.resource_provisioner.call(self.fexec, payload, self.job.local_function)
        invoke_output = self.resource_provisioner.wait_futures(self.fexec, futures)

        if self.job.dynamic_batch:
            if not self.extra_futures.empty():
                extra_invoke_output = self.wait_extra_task_executors()
                self.job.extra_invoke_output = extra_invoke_output

            self.close_dynamic_job(invoke_output)
        else:
            self.job.output = invoke_output

        self.job.orchestrator_stats["finished"] = time.time()
        if self.job.get_cloudwatch_report:
            self.job = self.resource_provisioner.get_cloudwatch_report(self.fexec, self.job)
        self.resource_provisioner.save_output(self.fexec, self.job)
        if self.job.orchestrator_backend == "local":
            os.remove(f'/tmp/obifaas/{filename}')
        return self.job.to_dict()
