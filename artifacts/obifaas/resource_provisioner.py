import json
import logging
import time
import os
from lithops import FunctionExecutor
from .constants import EC2_METADATA_SERVICE, DEFAULT_TASK_MANAGERS, DEFAULT_SPLIT_SIZE

from .job import Job

logger = logging.getLogger()

class ResourceProvisioner():
    '''
    Class that represents the resource provisioner for the orchestrator.
    :param job_policy: function. The job policy to be used by the orchestrator. It assigns the parameters to the job based on the input size.
                                 It receives the job, the maximum number of task managers and the maximum split size as parameters.
    :param max_job_managers: int. The maximum number of job managers to be used. The orchestrator will not exceed this number, it has the priority.
    :param max_task_managers: int. The maximum number of task managers. The orchestrator will not exceed this number, it has the priority.
    '''

    def __init__(self, job_policy, max_job_managers: int = 1, max_task_managers: int = 1):
        self.job_policy = job_policy
        self.max_job_managers = max_job_managers
        self.max_task_managers = max_task_managers
        self.count_job_managers = 0
        self.count_task_managers = 0

    def increment_count(self, num_task_managers):
        '''
        Increments the count of task managers by num_task_managers and job managers by 1
        :param num_task_managers: int. The number of task managers to be added
        '''
        self.count_task_managers += num_task_managers
        self.count_job_managers += 1
        logging.info(
            f"Incrementing count. Now {self.count_task_managers} task managers and {self.count_job_managers} job managers.")

    def decrement_count(self, num_task_managers):
        '''
        Decrements the count of task managers by num_task_managers and job managers by 1
        '''
        self.count_task_managers -= num_task_managers
        self.count_job_managers -= 1
        logging.info(
            f"Decrementing count. Now {self.count_task_managers} task managers and {self.count_job_managers} job managers.")

    def check_available_resources(self, job):
        '''
        Checks if there are available resources for the job
        :param job: Job. The job to be checked
        :return: bool. True if there are available resources, False otherwise
        '''
        logging.info(
            f"Available resources: ({self.count_task_managers}/{self.max_task_managers}) task managers and ({self.count_job_managers}/{self.max_job_managers}) job managers.")
        num_task_managers = job.num_task_managers
        if self.count_job_managers < self.max_job_managers and self.count_task_managers < self.max_task_managers:
            if self.count_task_managers + num_task_managers <= self.max_task_managers:
                logging.info(
                    f"Adding {num_task_managers} task managers will not exceed the maximum number of task managers {self.max_task_managers}.")
                return True
            else:
                logging.info(
                    f"Adding {num_task_managers} task managers will exceed the maximum number of task managers {self.max_task_managers}.")
                return False
        else:
            logging.info(f"Maximum number of task managers {self.max_task_managers} reached. No available resources.")
            return False

    def correct_resources(self, job):
        '''
        Corrects the resources of the job if the number of task managers exceeds the maximum number of task managers.
        Also corrects the number of task managers if it is greater than the number of inputs.
        :param job: Job. The job to be corrected
        :return: Job. The job with the corrected resources
        '''
        if self.count_task_managers + job.num_task_managers > self.max_task_managers:
            job.num_task_managers = self.max_task_managers - self.count_task_managers
            logger.info(f"Correcting resources. Using {job.num_task_managers} task managers.")

        if not job.num_task_managers:
            job.num_task_managers = 1

        if not job.split_size:
            job.split_size = 1

        if job.num_task_managers > job.num_inputs:
            job.num_task_managers = job.num_inputs
            logger.info(f"Correcting resources. Using {job.num_task_managers} task managers.")
        return job

    def apply_job_policy(self, job: Job):
        '''
        Assigns the parameters to the job based on the input size
        :param job: Job. The job to be assigned the parameters
        :return: Job. The job with the parameters assigned
        '''
        if not job.num_task_managers:
            job.num_task_managers = DEFAULT_TASK_MANAGERS
        if not job.split_size:
            job.split_size = DEFAULT_SPLIT_SIZE
        job = self.job_policy(job)
        return job

    def get_cloudwatch_report(self, fexec: FunctionExecutor, job):
        session = fexec.compute_handler.backend.aws_session
        logs_client = session.client('logs', region_name='eu-west-1')
        invoke_stats = job.output['lithops_stats']
        if invoke_stats:
            runtime_name = fexec.compute_handler.config['aws_lambda']['runtime']
            runtime_memory = fexec.compute_handler.config['aws_lambda']['runtime_memory']
            function_name = fexec.compute_handler.backend._format_function_name(runtime_name=runtime_name,
                                                                                runtime_memory=runtime_memory)
            log_group_name = f'/aws/lambda/{function_name}'

            for i, stat in enumerate(invoke_stats):
                count = 0
                while 'log_message' not in invoke_stats[i] and count < 30:
                    activation_id = stat['activation_id']
                    start_time = stat['host_submit_tstamp']
                    end_time = stat['host_status_done_tstamp']
                    response = logs_client.filter_log_events(
                        logGroupName=log_group_name,
                        filterPattern=f"\"REPORT RequestId: {activation_id}\"",
                        startTime=int(start_time * 1000),
                        endTime=int(end_time * 1000),
                    )
                    if response['events']:
                        for event in response['events']:
                            message = event['message']
                            print(f"Log Message: {message}")

                            # Search for the billed duration in the log message
                            if activation_id in message:
                                # Add message to the invoke stats and update in job
                                invoke_stats[i]['log_message'] = message
                                break
                    else:
                        time.sleep(1)
                        count += 1
            job.output['lithops_stats'] = invoke_stats
        return job

    def lithops_call(self, fexec: FunctionExecutor, payloads: list, exception_str: bool = True):
        '''
        Calls map_async of lithops with the payloads
        :param fexec: FunctionExecutor. The lithops function executor
        :param payloads: list. The payloads to be sent to the task managers
        :param timeout: int. The timeout for the call
        :param exception_str: bool. True if the exception should be returned as a string, False otherwise
        :return: dict. The results of the task managers
        '''
        num_task_managers = len(payloads)
        try:
            payload_list = []
            for payload in payloads:
                payload_list.append({'payload': payload})
            logger.info(f"Calling {num_task_managers} task managers.")
            futures = fexec.map_async(map_iterdata=payload_list)
            return futures
        except Exception as e:
            if exception_str:
                try:
                    e = str(e)
                except Exception as e2:
                    pass
            logger.error(f"Error in lithops call: {e}")
            logger.error(
                f"Depending on the error, you may need to redeploy the runtime. Use the redeploy_runtime method of the orchestrator.")
            return e

    def local_call(self, fexec: FunctionExecutor, payloads: list, local_function=None):
        '''
        Calls a pool of local task managers with the payloads
        :param payloads: list. The payloads to be sent to the task managers
        :return: list. The results of the task managers
        '''
        num_task_managers = len(payloads)
        try:
            payload_list = []
            for payload in payloads:
                payload_list.append({'payload': payload})
            logger.info(f"Calling {num_task_managers} task managers.")
            futures = fexec.map(map_function=local_function, map_iterdata=payload_list)
            return futures
        except Exception as e:
            logger.error(f"Error in lithops call: {e}")
            logger.error(
                f"Depending on the error, you may need to redeploy the runtime. Use the redeploy_runtime method of the orchestrator.")
            return e

    def wait_futures(self, fexec: FunctionExecutor, futures: list, timeout: int = None,
                     exception_str: bool = True):
        '''
        Waits for the futures of the task managers.
        :param fexec: FunctionExecutor. The function executor to be used
        :param futures: list. The futures of the task managers
        :param timeout: int. The timeout for the call
        :param exception_str: bool. True if the exception should be returned as a string, False otherwise
        '''
        num_task_managers = len(futures)
        results = []
        try:
            results = fexec.get_result(futures, timeout=timeout)
            logger.info(f"Finished {num_task_managers} task managers")
            try:
                stats = fexec.stats(futures)

            except Exception as e:
                stats = None
                logger.error(f"Error getting stats: {e}")
            lithops_results = {'lithops_results': results, 'lithops_stats': stats, 'lithops_config': fexec.config}
        except Exception as e:
            if exception_str:
                e = str(e)
            lithops_results = {'lithops_results': results, 'lithops_stats': None, 'lithops_config': fexec.config,
                               'error': e}
            logger.error(f"Error in lithops call: {e}")
            logger.error(
                f"Depending on the error, you may need to redeploy the runtime. Use the redeploy_runtime method of the orchestrator.")
        return lithops_results

    def call(self, fexec, payloads: list, local_function=None, exception_str: bool = True):
        '''
        Calls the function executor with the payloads. If the function executor is not provided, it calls the local function.
        :param fexec: FunctionExecutor. The function executor to be used
        '''
        num_task_managers = len(payloads)
        try:
            payload_list = []
            for payload in payloads:
                payload_list.append({'payload': payload})
            logger.info(f"Calling {num_task_managers} task managers.")
            if fexec.backend == 'localhost':
                futures = fexec.map(map_function=local_function, map_iterdata=payload_list)
            else:
                futures = fexec.map_async(map_iterdata=payload_list)
            return futures
        except Exception as e:
            if exception_str:
                try:
                    e = str(e)
                except Exception as e2:
                    pass
            logger.error(f"Error in lithops call: {e}")
            logger.error(
                f"Depending on the error, you may need to redeploy the runtime. Use the redeploy_runtime method of the orchestrator.")
            return e

    def save_output(self, fexec, job):
        '''
        Saves the output of the job to the output storage
        :param fexec: FunctionExecutor. The function executor to be used
        :param job: Job. The job to be saved
        :return: bool. True if the output was saved, False otherwise
        '''
        try:
            if job.output_storage == "s3" and fexec:
                logger.info(f"Saving output to s3 bucket {job.bucket} with key {job.output_location}.json")
                json_string = json.dumps(job.to_dict())
                fexec.storage.put_object(bucket=fexec.storage.bucket, key=f"{job.output_location}.json",
                                         body=json_string)
                return True
            elif job.output_storage == "local":
                logger.info(f"Saving output to local file {job.output_location}.json")
                # Create directory if it does not exist
                os.makedirs(os.path.dirname(job.output_location), exist_ok=True)
                with open(f"{job.output_location}.json", "w") as file:
                    json.dump(job.to_dict(), file, indent=4)
                return True
        except Exception as e:
            logger.error(f"Error saving output: {e}")
            return False

        logger.info(f"No Output storage was declared. Output not saved, but printed as json. ")
        # Print the json to the logger
        logger.info(json.dumps(job.to_dict()))
        return True

    def get_ec2_metadata(self):
        '''
        ONLY USE IF INSIDE AN EC2 MACHINE
        Retrieves the metadata of the EC2 instance. Uses the EC2 instance metadata service
        https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html
        :return: dict. The metadata of the EC2 instance
        '''
        import urllib.request
        def get_instance_metadata(metadata_path):
            try:
                # Make a GET request to the EC2 instance metadata service
                response = urllib.request.urlopen(metadata_path)

                # Decode the response
                metadata = response.read().decode()

                # Return the metadata as a string
                return metadata.strip()
            except Exception as e:
                logger.error(f"An error occurred while retrieving metadata: {e}")
                return None

        ec_2_metadata = {}
        ec_2_metadata['instance_id'] = get_instance_metadata(f'{EC2_METADATA_SERVICE}/instance-id')
        ec_2_metadata['mac'] = get_instance_metadata(f'{EC2_METADATA_SERVICE}/mac')
        ec_2_metadata['subnet_id'] = get_instance_metadata(
            f'{EC2_METADATA_SERVICE}/network/interfaces/macs/{ec_2_metadata["mac"]}/subnet-id')
        ec_2_metadata['security_group_name'] = get_instance_metadata(
            f'{EC2_METADATA_SERVICE}/security-groups')
        ec_2_metadata['security_group_id'] = get_instance_metadata(
            f'{EC2_METADATA_SERVICE}/network/interfaces/macs/{ec_2_metadata["mac"]}/security-group-ids')
        ec_2_metadata['ip'] = get_instance_metadata(f'{EC2_METADATA_SERVICE}/local-ipv4')
        logger.info(f"EC2 metadata: {ec_2_metadata}")
        return ec_2_metadata

