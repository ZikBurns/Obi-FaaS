import logging
import time
import threading
from queue import Queue
from .included_function.grpc_assets.split_grpc_pb2 import splitResponse
from .included_function.grpc_assets.split_grpc_pb2_grpc import SPLITRPCServicer, add_SPLITRPCServicer_to_server
logger = logging.getLogger()




class BatchClass():
    '''
    Class that represents a split to be executed by the orchestrator.
    :param SID: str. The split ID
    :param split: list. The split to be executed
    :param TID: str. The task manager ID
    :param start_time: float. The time when the split started
    :param end_time: float. The time when the split ended
    :param output: dict. The output of the split
    :param retries_count: int. The number of retries of the split
    '''

    def __init__(self, SID: str, split: list):
        self.SID = SID
        self.split = split
        self.TID = None
        self.start_time = None
        self.end_time = None
        self.output = {}
        self.retries_count = 0

    def to_dict(self):
        '''
        Returns the split as a dictionary
        :return: dict. The split as a dictionary
        '''
        return {
            "SID": self.SID,
            "split": self.split,
            "TID": self.TID,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "output": self.output,
            "retries_count": self.retries_count
        }

    def to_dict_reduced(self):
        '''
        Returns the split as a dictionary with reduced information
        :return: dict. The split as a dictionary
        '''
        return {
            "SID": self.SID,
            "TID": self.TID,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "retries_count": self.retries_count
        }


class ThreadSafeDict:
    '''
    Class that represents a thread-safe dictionary.
    '''

    def __init__(self):
        self.dict = {}
        self.lock = threading.Lock()

    def update(self, key, value):
        '''
        Updates the dictionary with the key and value. If key does not exist, it is created.
        :param key: str. The key to be updated
        :param value: any. The value to be updated
        '''
        with self.lock:
            self.dict[key] = value

    def get(self, key):
        '''
        Gets the value of the key
        :param key: str. The key to get the value
        :return: any. The value of the key
        '''
        with self.lock:
            return self.dict.get(key)

    def remove(self, key):
        '''
        Removes the key from the dictionary.
        :param key: str. The key to be removed
        '''
        with self.lock:
            if key in self.dict:
                del self.dict[key]

    def pop(self, key):
        '''
        Pops the key from the dictionary.
        :param key: str. The key to be popped
        :return: any. The value of the key
        '''
        with self.lock:
            return self.dict.pop(key)

    def sort(self):
        '''
        Sorts the dictionary by the values.
        '''
        try:
            with self.lock:
                self.dict = dict(sorted(self.dict.items(), key=lambda item: item[1]))
        except Exception as e:
            logger.error(f"Error sorting dictionary: {e}")

    def to_list(self):
        '''
        Returns the dictionary as a list.
        :return: list. The dictionary as a list
        '''
        with self.lock:
            return list(self.dict)

    def keys(self):
        '''
        Returns the keys of the dictionary.
        :return: list. The keys of the dictionary
        '''
        with self.lock:
            return self.dict.keys()

    def empty(self):
        '''
        Returns True if the dictionary is empty, False otherwise.
        :return: bool. True if the dictionary is empty, False otherwise
        '''
        with self.lock:
            return not bool(self.dict)

    def length(self):
        '''
        Returns the length of the dictionary.
        :return: int. The length of the dictionary
        '''
        with self.lock:
            return len(self.dict)

    def has(self, key):
        '''
        Returns True if the key is in the dictionary, False otherwise.
        '''
        with self.lock:
            return key in self.dict


class ThreadSafeList():
    def __init__(self):
        # initialize the list
        self._list = list()
        # initialize the lock
        self._lock = threading.Lock()

    # add a value to the list
    def append(self, value):
        # acquire the lock
        with self._lock:
            # append the value
            self._list.append(value)

    # remove and return the last value from the list
    def pop(self):
        # acquire the lock
        with self._lock:
            # pop a value from the list
            return self._list.pop()

    # read a value from the list at an index
    def get(self, index):
        # acquire the lock
        with self._lock:
            # read a value at the index
            return self._list[index]

    # return the number of items in the list
    def length(self):
        # acquire the lock
        with self._lock:
            return len(self._list)

    def empty(self):
        # acquire the lock
        with self._lock:
            return not bool(self._list)

    def extend(self, values):
        # acquire the lock
        with self._lock:
            self._list.extend(values)

    def to_list(self):
        # acquire the lock
        with self._lock:
            return self._list


class Speculator():
    '''
    Class that represents the speculator for the orchestrator.
    :param job_manager: JobManager. The job manager to be used
    :param speculation_multiplier: int. The multiplier for speculative execution
    :param speculation_quartile: float. The quartile for speculative execution
    :param speculation_interval: int. The interval for speculative execution
    '''

    def __init__(self, job_manager, speculation_multiplier, speculation_quartile, speculation_interval):
        self.job_manager = job_manager
        self.speculation_multiplier = speculation_multiplier
        self.speculation_quartile = speculation_quartile
        self.speculation_interval = speculation_interval
        self.speculation_thread = None
        self.tid_check_dict = ThreadSafeDict()
        for tid in range(self.job_manager.job.num_task_managers):
            self.tid_check_dict.update(str(tid + 1), 0)

    def speculate(self):
        """
        Speculates that a TID is taking too long and adds the split back to the pending queue.
        Every speculation_interval seconds:
        1- Calculates the median time of execution of the done splits.
        2- Orders the tid_check_dict by the time of the last check.
        3- Checks if the time of the last check is greater than the median time of execution multiplied by the speculation_multiplier.
        4- If it is, speculates that the TID is taking too long. If the split is not a copy, creates the copy and adds it to the pending queue.
        5- Checks if all splits are done. If they are, stops the speculation.
        """
        logger.info("Starting speculation.")
        while True:
            # Get the times of execution of the done splits
            tid_times = []
            for sid in self.job_manager.split_controller.done_splits.to_list():
                split = self.job_manager.split_controller.done_splits.get(sid)
                tid = split.TID
                start_time = split.start_time
                end_time = split.end_time
                tid_times.append((end_time - start_time))

            # Get median time of execution
            median_time = sorted(tid_times)[len(tid_times) // 2]

            # Order the tid_check_dict by the time of the last check
            self.tid_check_dict.sort()
            tids = self.tid_check_dict.to_list()

            # For each TID
            for tid in tids:
                tid_last_time = self.tid_check_dict.get(tid)
                if tid_last_time:
                    current_tid_time = time.time() - tid_last_time
                    # Check if the time of the last check is greater than the median time of execution multiplied by the speculation_multiplier
                    if current_tid_time > median_time * self.speculation_multiplier:
                        logger.info(f"Speculating that TID {tid} is taking too long. Time: {current_tid_time} seconds")
                        sid = self.job_manager.split_controller.find_sid_with_tid(tid)
                        if sid and sid.isdigit():
                            split_copy = self.job_manager.split_controller.create_copy(sid)
                            if split_copy:
                                self.job_manager.split_controller.put_split_pending(split_copy)
                                if self.job_manager.job.speculation_add_task_manager and split_copy.retries_count <= 1:
                                    self.job_manager.add_extra_task_executors(1)
                            else:
                                logger.debug(f"Split reached maximum retries.")

            if self.job_manager.split_controller.finished_all_splits():
                # Print the SIDs of the done splits
                logger.info(f"All splits are done: {self.job_manager.split_controller.done_splits.to_list()}")
                logger.info(f"Stopping speculation. Now waiting for Task Managers to finish...")
                break
            else:
                # There are still splits to be done
                sids = self.job_manager.split_controller.running_splits.to_list()
                logger.debug(f"Running splits now: {sids}")
            time.sleep(self.speculation_interval)

    def speculator_can_start(self):
        '''
        Returns True if the speculator can start, False otherwise.
        :return: bool. True if the speculator can start, False otherwise
        '''
        if self.speculation_thread is None:
            num_done_splits = self.job_manager.split_controller.done_splits.length()
            num_splits = len(self.job_manager.split_controller.splits)
            logger.debug(f"{num_done_splits / num_splits} currently to get {self.speculation_quartile} quartile.")
            if num_done_splits / num_splits > self.speculation_quartile:
                logger.info(
                    f"{num_done_splits}/{num_splits} splits are above the {self.speculation_quartile} quartile. Starting to speculate.")
                return True
            else:
                return False
        else:
            return False

    def start_speculator_thread(self):
        '''
        Starts the speculator thread if the number of done splits is above the speculation_quartile.
        '''
        self.speculation_thread = threading.Thread(target=self.speculate)
        self.speculation_thread.start()


class SplitEnumerator(SPLITRPCServicer):
    '''
    Class that represents the split enumerator for the orchestrator.
    :param job_manager: JobManager. The job manager to be used
    '''

    def __init__(self, job_manager):
        """
        Constructor for the SplitEnumerator class
        """
        self.job_manager = job_manager
        self.speculator = None
        if self.job_manager.job.speculation_enabled:
            self.speculator = Speculator(job_manager, job_manager.job.speculation_multiplier,
                                         job_manager.job.speculation_quartile, job_manager.job.speculation_interval)

    def get_outputs_request(self, request):
        '''
        Gets the outputs from the request.
        :param request: urlResponse. The request to get the outputs
        :return: dict. The outputs of the request
        '''
        outputs = {}
        # Get results from the request
        for result in request.outputs:
            try:
                result_value_dict = eval(result.value)
            except Exception as e:
                result_value_dict = result.value
            if result.key:
                outputs.update({result.key: result_value_dict})
        return outputs

    def pop_running_split(self, request_sid, outputs):
        '''
        Ends the running split by poping it from the running splits.
        :param request_sid: str. The SID of the request
        :param outputs: dict. The outputs of the request
        :return: Split. The split that was ended
        '''
        try:
            logger.debug(f"Poping request SID {request_sid} from the running splits.")
            done_split = self.job_manager.split_controller.running_splits.pop(request_sid)
        except Exception as e:
            logger.debug(f"Request SID {request_sid} not found in the running splits.")
            return None

        done_split.end_time = time.time()
        done_split.output = outputs
        return done_split

    def conclude_split(self, request_sid, done_split):
        '''
        Handles the split by adding it to the done splits.
        :param request_sid: str. The SID of the request
        :param done_split: Split. The split that was done
        '''
        # If it is a copy of another split
        if "_" in request_sid:
            # Get the original SID
            original_sid = request_sid.split("_")[0]
            # Get the original split
            original_split = self.job_manager.split_controller.running_splits.get(original_sid)
            # If the original slit is still running
            if original_split:
                logger.info(
                    f"The copy of split {original_sid} with SID {request_sid} is done before the original. Time: {done_split.end_time - done_split.start_time} seconds.")
                with self.job_manager.split_controller.running_splits.lock:
                    original_split.retries_count = self.job_manager.split_controller.max_split_retries
                done_split.SID = original_sid
                self.job_manager.split_controller.done_splits.update(original_sid, done_split)
            else:
                # If the original split is not running, it is because it was already done before
                logger.info(f"Original split {original_sid} finished before the copy.")
        else:
            # If the split is not a copy, add it to the done splits
            self.job_manager.split_controller.done_splits.update(request_sid, done_split)
            logger.info(f"Split {request_sid} done. Time: {done_split.end_time - done_split.start_time} seconds")

    def Assign(self, request, context):
        '''
        Assigns the split to the task manager.
        :param request: urlResponse. The request to be assigned
        :param context: grpc context. The context of the request
        :return: urlResponse. The response of the request
        '''
        if self.speculator:
            self.speculator.tid_check_dict.update(request.tid, time.time())
        keep_alive = request.keep_alive
        if keep_alive:
            logger.debug(f"Keep alive from TID {request.tid}")
            return splitResponse(inputs=[], sid=None)

        # If the request carries a finished split
        sids = self.job_manager.split_controller.running_splits.to_list()
        logger.debug(f"Running splits when requested: {sids}")

        # If the request carries outputs
        if request.outputs and request.sid:
            outputs = self.get_outputs_request(request)
            done_split = self.pop_running_split(request.sid, outputs)
            if done_split:
                # Save the output of the split
                self.conclude_split(request.sid, done_split)

        # If there is speculator
        if self.speculator and request.tid:
            # Update the time of the last check of the TID
            self.speculator.tid_check_dict.update(request.tid, time.time())
            if self.speculator.speculator_can_start():
                # If the speculator can start, start the speculator thread
                self.speculator.start_speculator_thread()

        # Get next split from the split controller
        split = self.job_manager.split_controller.get_next_split()

        # Put split in running state if there is a split
        split = self.job_manager.split_controller.put_split_running(split, request.tid)

        # If there is a split to run, send it to the task manager
        if split:
            logger.info(f"Sending split {split.SID} to task manager {request.tid}")
            print(split.split)
            return splitResponse(inputs=split.split, sid=split.SID)
        else:
            # If there are no more splits to run, end the task manager
            logger.info(f"Ending task manager {request.tid}")
            if self.speculator:
                self.speculator.tid_check_dict.remove(request.tid)
                tids = self.speculator.tid_check_dict.to_list()
                logger.debug(f"Running Task Managers: {tids}")
            return splitResponse(inputs=[], sid=None)


class SplitController():
    '''
    Class with the structures and operations to handle the three states of the splits: pending, running and done.
    :param job_input: list. The input data to be processed
    :param split_size: int. The size of the splits
    :param max_split_retries: int. The maximum number of retries for a split
    '''

    def __init__(self, job_input, split_size, max_split_retries, heterogeneous_splits=None):
        self.pending_queue = Queue()
        self.running_splits = ThreadSafeDict()
        self.done_splits = ThreadSafeDict()
        split_inputs = [job_input[i:i + split_size] for i in range(0, len(job_input), split_size)]
        self.splits = [BatchClass(str(i + 1), split) for i, split in enumerate(split_inputs)]

        if heterogeneous_splits:
            self.splits = [BatchClass(str(i + 1), split) for i, split in enumerate(heterogeneous_splits)]

        self.split_size = split_size
        self.max_split_retries = max_split_retries
        logger.info(f"Created {len(self.splits)} splits with size {split_size}")
        sids = [split.SID for split in self.splits]
        logger.debug(f"Splits: {sids}")

    def initialize_pending_queue(self):
        '''
        Initializes the pending queue with the splits.
        '''
        for split in self.splits:
            self.pending_queue.put(split)

    def has_pending_or_running_splits(self):
        '''
        Returns True if there are pending or running splits, False otherwise.
        '''
        return not (self.pending_queue.empty() and self.running_splits.empty())

    def finished_all_splits(self):
        '''
        Returns True if all splits are done, False otherwise.
        '''
        done_splits = self.done_splits.length()
        if done_splits == len(self.splits):
            logger.info(f"All splits finished. Done splits: {done_splits}/{len(self.splits)}")
            return True

    def create_copy(self, sid):
        '''
        Creates a copy of a split if it is already in the running splits. If the split has already been retried the maximum number of times, it returns None.
        :param sid: str. The SID of the split
        :return: Split. The copy of the split. None if the split has already been retried the maximum number of times.
        '''
        split = self.running_splits.get(sid)
        current_retries_count = split.retries_count
        # If the split exists and has not been retried the maximum number of times
        if split and current_retries_count < self.max_split_retries:
            split.retries_count += 1
            current_retries_count = split.retries_count
            # Create a new SID for the copy.
            new_sid = f"{str(int(split.SID))}_{current_retries_count}"
            logger.info(
                f"Split {split.SID} is already in the running splits. Creating a copy of the split with SID {new_sid} (retry {split.retries_count}).")
            new_split = BatchClass(new_sid, split.split)
            new_split.retries_count = current_retries_count
            return new_split
        else:
            return None

    def order_running_splits(self):
        '''
        Orders the running splits by the start time.
        '''
        start = time.time()
        with self.running_splits.lock:
            running_splits_list = [(sid, split) for sid, split in self.running_splits.dict.items()]
            sorted_running_splits_list = sorted(running_splits_list, key=lambda x: x[1].start_time)
            sorted_running_splits_dict = {sid: split for sid, split in sorted_running_splits_list}
            self.running_splits.dict = sorted_running_splits_dict
        end = time.time()
        logger.debug(f"Ordered running splits in {end - start} seconds.")
        pass

    def get_next_split(self):
        '''

        '''
        if self.finished_all_splits() or self.pending_queue.empty():
            return None
        else:
            split = self.pending_queue.get()
            return split

    def put_split_running(self, split, tid):
        if split:
            logger.debug(f"Split {split.SID} is not in the running splits. Adding it to the running splits.")
            split.TID = tid
            split.start_time = time.time()
            self.running_splits.update(split.SID, split)
            return split
        else:
            return None

    def put_split_pending(self, split):
        if split:
            logger.debug(f"Adding {split.SID} to the pending queue.")
            self.pending_queue.put(split)
            return split
        else:
            return None

    def find_sid_with_tid(self, tid):
        for sid in self.running_splits.to_list():
            split = self.running_splits.get(sid)
            with self.running_splits.lock:
                if split.TID == tid:
                    return sid
        return None

    def move_split_to_pending(self, sid):
        split = self.running_splits.get(sid)
        self.running_splits.remove(sid)
        self.pending_queue.put(split)
        logger.info(f"Split {sid} moved from running to pending queue")

