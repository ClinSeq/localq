"""
LocalQServer
"""

import time
import threading

import sys

import networkx.drawing.nx_pydot as nx_pydot

from localq.status import Status
import networkx as nx
import os
from localq.job_prioritizer import JobPrioritizer as Prioritizer
from localq.job import Job


class LocalQServer():
    def __init__(self, num_cores_available, interval, priority_method="fifo", use_shell=False):
        self.num_cores_available = int(num_cores_available)
        self.interval = float(interval)
        self.priority_method = priority_method
        self._last_jobid = 0
        self.use_shell = use_shell
        self.graph = nx.MultiDiGraph()
        self.pid = os.getpid()

    def get_pid(self):
        return self.pid

    def add(self, cmd, num_cores, rundir=".", stdout=None, stderr=None, name=None, dependencies=None):
        """
        Add a job to the queue. Dependencies need to be empty or all have status COMPLETED for
        a job to run.
        :param cmd: Command line string or list of command parts
        :param num_cores: Number of cores needed for this job
        :param rundir: Directory where to run the job
        :param stdout: Filename to write stdout
        :param stderr: Filename to write stderr
        :param name: Job name
        :param dependencies: List of job ids of dependencies.
        :return: job ID if successfully submitted. None if number of requested cores
        is bigger the server's total core count.
        """

        job_id = self._get_new_jobid()
        job = Job(job_id, cmd, num_cores, stdout=stdout, stderr=stderr,
                  rundir=rundir, name=name, use_shell=self.use_shell,
                  dependencies=dependencies)

        # if number of requested cores is bigger then the number of cores available to the system, fail submission.
        if job.num_cores > self.num_cores_available:
            return None
        else:
            self.graph.add_node(job)
            # If we have no dependencies get an empty list.
            for dependency_job_id in dependencies or []:
                dependency_job = self.get_job_with_id(dependency_job_id)
                self.graph.add_edge(dependency_job, job)
            return job.jobid

    def add_script(self, script, num_cores, rundir=".", stdout=None, stderr=None, name=None, dependencies=None):
        cmd = ["sh", script]
        return self.add(cmd, num_cores, stdout=stdout, stderr=stderr, rundir=rundir, name=name,
                        dependencies=dependencies)

    def _get_new_jobid(self):
        """
        Method to get a new jobid to use for a job. Increments internal jobid tracker variable each time it's called.
        :return: a job id (int)
        """
        self._last_jobid += 1
        return self._last_jobid

    def get_status_all(self):
        """
        Get the status of all know jobs.
        :return: a dict with job-ids as keys, and their status as values.
        """
        jobs_and_status = map(lambda job: (job.jobid, job.status()), self.graph.nodes())
        return dict(jobs_and_status)

    def list_queue(self):
        retstr = "\t".join(["JOBID", "PRIO", "STATUS", "NUM_CORES", "START_TIME", "END_TIME", "NAME", "CMD", "\n"])
        for j in self.jobs():
            retstr += j.info() + "\n"
        return retstr.strip("\n")

    def jobs(self):
        """
        get all jobs
        :return: a list of all jobs added to this server
        """
        return self.graph.nodes()

    def get_status(self, jobid):
        """
        Check in which state (list) a job is.
        :param jobid: a job id to search for
        :return: Any of Status.PENDING, RUNNING, COMPLETED or FAILED.
        If no job with the given ID is found, Status.NOT_FOUND is returned.
        """
        job = self.get_job_with_id(int(jobid))
        if job:
            return job.status()
        else:
            return Status.NOT_FOUND

    def stop_job_with_id(self, jobid):
        """
        stop a job with a give ID
        :param jobid: Job ID of job to stop
        :return: the jobs new ID after it was tried to be stopped (None if it can not find the job)
        """
        job = self.get_job_with_id(int(jobid))
        if job:
            job.kill()
            return jobid
        else:
            return None

    def stop_all_jobs(self):
        for job in self.jobs():
            print "Sending SIGTERM to " + str(job.jobid)
            job.kill()

    def _prioritize(self, pending_jobs):
        use_this_prioritzer = getattr(Prioritizer, self.priority_method)
        return use_this_prioritzer(
            pending_jobs,
            cores_available=self.num_cores_available,
            cores_idle=self.cores_idle())

    def cores_idle(self):
        n_cores_busy = self.get_n_cores_booked()
        return self.num_cores_available - n_cores_busy

    def get_n_cores_booked(self):
        n_cores_booked = 0
        running_jobs = [j for j in self.jobs() if j.status() == Status.RUNNING]
        for job in running_jobs:
            n_cores_booked += job.num_cores
        return n_cores_booked

    def run(self):
        sys.stderr.write("Starting localqd with {0} available cores.\n".format(self.num_cores_available))
        sys.stderr.write("Checking queue every {0} seconds\n\n".format(self.interval))

        def check_queue():
            while True:
                pending_jobs = self.get_runnable_jobs()
                prioritized_jobs = self._prioritize(pending_jobs)

                # check if new jobs can be started
                for job in prioritized_jobs:
                    if self.cores_idle() >= job.num_cores:
                        job.run()
                    else:  # If highest-priority job can't start, don't try to start next job until next cycle
                        continue

                time.sleep(self.interval)

        thread = threading.Thread(target=check_queue)
        thread.setDaemon(True)
        thread.start()

    def has_finished(self):
        has_f = True
        s = self.get_status_all()
        for jobid in s:
            # if any job has status running or pending,
            # the pipeline needs to run
            if s[jobid] in [Status.RUNNING, Status.PENDING]:
                has_f = False
        return has_f


    def wait(self):
        while True:
            time.sleep(self.interval)
            if self.has_finished():
                break

    def get_server_cores(self):
        """
        :return: Number of cores available to the server
        """
        return self.num_cores_available

    def write_dot(self, f):
        """ Write a dot file with the pipeline graph
        :param f: file to write
        :return: None
        """
        nx_pydot.write_dot(self.graph, f)

    def get_ordered_jobs(self):
        """ Method to order the tasks in the pipeline
        :return: An array of paths for the runner to run
        """
        if not nx.is_directed_acyclic_graph(self.graph):
            raise Exception("The submitted pipeline is not a DAG. Check the pipeline for loops.")

        ordered_jobs = nx.topological_sort(self.graph)
        return ordered_jobs

    def get_runnable_jobs(self):
        """
        Get a list of pending jobs that are ready to be run based on dependencies
        :return: List of Jobs
        """
        pending_jobs = [j for j in self.get_ordered_jobs() if j.status() == Status.PENDING]
        ready_jobs = []
        for job in pending_jobs:
            # if there are no dependencies, the job is always ready
            if not job.dependencies:
                ready_jobs.append(job)
            else:
                # get a list containing the status of all dependencies
                dependency_status = [self.get_status(depid) for depid in job.dependencies]
                # if a uniqiefied list contains a single element, and that element is "COMPLETED"
                # then the job is ready
                if len(set(dependency_status)) == 1 and dependency_status[0] == Status.COMPLETED:
                    ready_jobs.append(job)
        return ready_jobs

    def get_node_with_name(self, name_to_find):
        """
        Function to find a node in a graph by it's taskname
        :param name_to_find: name of task to get
        :return: the Job if it's found, None otherwise
        """
        for job in self.jobs():
            if job.name == name_to_find:
                return job
        return None

    def get_job_with_id(self, jobid_to_find):
        """
        Function to find a node in a graph by it's job id
        :param jobid_to_find: name of task to get
        :return: the Job if it's  found, None otherwise
        """
        for job in self.jobs():
            if job.jobid == jobid_to_find:
                return job
        return None
