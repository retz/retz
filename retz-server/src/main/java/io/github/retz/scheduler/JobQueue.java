/**
 *    Retz
 *    Copyright (C) 2016 Nautilus Technologies, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package io.github.retz.scheduler;

import io.github.retz.cli.TimestampHelper;
import io.github.retz.db.Database;
import io.github.retz.db.JobNotFoundException;
import io.github.retz.protocol.StatusResponse;
import io.github.retz.protocol.data.Job;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * On memory job queue to mediate job execution requests and resources
 * TODO: make all these data tolerant against node or network failure
 * TODO: FIXME: re-design all exception handling, which to supress / which to return to client
 */
public class JobQueue {
    private static final Logger LOG = LoggerFactory.getLogger(JobQueue.class);
    private static final AtomicInteger COUNTER;

    static {
        int latest = Database.getInstance().getLatestJobId();
        COUNTER = new AtomicInteger(latest + 1);
    }

    private JobQueue() {
    }

    public static List<Job> getAll(String id) {
        try {
            return Database.getInstance().getAllJobs(id);
        } catch (IOException e) {
            LOG.error(e.toString());
            throw new RuntimeException("Database is not available currently");
        }
    }

    // As this is issued by server without any transaction, this id may have
    // skip, but monotonically increasing afaik
    public static int issueJobId() {
        return COUNTER.getAndIncrement(); // Just have to be unique
    }

    public static void push(Job job) throws InterruptedException {
        // TODO: set a cap of queue
        Database.getInstance().safeAddJob(job);
    }

    public static void cancelAll(List<Job> jobs) {
        for (Job job : jobs) {
            if (job.state() != Job.JobState.KILLED) {
                LOG.warn("Job state isn't yet KILLED: changing here from {}", job);
                job.killed(TimestampHelper.now(), Optional.empty(), "Changed via JobQueue.cancelAll check");
            }
        }
        Database.getInstance().updateJobs(jobs);
    }

    public static Optional<String> cancel(int id, String reason) throws SQLException, IOException, JobNotFoundException {
        Database.getInstance().updateJob(id, (job -> {
            job.killed(TimestampHelper.now(), Optional.empty(), reason);
            LOG.info("Job id={} has been canceled.", id);
            return Optional.of(job);
        }));
        Optional<Job> maybeJob = getJob(id);
        if (maybeJob.isPresent()) {
            return Optional.ofNullable(maybeJob.get().taskId());
        } else {
            return Optional.empty();
        }
    }

    // @doc take as much jobs as in the max cpu/memMB
    public static List<Job> findFit(int cpu, int memMB) {
        try {
            return Database.getInstance().findFit(cpu, memMB);
        } catch (IOException e) {
            LOG.error(e.toString());
            return new LinkedList<>();
        }
    }

    public static List<Job> queued(int limit) throws SQLException, IOException {
        return Database.getInstance().queued(limit);
    }

    public synchronized static Optional<Job> getJob(int id) {
        try {
            return Database.getInstance().getJob(id);
        } catch (IOException e) {
            LOG.error(e.toString());
        }
        return Optional.empty();
    }

    public synchronized static void clear() {
        Database.getInstance().deleteAllJob(Integer.MAX_VALUE);
    }

    public static int size() {
        return Database.getInstance().countJobs();
    }

    public static void starting(Job job, Optional<String> url, String taskId) {
        try {
            Database.getInstance().setJobStarting(job.id(), url, taskId);
        } catch (IOException e) {

        } catch (SQLException e) {

        } catch (JobNotFoundException e) {

        }
    }

    static void started(String taskId, Optional<String> maybeUrl) throws IOException, SQLException, JobNotFoundException {
        Optional<Job> maybeJob = Database.getInstance().getJobFromTaskId(taskId);
        if (maybeJob.isPresent()) {
            Database.getInstance().updateJob(maybeJob.get().id(), job -> {
                job.started(taskId, maybeUrl, TimestampHelper.now());
                return Optional.of(job);
            });
        }
    }

    public static Optional<Job> getFromTaskId(String taskId) {
        try {
            return Database.getInstance().getJobFromTaskId(taskId);
        } catch (IOException e) {
            LOG.error(e.toString());
            return Optional.empty();
        }
    }

    public static void retry(String taskId, String reason) throws SQLException, JobNotFoundException {
        try {
            Optional<Job> maybeJob = Database.getInstance().getJobFromTaskId(taskId);
            if (maybeJob.isPresent()) {
                int threshold = 5;
                Database.getInstance().updateJob(maybeJob.get().id(), job -> {
                    if (job.retry() > threshold) {
                        String msg = String.format("Giving up Job retry: %d / id=%d, last reason='%s'", threshold, job.id(), reason);
                        LOG.warn(msg);
                        job.killed(TimestampHelper.now(), Optional.empty(), msg);

                    } else {
                        job.doRetry();
                        LOG.info("Scheduled retry {}/{} of Job(taskId={}), reason='{}'",
                                job.retry(), threshold, job.taskId(), reason);
                    }
                    return Optional.of(job);
                });
            }
        } catch (IOException e) {
            LOG.warn("Retry failed: {}", e.toString());
        }
    }

    // Whether it's success, fail, or killed
    static void finished(String taskId, Optional<String> maybeUrl, int ret, String finished) throws SQLException, JobNotFoundException {
        try {
            Optional<Job> maybeJob = Database.getInstance().getJobFromTaskId(taskId);
            if (maybeJob.isPresent()) {
                Database.getInstance().updateJob(maybeJob.get().id(), job -> {
                    job.finished(finished, maybeUrl, ret);
                    return Optional.of(job);
                });
                LOG.info("Job id={} has finished at {} with return value={}", maybeJob.get().id(), finished, ret);
            }
        } catch (IOException e) {
            LOG.error(e.toString());
        }
    }

    public static void failed(String taskId, Optional<String> maybeUrl, String msg) throws SQLException, JobNotFoundException {
        try {
            Optional<Job> maybeJob = Database.getInstance().getJobFromTaskId(taskId);
            if (maybeJob.isPresent()) {
                Database.getInstance().updateJob(maybeJob.get().id(), job -> {
                    job.killed(TimestampHelper.now(), maybeUrl, msg);
                    return Optional.of(job);
                });
                LOG.info("Job id={} has failed: {}", maybeJob.get().id(), msg);
            }
        } catch (IOException e) {
            LOG.error(e.toString());
        }
    }

    public static int countRunning() {
        return Database.getInstance().countRunning();
    }

    //TODO: make this happen.... with admin APIs
    public static void compact() {
        throw new AssertionError("Not yet implemented");
    }

    // Methods for test
    public static void setStatus(StatusResponse response) {
        response.setStatus(JobQueue.size(), countRunning());
    }

    /*
     * Update local job state according to status in Mesos
     *
                      | STARTING   | STARTED
        --------------+------------+------------
        TASK_RUNNING  | STARTED    | keep
        TASK_STAGING  | keep       | -
        TASK_STARTING | keep       | -
        TASK_LOST     | QUEUED     | QUEUED
        TASK_FINISHED | FINISHED   | FINISHED
        TASK_KILLED   | KILLED     | KILLED
        TASK_ERROR    | KILLED     | KILLED
     */
    public static Job updateJobStatus(Job job, String stateInMesos) {
        Protos.TaskState taskState = Protos.TaskState.valueOf(stateInMesos);
        switch (taskState.getNumber()) {
            case Protos.TaskState.TASK_STAGING_VALUE:
            case Protos.TaskState.TASK_STARTING_VALUE:
                break;
            case Protos.TaskState.TASK_RUNNING_VALUE:
                if (job.state() == Job.JobState.STARTING) {
                    job.started(job.taskId(), Optional.empty(), TimestampHelper.now());
                }
                break;
            case Protos.TaskState.TASK_LOST_VALUE:
                job.doRetry();
                break;
            case Protos.TaskState.TASK_FINISHED_VALUE:
                //TODO: recover all information right here
                job.finished(TimestampHelper.now(), Optional.empty(), 0);
                break;
            case Protos.TaskState.TASK_KILLED_VALUE:
            case Protos.TaskState.TASK_KILLING_VALUE:
                job.killed(TimestampHelper.now(), Optional.empty(), "KILLED");
                break;
            case Protos.TaskState.TASK_ERROR_VALUE:
                job.killed(TimestampHelper.now(), Optional.empty(), "ERROR");
                break;
            default:
                LOG.error("Unknown state: {}", stateInMesos);
        }
        return job;
    }
}
