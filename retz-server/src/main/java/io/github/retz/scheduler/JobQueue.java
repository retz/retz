/**
 *    Retz
 *    Copyright (C) 2016-2017 Nautilus Technologies, Inc.
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
import io.github.retz.misc.LogUtil;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.ResourceQuantity;
import io.github.retz.protocol.exception.JobNotFoundException;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
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
        int latest;
        try {
            latest = Database.getInstance().getLatestJobId();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        COUNTER = new AtomicInteger(latest + 1);
    }

    private JobQueue() {
    }

    public static List<Job> list(String user, Job.JobState state, Optional<String> tag, int limit) throws IOException {
        return Database.getInstance().listJobs(user, state, tag, limit);
    }

    // As this is issued by server without any transaction, this id may have
    // skip, but monotonically increasing afaik
    public static int issueJobId() {
        return COUNTER.getAndIncrement(); // Just have to be unique
    }

    public static void push(Job job) throws IOException {
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
        try {
            Database.getInstance().updateJobs(jobs);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Optional<Job> cancel(int id, String reason) throws IOException, JobNotFoundException {
        Optional<Job> maybeJob = getJob(id);
        if (maybeJob.isPresent()) {
            Database.getInstance().updateJob(id, (job -> {
                if (job.state() == Job.JobState.KILLED || job.state() == Job.JobState.FINISHED) {
                    return Optional.empty();
                }
                job.killed(TimestampHelper.now(), Optional.empty(), reason);
                LOG.info("Job id={} has been canceled.", id);
                return Optional.of(job);
            }));
            return getJob(id);
        }
        return maybeJob;
    }

    // @doc take as much jobs as in the max cpu/memMB
    public static List<Job> findFit(List<String> orderBy, ResourceQuantity total) {
        try {
            return Database.getInstance().findFit(orderBy, total.getCpu(), total.getMemMB());
        } catch (IOException e) {
            LogUtil.warn(LOG, "JobQueue.findFit() failed, returns emptyList", e);
            return Collections.emptyList();
        }
    }

    public static List<Job> queued(int limit) throws IOException {
        return Database.getInstance().queued(limit);
    }

    public synchronized static Optional<Job> getJob(int id) throws IOException {
        return Database.getInstance().getJob(id);
    }

    public synchronized static void clear() throws IOException {
        Database.getInstance().deleteAllJob(Integer.MAX_VALUE);
    }

    public static int size() throws IOException {
        return Database.getInstance().countJobs();
    }

    public static void starting(Job job, Optional<String> url, String taskId) {
        try {
            Database.getInstance().setJobStarting(job.id(), url, taskId);
        } catch (IOException | JobNotFoundException e) {
            LogUtil.warn(LOG, "JobQueue.starting() failed", e);
        }
    }

    static void started(String taskId, Optional<String> maybeUrl) throws IOException, JobNotFoundException {
        Optional<Job> maybeJob = Database.getInstance().getJobFromTaskId(taskId);
        Database.getInstance().updateJob(maybeJob.get().id(), job -> {
            job.started(taskId, maybeUrl, TimestampHelper.now());
            return Optional.of(job);
        });
    }

    public static Optional<Job> getFromTaskId(String taskId) {
        try {
            return Database.getInstance().getJobFromTaskId(taskId);
        } catch (IOException e) {
            LogUtil.warn(LOG, "JobQueue.getFromTaskId() failed, returns empty", e);
            return Optional.empty();
        }
    }

    public static void retry(String taskId, String reason) throws JobNotFoundException {
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
            LogUtil.warn(LOG, "JobQueue.retry() failed", e);
        }
    }

    // Whether it's success, fail, or killed
    static void finished(String taskId, Optional<String> maybeUrl, int ret, String finished) throws JobNotFoundException {
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
            LogUtil.warn(LOG, "JobQueue.finished() failed", e);
        }
    }

    public static void failed(String taskId, Optional<String> maybeUrl, String msg) throws JobNotFoundException {
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
            LogUtil.warn(LOG, "JobQueue.failed() failed", e);
        }
    }

    public static int countRunning() {
        try {
            return Database.getInstance().countRunning();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    //TODO: make this happen.... with admin APIs
    public static void compact() {
        throw new AssertionError("Not yet implemented");
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
