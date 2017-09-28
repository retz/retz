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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * On memory job queue to mediate job execution requests and resources
 * TODO: make all these data tolerant against node or network failure
 * TODO: FIXME: re-design all exception handling, which to supress / which to return to client
 */
public final class JobQueue {
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
    public static List<Job> findFit(List<String> orderBy, ResourceQuantity total) throws IOException {
        return Database.getInstance().findFit(orderBy, total.getCpu(), total.getMemMB());
    }

    public static List<Job> queued(int limit) throws IOException {
        return Database.getInstance().queued(limit);
    }

    public static synchronized Optional<Job> getJob(int id) throws IOException {
        return Database.getInstance().getJob(id);
    }

    public static synchronized void clear() throws IOException {
        Database.getInstance().deleteAllJob(Integer.MAX_VALUE);
    }

    public static int size() throws IOException {
        return Database.getInstance().countJobs();
    }

    public static void starting(Job job, Optional<String> url, String taskId) throws IOException {
        try {
            Database.getInstance().setJobStarting(job.id(), url, taskId);
        } catch (JobNotFoundException e) {
            LOG.warn("JobQueue.starting() failed", e);
        }
    }

    static void started(String taskId, String slaveId, Optional<String> maybeUrl) throws IOException, JobNotFoundException {
        Optional<Job> maybeJob = Database.getInstance().getJobFromTaskId(taskId);
        Database.getInstance().updateJob(maybeJob.get().id(), job -> {
            // the state of job is already started; don't update timestamp
            String ts = (job.state() == Job.JobState.STARTED) ? job.started() : TimestampHelper.now();
            job.started(taskId, slaveId, maybeUrl, ts);
            return Optional.of(job);
        });
    }

    public static Optional<Job> getFromTaskId(String taskId) throws IOException {
        return Database.getInstance().getJobFromTaskId(taskId);
    }

    public static void retry(String taskId, String reason) throws IOException, JobNotFoundException {
        Optional<Job> maybeJob = Database.getInstance().getJobFromTaskId(taskId);
        if (maybeJob.isPresent()) {
            int threshold = 5;
            Database.getInstance().updateJob(maybeJob.get().id(), job -> {
                if (job.retry() > threshold) {
                    String msg = String.format("Giving up Job retry: %d / id=%d, last reason='%s'", threshold, job.id(),
                            reason);
                    LOG.warn(msg);
                    job.killed(TimestampHelper.now(), Optional.empty(), msg);

                } else {
                    job.doRetry();
                    LOG.info("Scheduled retry {}/{} of Job(taskId={}), reason='{}'", job.retry(), threshold,
                            job.taskId(), reason);
                }
                return Optional.of(job);
            });
        }
    }

    // Whether it's success, fail, or killed
    static void finished(String taskId, Optional<String> maybeUrl, int ret, String finished) throws IOException, JobNotFoundException {
        Optional<Job> maybeJob = Database.getInstance().getJobFromTaskId(taskId);
        if (maybeJob.isPresent()) {
            Database.getInstance().updateJob(maybeJob.get().id(), job -> {
                job.finished(finished, maybeUrl, ret);
                return Optional.of(job);
            });
            LOG.info("Job id={} has finished at {} with return value={}", maybeJob.get().id(), finished, ret);
        }
    }

    public static void failed(String taskId, Optional<String> maybeUrl, String msg) throws IOException, JobNotFoundException {
        Optional<Job> maybeJob = Database.getInstance().getJobFromTaskId(taskId);
        if (maybeJob.isPresent()) {
            Database.getInstance().updateJob(maybeJob.get().id(), job -> {
                job.killed(TimestampHelper.now(), maybeUrl, msg);
                return Optional.of(job);
            });
            LOG.info("Job id={} has failed: {}", maybeJob.get().id(), msg);
        }
    }

    public static int countRunning() throws IOException {
        return Database.getInstance().countRunning();
    }

}
