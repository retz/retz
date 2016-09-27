/**
 * Retz
 * Copyright (C) 2016 Nautilus Technologies, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.retz.scheduler;

import io.github.retz.cli.TimestampHelper;
import io.github.retz.protocol.StatusResponse;
import io.github.retz.protocol.data.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * On memory job queue to mediate job execution requests and resources
 * TODO: make all these data tolerant against node or network failure
 */
public class JobQueue {
    private static final Logger LOG = LoggerFactory.getLogger(JobQueue.class);
    private static final AtomicInteger COUNTER;

    static {
        int latest = Database.getLatestJobId();
        COUNTER = new AtomicInteger(latest + 1);
    }

    private JobQueue() {
    }

    public static List<Job> getAll() {
        try {
            return Database.getAllJobs();
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
        Database.safeAddJob(job);
    }

    public static Optional<String> cancel(int id, String reason) {
        Database.updateJob(id, (job -> {
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
    public synchronized static List<Job> findFit(int cpu, int memMB) {
        try {
            return Database.findFit(cpu, memMB);
        } catch (IOException e) {
            LOG.error(e.toString());
            return new LinkedList<>();
        }
    }

    public synchronized static Optional<Job> getJob(int id) {
        try {
            return Database.getJob(id);
        } catch (IOException e) {
            LOG.error(e.toString());
        }
        return Optional.empty();
    }

    public synchronized static void clear() {
        Database.deleteAllJob(Integer.MAX_VALUE);
    }

    public static int size() {
        return Database.countJobs();
    }

    public static void starting(Job job, Optional<String> url, String taskId) {
        Database.setJobStarting(job.id(), url, taskId);
    }

    static void started(String taskId, Optional<String> maybeUrl) {
        try {
            Optional<Job> maybeJob = Database.getJobFromTaskId(taskId);
            if (maybeJob.isPresent()) {
                Database.updateJob(maybeJob.get().id(), job -> {
                    job.started(taskId, maybeUrl, TimestampHelper.now());
                    return Optional.of(job);
                });
            }
        } catch (IOException e) {
            LOG.warn("Couldn't update status of {} to started: {}", taskId, e.toString());
        }
    }

    public static Optional<Job> getFromTaskId(String taskId) {
        try {
            return Database.getJobFromTaskId(taskId);
        } catch (IOException e) {
            LOG.error(e.toString());
            return Optional.empty();
        }
    }

    public static void retry(String taskId, String reason) {
        try {
            Optional<Job> maybeJob = Database.getJobFromTaskId(taskId);
            if (maybeJob.isPresent()) {
                int threshold = 5;
                Database.updateJob(maybeJob.get().id(), job -> {
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
    static void finished(String taskId, Optional<String> maybeUrl, int ret, String finished) {
        try {
            Optional<Job> maybeJob = Database.getJobFromTaskId(taskId);
            if (maybeJob.isPresent()) {
                Database.updateJob(maybeJob.get().id(), job -> {
                    job.finished(finished, maybeUrl, ret);
                    return Optional.of(job);
                });
                LOG.info("Job id={} has finished at {} with return value={}", maybeJob.get().id(), finished, ret);
            }
        } catch (IOException e) {
            LOG.error(e.toString());
        }
    }

    public static void failed(String taskId, Optional<String> maybeUrl, String msg) {
        try {
            Optional<Job> maybeJob = Database.getJobFromTaskId(taskId);
            if (maybeJob.isPresent()) {
                Database.updateJob(maybeJob.get().id(), job -> {
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
        return Database.countRunning();
    }

    //TODO: make this happen.... with admin APIs
    public static void compact() {
        throw new AssertionError("Not yet implemented");
    }

    // Methods for test
    public static void setStatus(StatusResponse response) {
        response.setStatus(JobQueue.size(), countRunning());
    }
}
