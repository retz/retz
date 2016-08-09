/**
 *    Retz
 *    Copyright (C) 2016 Nautilus Technologies, KK.
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

import io.github.retz.protocol.Job;
import io.github.retz.protocol.StatusResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * On memory job queue to mediate job execution requests and resources
 * TODO: make all these data tolerant against node or network failure
 */
public class JobQueue {
    private static final Logger LOG = LoggerFactory.getLogger(JobQueue.class);

    // CMT: I thought capital case was for static const, like in C++
    // REVIEW: static final fields should have CAPITAL_CASE name
    private static final BlockingQueue<Job> JOB_QUEUE = new LinkedBlockingDeque<>();
    private static final AtomicInteger COUNTER = new AtomicInteger(1);

    private static final ConcurrentHashMap<String, Job> running = new ConcurrentHashMap<>(); // TaskID#getValue() -> Job
    private static final ConcurrentLinkedDeque<Job> finished = new ConcurrentLinkedDeque<>();

    private JobQueue() {
    }

    public static BlockingQueue<Job> get() {
        return JOB_QUEUE;
    }

    public static List<Job> getAll() {
        return new ArrayList<>(get());
    }

    public static int issueJobId() {
        return COUNTER.getAndIncrement(); // Just have to be unique
    }
    public static void push(Job job) throws InterruptedException {
        // TODO: set a cap of queue
        BlockingQueue<Job> queue = get();
        queue.put(job);
    }

    public static Optional<Job> pop() {
        BlockingQueue<Job> queue = get();
        return Optional.ofNullable(queue.poll());
    }

    // @doc take as much jobs as in the max cpu/memMB
    public synchronized static List<Job> popMany(int cpu, int memMB) {
        int totalCpu = 0;
        int totalMem = 0;
        List<Job> ret = new LinkedList<>();
        Job job;
        while (totalCpu <= cpu && totalMem <= memMB){
            job = get().peek();
            if (job == null) {
                break;
            } else if (totalCpu + job.cpu().getMax() <= cpu && totalMem + job.memMB().getMax() <= memMB) {
                ret.add(get().remove());
                totalCpu += job.cpu().getMax();
                totalMem += job.memMB().getMax();
            } else if (totalCpu + job.cpu().getMin() < cpu && totalMem + job.memMB().getMin() < memMB) {
                ret.add(get().remove());
                break;
            } else {
                break;
            }
        }
        return ret;
    }

    public synchronized static Optional<Job> getJob(int id) {
        for(Job job : get()) {
            if (id == job.id()) {
                return Optional.of(job);
            }
        }
        for(Map.Entry<String, Job> entry : running.entrySet()) {
            if (id == entry.getValue().id()) {
                return Optional.of(entry.getValue());
            }
        }
        for(Job job : finished) {
            if (id == job.id()) {
                return Optional.of(job);
            }
        }
        return Optional.empty();
    }

    public static String now() {
        return Calendar.getInstance().getTime().toString();
    }

    public synchronized static void clear() {
        JOB_QUEUE.clear();
    }

    public static int size() {
        return get().size();
    }

    public static void start(String taskId, Job job) {
        running.put(taskId, job);
    }

    public static void recoverRunning() {
        for(Iterator<Map.Entry<String, Job>> it = running.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, Job> entry = it.next();
            try {
                push(entry.getValue());
            } catch (InterruptedException e) {
                LOG.warn("Interruption: Job(id={}) must be rerun ({})", entry.getValue().id(), e.getMessage());
                continue; // Avoid remove in case we could retry removal again
            }
            it.remove();
        }
    }

    public static Job finish(String taskId) {
        Job job = running.remove(taskId);
        finished.add(job);
        return job;
    }

    public static Job kill(String taskId) {
        Job job = running.remove(taskId);
        finished.add(job);
        return job;
    }

    public static void kill(Job job) {
        finished.add(job);
    }

    public static Map<String, Job> getRunning() {
        return running;
    }

    public static void getAllFinished(List<Job> list, int limit) {
        list.addAll(finished);
    }

    public static void compact() {
        int size = finished.size() / 2;
        while (finished.size() > size) {
            finished.remove();
        }
    }

    // Methods for test
    public static void setStatus(StatusResponse response) {
        response.setStatus(JobQueue.size(), running.size());
    }
}
