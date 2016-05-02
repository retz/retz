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
package com.asakusafw.retz.mesos;

import com.asakusafw.retz.protocol.Job;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @doc On memory job queue to mediate job execution requests and resources
 */
public class JobQueue {
    // CMT: I thought capital case was for static const, like in C++
    // REVIEW: static final fields should have CAPITAL_CASE name
    private static final BlockingQueue<Job> JOB_QUEUE = new LinkedBlockingDeque<>();
    private static final AtomicInteger COUNTER = new AtomicInteger(1);

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

    public static String now() {
        return Calendar.getInstance().getTime().toString();
    }

    public synchronized static void clear() {
        JOB_QUEUE.clear();
    }

    public static int size() {
        return get().size();
    }
}
