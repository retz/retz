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
package com.asakusafw.retz.executor;


import com.asakusafw.retz.protocol.MetaJob;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*
 * @doc Unlike CPUManager, this is a singleton thread that works concurrently, waits for local processes finish,
 * not to block RetzExecutor callbacks.
 */
public class LocalProcessManager implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(LocalProcessManager.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final LocalProcessManager MANAGER = new LocalProcessManager();
    private static Thread managerThread;

    {
        MAPPER.registerModule(new Jdk8Module());
    }
    private LocalProcessManager() {
    }

    public static synchronized void start(ExecutorDriver driver) {
        if (managerThread != null) {
            return;
        }
        MANAGER.setDriver(driver);

        managerThread = new Thread(MANAGER);
        managerThread.setName("ProcessManager");
        managerThread.start();
    }

    public static synchronized void join() throws InterruptedException {
        if (managerThread != null)
            managerThread.join();
    }

    public static synchronized void startTask(Protos.TaskInfo task,
                                              int cpus, int memMB) {
        List<Integer> assigned = CPUManager.get().assign(task.getTaskId().getValue(), cpus);
        LOG.info("Given {}cpus/{}MB by Mesos, CPUManager got {}", cpus, memMB, assigned.size());

        if (assigned.isEmpty()) {
            LOG.error("No CPU was assigned by CPUManager at task {}", task.getTaskId().getValue());
            // XXX: must not reach here...
            MANAGER.killed(task);
            return;
        }
        try {
            LOG.debug(task.getData().toStringUtf8());

            MetaJob metaJob = MAPPER.readValue(task.getData().toByteArray(), MetaJob.class);
            LocalProcess p = new LocalProcess(task, metaJob, assigned);
            MANAGER.starting(task.getTaskId());

            if (p.start()) {
                MANAGER.started(task.getTaskId().getValue(), p);
            } else {
                MANAGER.killed(task);
            }
        } catch (IOException e) {
            LOG.debug(e.getMessage());
            MANAGER.killed(task);
        }

    }

    public static boolean isTaskFinished(Protos.TaskID taskID) {
        return MANAGER.processes.containsKey(taskID.getValue());
    }

    // TaskID => LocalProcess
    private Map<String, LocalProcess> processes = new ConcurrentHashMap<>();
    private ExecutorDriver driver;

    public void starting(Protos.TaskID taskId) {
        Protos.TaskStatus.Builder builder = Protos.TaskStatus.newBuilder()
                .setTaskId(taskId)
                .setMessage("starting")
                .setState(Protos.TaskState.TASK_STARTING);
        driver.sendStatusUpdate(builder.build());
    }
    public void started(String taskID, LocalProcess p) {
        processes.put(taskID, p);

        Protos.TaskStatus.Builder builder = Protos.TaskStatus.newBuilder()
                .setTaskId(p.getTaskInfo().getTaskId())
                .setMessage("started")
                .setState(Protos.TaskState.TASK_RUNNING);
        driver.sendStatusUpdate(builder.build());
    }

    public void killed(Protos.TaskInfo task) {
        Protos.TaskStatus.Builder builder = Protos.TaskStatus.newBuilder()
                .setTaskId(task.getTaskId())
                .setMessage("-42")
                .setState(Protos.TaskState.TASK_KILLED);

        driver.sendStatusUpdate(builder.build());
    }

    public void setDriver(ExecutorDriver driver) {
        this.driver = driver;
    }

    public void poll() throws InterruptedException {
        List<LocalProcess> finished = new LinkedList<>();
        for (Map.Entry<String, LocalProcess> pair : processes.entrySet()) {
            if (pair.getValue().poll()) {
                finished.add(processes.remove(pair.getKey()));
            }
        }
        for (LocalProcess localProcess : finished) {
            Protos.TaskInfo task = localProcess.getTaskInfo();
            CPUManager.get().free(task.getTaskId().getValue());
            int status = localProcess.handle();

            Protos.TaskStatus.Builder builder = Protos.TaskStatus.newBuilder()
                    .setTaskId(task.getTaskId());

            if (status == 0) {
                LOG.info("Task {} success", task.getTaskId().getValue());
                builder.setState(Protos.TaskState.TASK_FINISHED);
            } else {
                LOG.error("Task {} failed", task.getTaskId().getValue());
                builder.setState(Protos.TaskState.TASK_FAILED);
            }
            builder.setMessage(Integer.toString(status));
            driver.sendStatusUpdate(builder.build());
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                MANAGER.poll();
                Thread.sleep(512);
            } catch (InterruptedException e) {
                LOG.warn("Polling error: {}", e.getMessage());
            }
        }
    }
}
