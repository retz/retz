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
package io.github.retz.localexecutor;

import io.github.retz.mesos.Resource;
import io.github.retz.mesos.ResourceConstructor;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class RetzExecutor implements Executor {
    private static final Logger LOG = LoggerFactory.getLogger(RetzExecutor.class);

    private Protos.ExecutorInfo executorInfo;
    private Protos.FrameworkInfo frameworkInfo;
    private Protos.SlaveInfo slaveInfo;

    public RetzExecutor() {
    }

    @Override
    public void reregistered(ExecutorDriver driver, Protos.SlaveInfo slaveInfo) {
    }

    @Override
    public void registered(ExecutorDriver driver, Protos.ExecutorInfo executorInfo, Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
        this.executorInfo = executorInfo;
        LOG.info("Executor registered: ID={}, name={}", executorInfo.getExecutorId(), executorInfo.getName());
        for (Protos.Resource resource : executorInfo.getResourcesList()) {
            LOG.debug("Resource {}: {}", resource.getName(), resource.toString());
        }

        this.frameworkInfo = frameworkInfo;
        this.slaveInfo = slaveInfo;
        LOG.info("Framework={}, SlaveId=", frameworkInfo.getName(), slaveInfo.getId().getValue());
    }

    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {
        if (new String(data, StandardCharsets.UTF_8).equals("stop")) {
            LOG.info("Stop message from framework");
            driver.stop();
            return;
        }
        LOG.info("Message from framework: {}", String.valueOf(data));
    }

    @Override
    public void disconnected(ExecutorDriver driver) {
        LOG.warn("Disconnected from Mesos");
    }

    @Override
    public void error(ExecutorDriver driver, String message) {
        LOG.error(message);
    }

    @Override
    public void killTask(ExecutorDriver driver, Protos.TaskID taskId) {
        LOG.info("RetzExecutor#killTask called on {}", taskId.getValue());
        LocalProcessManager.killTask(taskId);
    }

    @Override
    public void launchTask(ExecutorDriver driver, Protos.TaskInfo task) {

        LOG.info("Starting task {}", task.getTaskId().getValue());

        // PUT a cap on CPU usage by telling assigned resources to M3BP
        // Or it's gonna use *all* Sockets in the machine
        Resource resource = ResourceConstructor.decode(task.getResourcesList());
        LOG.info("Resource: {}", resource.toString());

        LocalProcessManager.startTask(task, (int) resource.cpu(), resource.memMB());

        // Reuse, don't stop or Agent automatically kills this process
        //driver.stop();
    }


    @Override
    public void shutdown(ExecutorDriver driver) {
        LOG.info("Executor shutting down");
        LocalProcessManager.stop();
        LocalProcessManager.join();
    }

    // Only for tests
    Protos.FrameworkInfo getFrameworkInfo() {
        return frameworkInfo;
    }

    Protos.ExecutorInfo getExecutorInfo() {
        return executorInfo;
    }

    Protos.SlaveInfo getSlaveInfo() {
        return slaveInfo;
    }
}
