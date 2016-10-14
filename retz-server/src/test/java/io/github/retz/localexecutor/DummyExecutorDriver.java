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

import io.github.retz.mesos.ResourceConstructor;
import org.apache.commons.io.FilenameUtils;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.junit.rules.TemporaryFolder;

import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class DummyExecutorDriver implements ExecutorDriver {
    Executor executor;

    Protos.ExecutorInfo executorInfo;
    Protos.FrameworkInfo frameworkInfo;
    Protos.SlaveInfo slaveInfo;

    TemporaryFolder folder;

    Protos.TaskStatus statusUpdate;

    public DummyExecutorDriver(Executor executor, TemporaryFolder folder) {
        this.executor = Objects.requireNonNull(executor);
        this.folder = Objects.requireNonNull(folder);

        frameworkInfo = Protos.FrameworkInfo.newBuilder()
                .setId(Protos.FrameworkID.newBuilder().setValue("dummy-framework-id").build())
                .setUser("me")
                .setPrincipal("*")
                .setName("dummy-framework-name")
                .build();
        executorInfo = buildExecutorInfo(folder, frameworkInfo);
        slaveInfo = Protos.SlaveInfo.newBuilder()
                .setId(Protos.SlaveID.newBuilder().setValue("dummy-slave-id").build())
                .addAllResources(ResourceConstructor.construct(4, 4096))
                .setHostname("localhost-dummy:5053")
                .build();
        statusUpdate = null;
    }

    public synchronized Protos.Status stop() {
        executor.shutdown(this);
        //statusUpdate = null;
        return Protos.Status.DRIVER_STOPPED;
    }

    public Protos.Status start() {
        executor.registered(this, executorInfo, frameworkInfo, slaveInfo);
        return Protos.Status.DRIVER_RUNNING;
    }

    public Protos.Status run() {
        start();
        return stop();
    }

    public Protos.Status abort() {
        return Protos.Status.DRIVER_ABORTED;
    }

    public Protos.Status join() {
        return Protos.Status.DRIVER_STOPPED;
    }

    @Override
    public synchronized Protos.Status sendStatusUpdate(Protos.TaskStatus status) {
        //System.err.println("Driver#sendStatusUpdate with " + status);
        statusUpdate = status;
        return Protos.Status.DRIVER_RUNNING;
    }

    public Optional<Protos.TaskStatus> getUpdatedStatus() {
        return Optional.ofNullable(statusUpdate);
    }

    @Override
    public Protos.Status sendFrameworkMessage(byte[] data) {
        return Protos.Status.DRIVER_RUNNING;
    }

    private Protos.ExecutorInfo buildExecutorInfo(TemporaryFolder folder, Protos.FrameworkInfo frameworkInfo) {
        // Copied from Applications.Application

        URL jarUrl = DummyExecutorDriver.class.getProtectionDomain().getCodeSource().getLocation();
        String jarFile = FilenameUtils.getName(jarUrl.toString());

        String[] appFilesArray = {
                "file://foo/bar/baz.tar.gz",
                "http://example.com:4242/day/of/gluttony.tgz"
        };
        List<String> appFiles = Arrays.asList(appFilesArray);

        String appName = "dummy-executor-driver-dummy-app";

        // Actually this is not used as this is just a test
        String cmd = "java -cp " + jarFile + " " + MesosExecutorLauncher.getFullClassName();
        Protos.CommandInfo.Builder commandInfoBuilder = Protos.CommandInfo.newBuilder()
                .setEnvironment(Protos.Environment.newBuilder()
                        .addVariables(Protos.Environment.Variable.newBuilder()
                                .setName("ASAKUSA_HOME").setValue(".").build()))
                .setValue(cmd)
                .setShell(true)
                .addUris(Protos.CommandInfo.URI.newBuilder().setValue(jarUrl.toString()).setCache(false)); // In production, set this true

        for (String file : appFiles) {
            commandInfoBuilder.addUris(Protos.CommandInfo.URI.newBuilder().setValue(file).setCache(true));
        }

        Protos.ExecutorInfo executorInfo = Protos.ExecutorInfo.newBuilder()
                .setCommand(commandInfoBuilder.build())
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue(appName).build())
                .setFrameworkId(frameworkInfo.getId())
                .build();
        return executorInfo;
    }
}
