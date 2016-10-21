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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.retz.mesos.Resource;
import io.github.retz.mesos.ResourceConstructor;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.DockerContainer;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.MesosContainer;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static io.github.retz.scheduler.Applications.appToCommandInfo;
import static io.github.retz.scheduler.Applications.appToContainerInfo;

public class TaskBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(TaskBuilder.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Resource assigned;
    Protos.TaskInfo.Builder builder;

    static {
        MAPPER.registerModule(new Jdk8Module());
    }

    TaskBuilder() {
        builder = Protos.TaskInfo.newBuilder();
    }

    public TaskBuilder setResource(Resource r, Protos.SlaveID slaveID) {
        builder.addAllResources(ResourceConstructor.construct((int)r.cpu(), r.memMB(), r.diskMB(), r.gpu()));
        builder.setSlaveId(slaveID);
        return this;
    }

    // @doc assign as much CPU/Memory as possible
    public TaskBuilder setOffer(Resource offered, int cpu, int memMB, int gpu, Protos.SlaveID slaveID) {
        assert cpu <= offered.cpu();
        assert memMB <= offered.memMB();
        assert gpu <= offered.gpu();
        int assignedCpu = cpu;
        int assignedMem = memMB;
        int assignedGPU = gpu;

        assigned = new Resource(assignedCpu, assignedMem, 0, 0, assignedGPU);
        builder.addAllResources(ResourceConstructor.construct(assignedCpu, assignedMem, 0, assignedGPU))
                //builder.addAllResources(offer.getResourcesList())
                .setSlaveId(slaveID);
        offered.subCPU(assignedCpu);
        offered.subMemMB(assignedMem);
        offered.subGPU(assignedGPU);
        LOG.debug("Assigning cpu={}, mem={}, gpus={}", assignedCpu, assignedMem, assignedGPU);
        return this;
    }

    public TaskBuilder setName(String name) {
        builder.setName(name);
        return this;
    }

    public TaskBuilder setTaskId(String id) {
        //TODO set some unique batch execution id here
        builder.setTaskId(Protos.TaskID.newBuilder().setValue(id).build());
        return this;
    }

    public TaskBuilder setCommand(Job job, Application application) {
        Protos.CommandInfo commandInfo = appToCommandInfo(application, job);
        builder.setCommand(commandInfo);

        if (application.container() instanceof DockerContainer) {
            Protos.ContainerInfo containerInfo = appToContainerInfo(application);
            builder.setContainer(containerInfo);

        } else if (!(application.container() instanceof MesosContainer)) {
            LOG.error("Unknown container: {}", application.container());
            throw new AssertionError();
        }

        return this;
    }

    public Protos.TaskInfo build() {
        return builder.build();
    }

    public Resource getAssigned() {
        return assigned;
    }
}
