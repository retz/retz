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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.protobuf.ByteString;
import io.github.retz.mesos.Resource;
import io.github.retz.mesos.ResourceConstructor;
import io.github.retz.protocol.data.*;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.github.retz.scheduler.Applications.appToCommandInfo;
import static io.github.retz.scheduler.Applications.appToContainerInfo;

public class TaskBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(TaskBuilder.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Resource assigned;

    Protos.TaskInfo.Builder builder;
    {
        MAPPER.registerModule(new Jdk8Module());
    }

    TaskBuilder() {
        builder = Protos.TaskInfo.newBuilder();
    }

    // @doc assign as much CPU/Memory as possible
    public TaskBuilder setOffer(Resource offered, Range cpu, Range memMB, int gpu, Protos.SlaveID slaveID) {
        assert cpu.getMin() <= offered.cpu();
        assert memMB.getMin() <= offered.memMB();
        assert gpu <= offered.gpu();
        int assignedCpu = Integer.min((int) offered.cpu(), cpu.getMax());
        int assignedMem = Integer.min(offered.memMB(), memMB.getMax());
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

    public TaskBuilder setVolume(Resource r, String volumeId) {
        assert r.volumes().containsKey(volumeId);
        LOG.debug(r.volumes().get(volumeId).toString());
        builder.addResources(r.volumes().get(volumeId));
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

    public TaskBuilder setExecutor(Job job, Application application, Protos.FrameworkID frameworkId)
            throws JsonProcessingException
    {
        if (application.container() instanceof MesosContainer) {
            Protos.ExecutorInfo executorInfo = Applications.appToExecutorInfo(application, frameworkId);
            builder.setExecutor(executorInfo);
            this.setJob(job, Applications.encodable(application));

        } else if (application.container() instanceof DockerContainer) {
            Protos.CommandInfo commandInfo = appToCommandInfo(application, job.cmd());
            builder.setCommand(commandInfo);
            Protos.ContainerInfo containerInfo = appToContainerInfo(application);
            builder.setContainer(containerInfo);

        } else {
            LOG.error("Unknown container: {}", application.container());
            throw new AssertionError();
        }

        return this;
    }

    // Only available at RetzExecutor
    private TaskBuilder setJob(Job job, Application app) throws JsonProcessingException {
        MetaJob metaJob = new MetaJob(job, app);
        String json = TaskBuilder.MAPPER.writeValueAsString(metaJob);
        builder.setData(ByteString.copyFromUtf8(json));
        return this;
    }

    public Protos.TaskInfo build() {
        return builder.build();
    }

    public Resource getAssigned() {
        return assigned;
    }
}
