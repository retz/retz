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

import io.github.retz.protocol.Application;
import io.github.retz.protocol.Job;
import io.github.retz.protocol.MetaJob;
import io.github.retz.protocol.Range;
import io.github.retz.mesos.Resource;
import io.github.retz.mesos.ResourceConstructor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.protobuf.ByteString;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(TaskBuilder.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    Protos.TaskInfo.Builder builder;
    {
        MAPPER.registerModule(new Jdk8Module());
    }

    TaskBuilder() {
        builder = Protos.TaskInfo.newBuilder();
    }

    // @doc assign as much CPU/Memory as possible
    public TaskBuilder setOffer(Resource r, Range cpu, Range memMB, Range gpu, Protos.SlaveID slaveID) {
        assert cpu.getMin() <= r.cpu();
        assert memMB.getMin() <= r.memMB();
        assert gpu.getMin() <= r.gpu();
        int assignedCpu = Integer.min((int) r.cpu(), cpu.getMax());
        int assignedMem = Integer.min(r.memMB(), memMB.getMax());
        int assignedGPU = Integer.min(r.gpu(), gpu.getMax());

        builder.addAllResources(ResourceConstructor.construct(assignedCpu, assignedMem, 0, assignedGPU))
                //builder.addAllResources(offer.getResourcesList())
                .setSlaveId(slaveID);
        r.subCPU(assignedCpu);
        r.subMemMB(assignedMem);
        r.subGPU(assignedGPU);
        LOG.info("Assigning cpu={}, mem={}, gpus={}", assignedCpu, assignedMem, assignedGPU);
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

    public TaskBuilder setExecutor(Protos.ExecutorInfo e) {
        builder.setExecutor(e);
        return this;
    }

    public TaskBuilder setJob(Job job, Application app) throws JsonProcessingException {
        MetaJob metaJob = new MetaJob(job, app);
        String json = TaskBuilder.MAPPER.writeValueAsString(metaJob);
        builder.setData(ByteString.copyFromUtf8(json));
        return this;
    }

    public Protos.TaskInfo build() {
        return builder.build();
    }


}
