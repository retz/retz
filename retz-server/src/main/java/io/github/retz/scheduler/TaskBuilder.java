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
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.DockerContainer;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.MesosContainer;
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

    static {
        MAPPER.registerModule(new Jdk8Module());
    }

    TaskBuilder() {
        builder = Protos.TaskInfo.newBuilder();
    }

    public TaskBuilder setResource(Resource r, Protos.SlaveID slaveID) {
        //builder.addAllResources(ResourceConstructor.construct((int)r.cpu(), r.memMB(), r.diskMB(), r.gpu()));
        assigned = r;
        builder.addAllResources(r.construct());
        builder.setSlaveId(slaveID);
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
        Protos.CommandInfo commandInfo = appToCommandInfo(application, job, assigned.ports());
        builder.setCommand(commandInfo);

        if (application.container() instanceof DockerContainer) {
            Protos.ContainerInfo containerInfo = appToContainerInfo(application);
            builder.setContainer(containerInfo);

        } else if (!(application.container() instanceof MesosContainer)) {
            LOG.error("Unknown container: {}", application.container());
            throw new AssertionError();
        }
        if (application.getGracePeriod() > 0) {
            // seconds to nanoseconds
            long d = 1000000000L * application.getGracePeriod();
            builder.setKillPolicy(Protos.KillPolicy.newBuilder()
                    .setGracePeriod(Protos.DurationInfo.newBuilder().setNanoseconds(d)));
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
