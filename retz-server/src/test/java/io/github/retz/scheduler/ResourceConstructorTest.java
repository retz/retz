/**
 *    Retz
 *    Copyright (C) 2016-2017 Nautilus Technologies, Inc.
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

import io.github.retz.planner.spi.Resource;
import org.apache.mesos.Protos;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ResourceConstructorTest {
    @Test
    public void decode() {
        Protos.TaskInfo task = Protos.TaskInfo.newBuilder()
                .setTaskId(Protos.TaskID.newBuilder().setValue("foobar-taskid").build())
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("foobar-slaveid").build())
                .setName("foobar-taskname")
                .addAllResources(ResourceConstructor.construct(3, 256))
                .build();
        Resource r = ResourceConstructor.decode(task.getResourcesList());
        assertThat((int)r.cpu(), is(3));
        assertThat(r.memMB(), is(256));
    }

}
