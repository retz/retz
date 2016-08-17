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
package io.github.retz.executor;

import io.github.retz.executor.DummyExecutorDriver;
import io.github.retz.executor.LocalProcessManager;
import io.github.retz.executor.RetzExecutor;
import io.github.retz.protocol.Application;
import io.github.retz.protocol.Job;
import io.github.retz.protocol.MetaJob;
import io.github.retz.protocol.Range;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.protobuf.ByteString;
import org.apache.commons.io.FilenameUtils;
import org.apache.mesos.Protos;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


import io.github.retz.mesos.Resource;
import io.github.retz.mesos.ResourceConstructor;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Optional;

/**
 * Created by kuenishi on 6/9/16.
 */
public class RetzExecutorTest {

    DummyExecutorDriver driver;
    RetzExecutor executor;
    TemporaryFolder folder = new TemporaryFolder();
    ObjectMapper mapper;

    @Before
    public void before() throws IOException {
        folder.create();
        executor = new RetzExecutor();
        driver = new DummyExecutorDriver(executor, folder);
        mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        LocalProcessManager.start(driver);
    }

    @After
    public void after() {
        folder.delete();
    }

    @Test
    public void run() {
        assert driver.start() == Protos.Status.DRIVER_RUNNING;
        // check registered called

        assert executor.getExecutorInfo() != null;
        assert executor.getFrameworkInfo() != null;
        assert executor.getSlaveInfo() != null;

        assert driver.stop() == Protos.Status.DRIVER_STOPPED;
        // check shutdown called
    }

    // TODO: make this kind of tests as quickchecky state machine test
    @Test
    public void fuzz() {
        assert driver.start() == Protos.Status.DRIVER_RUNNING;

        // check not execption thrown
        executor.disconnected(driver);
        executor.reregistered(driver, executor.getSlaveInfo());
        executor.shutdown(driver);
        executor.error(driver, "noooooooooooooooooooooooooooooop");
    }

    @Test
    public void launchTaskTest() throws IOException, InterruptedException {
        assert driver.start() == Protos.Status.DRIVER_RUNNING;
        // check registered called

        System.err.println(folder.toString());

        String tempFilename = FilenameUtils.concat("/tmp", folder.newFile().getName());
        System.err.println("Temporary file: " + tempFilename);
        assert !new File(tempFilename).exists();

        Job job = new Job("appname", "touch " + tempFilename, null, new Range(1, 0), new Range(128, 0));
        Application app = new Application("appname", new LinkedList<>(), new LinkedList<>(), Optional.empty());
        MetaJob metaJob = new MetaJob(job, app);
        Protos.TaskInfo task = Protos.TaskInfo.newBuilder()
                .setTaskId(Protos.TaskID.newBuilder().setValue("foobar-task").build())
                .setData(ByteString.copyFromUtf8(mapper.writeValueAsString(metaJob)))
                .setExecutor(executor.getExecutorInfo())
                .setSlaveId(executor.getSlaveInfo().getId())
                .addAllResources(ResourceConstructor.construct(3, 512))
                .setName("yeah, holilday!")
                .build();
        executor.launchTask(driver, task);

        while(LocalProcessManager.isTaskFinished(task.getTaskId())) {
            Thread.sleep(512);
        }

        assert new File(tempFilename).exists();

        assert driver.stop() == Protos.Status.DRIVER_STOPPED;
        // check shutdown called
    }

    @Test
    public void notEnough() throws IOException {
        assert driver.start() == Protos.Status.DRIVER_RUNNING;
        // check registered called

        String tempFilename = FilenameUtils.concat("/tmp", folder.newFile().getName());
        System.err.println("Temporary file: " + tempFilename);
        assert !new File(tempFilename).exists();

        Job job = new Job("appname", "touch " + tempFilename, null, new Range(3, 0), new Range(128, 0));
        Protos.TaskInfo task = Protos.TaskInfo.newBuilder()
                .setTaskId(Protos.TaskID.newBuilder().setValue("foobar-task").build())
                .setData(ByteString.copyFromUtf8(mapper.writeValueAsString(job)))
                .setExecutor(executor.getExecutorInfo())
                .setSlaveId(executor.getSlaveInfo().getId())
                .addAllResources(ResourceConstructor.construct(1, 12))
                .setName("yeah, holilday!")
                .build();
        executor.launchTask(driver, task);

        assert !new File(tempFilename).exists();

        assert driver.stop() == Protos.Status.DRIVER_STOPPED;
        // check shutdown called
    }

}
