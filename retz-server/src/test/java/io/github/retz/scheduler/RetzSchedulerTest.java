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
import io.github.retz.cli.FileConfiguration;
import io.github.retz.db.Database;
import io.github.retz.mesos.ResourceConstructor;
import io.github.retz.protocol.data.*;
import org.apache.mesos.Protos;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RetzSchedulerTest {

    MesosSchedulerDummyDriver driver;
    RetzScheduler scheduler;
    ObjectMapper mapper;

    @Before
    public void before() throws Exception {
        mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());

        Protos.FrameworkInfo frameworkInfo = Protos.FrameworkInfo.newBuilder()
                .setUser("")
                .setName(RetzScheduler.FRAMEWORK_NAME)
                .build();

        InputStream in = Launcher.class.getResourceAsStream("/retz.properties");
        Launcher.Configuration conf = new Launcher.Configuration(new ServerConfiguration(in));

        Database.getInstance().init(conf.getServerConfig());
        scheduler = new RetzScheduler(conf, frameworkInfo);
        driver = new MesosSchedulerDummyDriver(scheduler, frameworkInfo, conf.getMesosMaster());
    }

    @After
    public void after() {
        driver.clear();
        Database.getInstance().clear();
        Database.getInstance().stop();
    }

    @Test
    public void justRun() {
        driver.start();
        driver.stop();
        driver.run();
    }

    // @Test
    public void decline() {
        driver.start();
        String offerId = "super duper blooper";

        Protos.Offer offers[] = {buildOffer(offerId, 4, 512)};

        driver.clear();
        driver.dummyOffer(Arrays.asList(offers));
        assertThat(driver.getDeclined().size(), is(1));
        assertThat(driver.getDeclined().get(0).getValue(), is(offerId));

        driver.stop();
    }

    //TODO: @Test
    public void launch() throws InterruptedException, JsonProcessingException, IOException {
        String files[] = {"http://foobar.boom.co.jp/foo.tar.gz"};
        Applications.load(new Application("fooapp", new LinkedList<String>(), new LinkedList<String>(),
                Arrays.asList(files), Optional.empty(), Optional.empty(), "deadbeef", new MesosContainer(), true));
        Job job = new Job("fooapp", "foocmd", null, 2, 256);
        JobQueue.push(job);

        driver.start();
        String offerId = "super duper blooper";

        Protos.Offer offers[] = {buildOffer(offerId, 4, 512)};

        driver.clear();
        driver.dummyOffer(Arrays.asList(offers));

        assertTrue(driver.getDeclined().isEmpty());
        assertThat(driver.getAccepted().size(), is(1));
        assertThat(driver.getTasks().size(), is(1));

        Protos.TaskInfo task = driver.getTasks().get(0);

        assertThat(mapper.readValue(task.getData().toStringUtf8(), Job.class).cmd(), is(job.cmd()));
        assertThat(task.getSlaveId().getValue(), is(offers[0].getSlaveId().getValue()));
        assertFalse(task.getTaskId().getValue().isEmpty());
        // TODO: Not knowing why this is empty
        for (Protos.Resource r : task.getExecutor().getResourcesList()) {
            System.err.println(r.getName());
            System.err.println(r.getScalar().getValue());
        }

        assertThat(JobQueue.countRunning(), is(1));
        driver.dummyTaskStarted();
        assertThat(JobQueue.countRunning(), is(1));

        // TODO: introduce DI to skip URL fetch
        //driver.dummyTaskFinish();
        //assertTrue(scheduler.getRunning().isEmpty());

        Applications.unload("fooapp");
        driver.stop();
    }

    @Test
    public void notEnough() throws Exception {
        String files[] = {"http://foobar.boom.co.jp/foo.tar.gz"};
        Database.getInstance().addUser(new User("Deadbeef", "cafebabe", true));
        Applications.load(new Application("fooapp", new LinkedList<String>(), new LinkedList<String>(),
                Arrays.asList(files), Optional.empty(), Optional.empty(), "Deadbeef", new MesosContainer(), true));
        Job job = new Job("fooapp", "foocmd", null, 2, 256);
        JobQueue.push(job);

        driver.start();
        String offerId = "super duper blooper";

        Protos.Offer offers[] = {buildOffer(offerId, 2, 34)};

        driver.clear();
        driver.dummyOffer(Arrays.asList(offers));

        assertThat(driver.getDeclined().size(), is(1));
        assertTrue(driver.getAccepted().isEmpty());
        assertTrue(driver.getTasks().isEmpty());

        Applications.unload("fooapp");
        driver.stop();
    }

    // TODO: @Test
    public void slaveFail() throws InterruptedException, JsonProcessingException, IOException {
        String files[] = {"http://foobar.boom.co.jp/foo.tar.gz"};
        Applications.load(new Application("fooapp", new LinkedList<String>(), new LinkedList<String>(),
                Arrays.asList(files), Optional.empty(), Optional.empty(), "Deadbeef", new MesosContainer(), true));
        Job job = new Job("fooapp", "foocmd", null, 2, 256);
        JobQueue.push(job);

        driver.start();
        String offerId = "super duper blooper";

        Protos.Offer offers[] = {buildOffer(offerId, 4, 512)};
        Protos.SlaveID slaveID = offers[0].getSlaveId();


        driver.clear();
        driver.dummyOffer(Arrays.asList(offers));

        assertTrue(driver.getDeclined().isEmpty());
        assertThat(driver.getAccepted().size(), is(1));
        assertThat(driver.getTasks().size(), is(1));

        Protos.TaskInfo task = driver.getTasks().get(0);
        assertThat(mapper.readValue(task.getData().toStringUtf8(), Job.class).cmd(), is(job.cmd()));
        assertThat(task.getSlaveId().getValue(), is(slaveID.getValue()));
        assertFalse(task.getTaskId().getValue().isEmpty());
        // TODO: Not knowing why this is empty
        for (Protos.Resource r : task.getExecutor().getResourcesList()) {
            System.err.println(r.getName());
            System.err.println(r.getScalar().getValue());
        }

        assertThat(JobQueue.countRunning(), is(1));
        driver.dummyTaskStarted();
        assertThat(JobQueue.countRunning(), is(1));

        // Simulate slave failure to test task re-assign
        scheduler.slaveLost(driver, slaveID);
        assertThat(JobQueue.countRunning(), is(0));

        // TODO: introduce
        //driver.dummyTaskFinish();
        //assertTrue(scheduler.getRunning().isEmpty());

        Applications.unload("fooapp");
        driver.stop();
    }

    private Protos.Offer buildOffer(String offerId, int cpus, int mem) {
        return RetzSchedulerTest.buildOffer(driver.frameworkInfo.getId(), offerId, cpus, mem);
    }
    static Protos.Offer buildOffer(Protos.FrameworkID fid, String offerId, int cpus, int mem) {

        String slaveId = "slave(1)@127.0.0.1:5051";

        Protos.Offer.Builder builder = Protos.Offer.newBuilder()
                .addAllResources(ResourceConstructor.construct(cpus, mem))
                .setSlaveId(Protos.SlaveID.newBuilder()
                        .setValue(slaveId)
                        .build())
                .setFrameworkId(fid)
                .setHostname("127.0.0.1:5051")
                .setId(Protos.OfferID.newBuilder().setValue(offerId).build());
        return builder.build();
    }

}
