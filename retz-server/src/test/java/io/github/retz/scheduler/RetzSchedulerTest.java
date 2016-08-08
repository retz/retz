/**
 * Retz
 * Copyright (C) 2016 Nautilus Technologies, KK.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.retz.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.retz.cli.FileConfiguration;
import io.github.retz.mesos.ResourceConstructor;
import io.github.retz.protocol.Application;
import io.github.retz.protocol.Job;
import io.github.retz.protocol.Range;
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

public class RetzSchedulerTest {

    MesosSchedulerDummyDriver driver;
    RetzScheduler scheduler;
    ObjectMapper mapper;

    @Before
    public void before() throws IOException, URISyntaxException {
        mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());

        JobQueue.clear();
        Protos.FrameworkInfo frameworkInfo = Protos.FrameworkInfo.newBuilder()
                .setUser("")
                .setName(RetzScheduler.FRAMEWORK_NAME)
                .build();

        InputStream in = MesosFrameworkLauncher.class.getResourceAsStream("/retz.properties");
        MesosFrameworkLauncher.Configuration conf = new MesosFrameworkLauncher.Configuration(new FileConfiguration(in));

        scheduler = new RetzScheduler(conf, frameworkInfo);
        driver = new MesosSchedulerDummyDriver(scheduler, frameworkInfo, conf.getMesosMaster());
    }

    @After
    public void after() {
        driver.clear();
    }

    @Test
    public void justRun() {
        driver.start();
        driver.stop();
        driver.run();
    }

    @Test
    public void decline() {
        driver.start();
        String offerId = "super duper blooper";

        Protos.Offer offers[] = {buildOffer(offerId, 4, 512)};

        driver.clear();
        driver.dummyOffer(Arrays.asList(offers));
        assert driver.getDeclined().size() == 1;
        assert driver.getDeclined().get(0).getValue().equals(offerId);

        driver.stop();
    }

    //TODO: @Test
    public void launch() throws InterruptedException, JsonProcessingException, IOException {
        String files[] = {"http://foobar.boom.co.jp/foo.tar.gz"};
        Applications.load(new Application("fooapp", new LinkedList<String>(), Arrays.asList(files), Optional.empty()));
        Job job = new Job("fooapp", "foocmd", null, new Range(2, 0), new Range(256, 0));
        JobQueue.push(job);

        driver.start();
        String offerId = "super duper blooper";

        Protos.Offer offers[] = {buildOffer(offerId, 4, 512)};

        driver.clear();
        driver.dummyOffer(Arrays.asList(offers));

        assert driver.getDeclined().isEmpty();
        assert driver.getAccepted().size() == 1;
        assert driver.getTasks().size() == 1;

        Protos.TaskInfo task = driver.getTasks().get(0);

        assert job.cmd().equals(mapper.readValue(task.getData().toStringUtf8(), Job.class).cmd());
        assert task.getSlaveId().getValue().equals(offers[0].getSlaveId().getValue());
        assert !task.getTaskId().getValue().isEmpty();
        // TODO: Not knowing why this is empty
        for (Protos.Resource r : task.getExecutor().getResourcesList()) {
            System.err.println(r.getName());
            System.err.println(r.getScalar().getValue());
        }

        assert scheduler.getRunning().size() == 1;
        driver.dummyTaskStarted();
        assert scheduler.getRunning().size() == 1;

        // TODO: introduce DI to skip URL fetch
        //driver.dummyTaskFinish();
        //assert scheduler.getRunning().isEmpty();

        Applications.unload("fooapp");
        driver.stop();
    }

    @Test
    public void notEnough() throws InterruptedException {
        String files[] = {"http://foobar.boom.co.jp/foo.tar.gz"};
        Applications.load(new Application("fooapp", new LinkedList<String>(), Arrays.asList(files), Optional.empty()));
        Job job = new Job("fooapp", "foocmd", null, new Range(2, 0), new Range(256, 0));
        JobQueue.push(job);

        driver.start();
        String offerId = "super duper blooper";

        Protos.Offer offers[] = {buildOffer(offerId, 2, 34)};

        driver.clear();
        driver.dummyOffer(Arrays.asList(offers));

        assert driver.getDeclined().size() == 1;
        assert driver.getAccepted().isEmpty();
        assert driver.getTasks().isEmpty();

        Applications.unload("fooapp");
        driver.stop();
    }

    // TODO: @Test
    public void slaveFail() throws InterruptedException, JsonProcessingException, IOException {
        String files[] = {"http://foobar.boom.co.jp/foo.tar.gz"};
        Applications.load(new Application("fooapp", new LinkedList<String>(), Arrays.asList(files), Optional.empty()));
        Job job = new Job("fooapp", "foocmd", null, new Range(2, 0), new Range(256, 0));
        JobQueue.push(job);

        driver.start();
        String offerId = "super duper blooper";

        Protos.Offer offers[] = {buildOffer(offerId, 4, 512)};
        Protos.SlaveID slaveID = offers[0].getSlaveId();


        driver.clear();
        driver.dummyOffer(Arrays.asList(offers));

        assert driver.getDeclined().isEmpty();
        assert driver.getAccepted().size() == 1;
        assert driver.getTasks().size() == 1;

        Protos.TaskInfo task = driver.getTasks().get(0);
        assert job.cmd().equals(mapper.readValue(task.getData().toStringUtf8(), Job.class).cmd());
        assert task.getSlaveId().getValue().equals(slaveID.getValue());
        assert !task.getTaskId().getValue().isEmpty();
        // TODO: Not knowing why this is empty
        for (Protos.Resource r : task.getExecutor().getResourcesList()) {
            System.err.println(r.getName());
            System.err.println(r.getScalar().getValue());
        }

        assert scheduler.getRunning().size() == 1;
        driver.dummyTaskStarted();
        assert scheduler.getRunning().size() == 1;

        // Simulate slave failure to test task re-assign
        scheduler.slaveLost(driver, slaveID);
        assert scheduler.getRunning().size() == 0;

        // TODO: introduce
        //driver.dummyTaskFinish();
        //assert scheduler.getRunning().isEmpty();

        Applications.unload("fooapp");
        driver.stop();
    }

    private Protos.Offer buildOffer(String offerId, int cpus, int mem) {

        String slaveId = "slave(1)@127.0.0.1:5051";

        Protos.Offer.Builder builder = Protos.Offer.newBuilder()
                .addAllResources(ResourceConstructor.construct(cpus, mem))
                .setSlaveId(Protos.SlaveID.newBuilder()
                        .setValue(slaveId)
                        .build())
                .setFrameworkId(driver.getFrameworkInfo().getId())
                .setHostname("127.0.0.1:5051")
                .setId(Protos.OfferID.newBuilder().setValue(offerId).build());
        return builder.build();
    }

}

