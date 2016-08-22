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
package io.github.retz.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.retz.cli.FileConfiguration;
import io.github.retz.protocol.*;
import io.github.retz.scheduler.Applications;
import io.github.retz.scheduler.JobQueue;
import io.github.retz.scheduler.MesosFrameworkLauncher;
import io.github.retz.scheduler.RetzScheduler;
import org.apache.commons.io.IOUtils;
import org.apache.mesos.Protos;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static spark.Spark.awaitInitialization;

public class WebConsoleTest {
    private WebConsole webConsole;
    private Client webClient;
    private ObjectMapper mapper;

    /**
     * Initializes the test.
     *
     * @throws Exception if some errors were occurred
     */
    @Before
    public void setUp() throws Exception {
        Protos.FrameworkInfo frameworkInfo = Protos.FrameworkInfo.newBuilder()
                .setUser("")
                .setName(RetzScheduler.FRAMEWORK_NAME)
                .build();

        InputStream in = MesosFrameworkLauncher.class.getResourceAsStream("/retz.properties");
        MesosFrameworkLauncher.Configuration conf = new MesosFrameworkLauncher.Configuration(new FileConfiguration(in));

        mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());

        RetzScheduler scheduler = new RetzScheduler(conf, frameworkInfo);
        webConsole = new WebConsole(24301);
        WebConsole.setScheduler(scheduler);
        awaitInitialization();
        webClient = new Client("ws://localhost:24301/cui");
    }

    /**
     * Cleans up the test.
     *
     * @throws Exception if some errors were occurred
     */
    @After
    public void tearDown() throws Exception {
        webClient.disconnect();
        webConsole.stop();
        JobQueue.clear();
    }

    @Test
    public void connect() throws Exception {
        assertTrue(webClient.connect());
        webClient.disconnect();
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void list() throws Exception {
        assertTrue(webClient.connect());
        ListJobResponse res = (ListJobResponse) webClient.list(64);
        assertTrue(res.queue().isEmpty());
        assertTrue(res.running().isEmpty());
        assertTrue(res.finished().isEmpty());
        webClient.disconnect();
    }

    @Test
    public void loadApp() throws Exception {
        JobQueue.clear();
        assertTrue(webClient.connect());
        {
            String[] files = {"http://example.com:234/foobar/test.tar.gz"};
            Response res = webClient.load("foobar", new LinkedList<>(), Arrays.asList(files));
            assertThat(res, instanceOf(LoadAppResponse.class));
            assertThat(res.status(), is("ok"));

            Optional<Applications.Application> app = Applications.get("foobar");
            assertTrue(app.isPresent());
            assertThat(app.get().appName, is("foobar"));
        }

        {
            ListAppResponse res = (ListAppResponse) webClient.listApp();
            assertThat(res.status(), is("ok"));
            System.err.println(res.applicationList().size());
            Application app = res.applicationList().get(0);
            assertThat(app.getAppid(), is("foobar"));
            System.out.println("================================" + res.applicationList());
            for (Application a : res.applicationList()) {
                System.out.println("=========== app:" + a);
                System.out.println("=========== app:" + a.getAppid());
                System.out.println("=========== app:" + a.getFiles());
            }
            assertThat(res.applicationList().size(), is(1));
        }
        webClient.unload("foobar");
        webClient.disconnect();
    }

    @Test
    public void schedule() throws Exception {
        JobQueue.clear();
        Optional<Job> maybeJob = JobQueue.pop();
        assertFalse(maybeJob.isPresent());

        assertTrue(webClient.connect());

        {
            // Job request without app must fail
            String cmd = "Mmmmmmmmmy commmmmand1!!!!!";
            Response res = webClient.schedule(new Job("foobar", cmd, null, new Range(1, 0), new Range(256, 0)));
            assertThat(res, instanceOf(ErrorResponse.class));

            maybeJob = JobQueue.pop();
            assertFalse(maybeJob.isPresent());

            GetJobResponse getJobResponse = (GetJobResponse) webClient.getJob(235561234);
            assertFalse(getJobResponse.job().isPresent());
        }

        {
            String[] files = {"http://example.com:234/foobar/test.tar.gz"};
            Response res = webClient.load("foobar", new LinkedList<>(), Arrays.asList(files));
            assertThat(res, instanceOf(LoadAppResponse.class));
            assertThat(res.status(), is("ok"));

            Optional<Applications.Application> app = Applications.get("foobar");
            assertTrue(app.isPresent());
            assertThat(app.get().appName, is("foobar"));
        }
        maybeJob = JobQueue.pop();
        assertFalse(maybeJob.isPresent());

        {
            // You know, these spaces are to be normalized
            String cmd = "Mmmmmmmmmy commmmmand1!!!!!";
            Response res = webClient.schedule(new Job("foobar", cmd, null, new Range(1, 0), new Range(200, 0)));
            assertThat(res, instanceOf(ScheduleResponse.class));
            ScheduleResponse sres = (ScheduleResponse) res;
            assertNotNull(sres.job.scheduled());
            assertThat(sres.job.id(), is(greaterThanOrEqualTo(0)));
            System.err.println(sres.job.scheduled());

            ListJobResponse listJobResponse = (ListJobResponse) webClient.list(64);
            assertThat(listJobResponse.queue().size(), is(1));

            GetJobResponse getJobResponse = (GetJobResponse) webClient.getJob(sres.job.id());
            Assert.assertEquals(sres.job.cmd(), getJobResponse.job().get().cmd());

            maybeJob = JobQueue.pop();
            assertTrue(maybeJob.isPresent());
            assertThat(maybeJob.get().cmd(), is(cmd));
            assertThat(maybeJob.get().appid(), is("foobar"));
        }
        webClient.unload("foobar");
        webClient.disconnect();

    }

    @Test
    public void runFail() throws Exception {
        JobQueue.clear();
        Optional<Job> maybeJob = JobQueue.pop();
        assertFalse(maybeJob.isPresent());

        assertTrue(webClient.connect());

        {
            // Job request without app must fail
            String cmd = "Mmmmmmmmmy commmmmand1!!!!!";
            Job job = new Job("foobar-nosuchapp", cmd, null, new Range(1, 0), new Range(256, 0));
            Job done = webClient.run(job);
            assertNull(done);

            maybeJob = JobQueue.pop();
            assertFalse(maybeJob.isPresent());
        }
    }

    @Test
    public void kill() throws Exception {
        assertTrue(webClient.connect());
        KillResponse res = (KillResponse) webClient.kill(0);
        System.err.println(res.status());
        webClient.disconnect();
    }

    @Test
    public void watch() throws Exception {
        assertTrue(webClient.connect());
        webClient.startWatch((watchResponse) -> {
            assertThat(watchResponse,  instanceOf(WatchResponse.class));
            System.err.println(((WatchResponse) watchResponse).status());
            return false;
        });
        //System.err.println(res.status);
        webClient.disconnect();
    }

    @Test
    public void ping() throws IOException {
        URL url = new URL("http://localhost:24301/ping");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setDoOutput(true);
        StringWriter writer = new StringWriter();
        IOUtils.copy(conn.getInputStream(), writer, "UTF-8");
        assertThat(writer.toString(), is("OK"));
    }

    @Test
    public void status() throws Exception {
        Job job = new Job("fooapp", "foocmd", null, new Range(12000, 0), new Range(12000, 0));
        JobQueue.push(job);
        assertTrue(webClient.connect());

        URL url = new URL("http://localhost:24301/status");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setDoOutput(true);
        Response res = mapper.readValue(conn.getInputStream(), Response.class);
        assertThat(res, instanceOf(StatusResponse.class));
        StatusResponse statusResponse = (StatusResponse) res;

        System.err.println(statusResponse.queueLength());
        assertThat(statusResponse.queueLength(), is(1));
        assertThat(statusResponse.sessionLength(), is(1));
        webClient.disconnect();
    }
}
