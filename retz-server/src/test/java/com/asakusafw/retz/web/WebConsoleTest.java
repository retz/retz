/**
 *    Retz
 *    Copyright (C) 2016 Nautilus Technologies, KK.
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
package com.asakusafw.retz.web;

import com.asakusafw.retz.cli.FileConfiguration;
import com.asakusafw.retz.mesos.Applications;
import com.asakusafw.retz.mesos.JobQueue;
import com.asakusafw.retz.mesos.MesosFrameworkLauncher;
import com.asakusafw.retz.mesos.RetzScheduler;
import com.asakusafw.retz.protocol.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.apache.commons.io.IOUtils;
import org.apache.mesos.Protos;
import org.junit.After;
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

import static org.junit.Assert.assertEquals;
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
        assert webClient.connect();
        webClient.disconnect();
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void list() throws Exception {
        assert webClient.connect();
        ListJobResponse res = (ListJobResponse) webClient.list();
        assert res.queue().isEmpty();
        webClient.disconnect();
    }

    @Test
    public void loadApp() throws Exception {
        JobQueue.clear();
        assert webClient.connect();
        {
            String[] files = {"http://example.com:234/foobar/test.tar.gz"};
            Response res = webClient.load("foobar", new LinkedList<>(), Arrays.asList(files));
            assert res instanceof LoadAppResponse;
            assert res.status().equals("ok");

            Optional<Applications.Application> app = Applications.get("foobar");
            assert app.isPresent();
            assert app.get().appName.equals("foobar");
        }

        {
            ListAppResponse res = (ListAppResponse) webClient.listApp();
            assert res.status().equals("ok");
            System.err.println(res.applicationList().size());
            Application app = res.applicationList().get(0);
            assert app.getAppid().equals("foobar");
            System.out.println("================================" + res.applicationList());
            for(Application a: res.applicationList()){
                System.out.println("=========== app:" + a);
                System.out.println("=========== app:" + a.getAppid());
                System.out.println("=========== app:" + a.getFiles());
            }
            assertEquals(1, res.applicationList().size());
        }
        webClient.unload("foobar");
        webClient.disconnect();
    }

    @Test
    public void schedule() throws Exception {
        JobQueue.clear();
        Optional<Job> maybeJob = JobQueue.pop();
        assert !maybeJob.isPresent();

        assert webClient.connect();

        {
            // Job request without app must fail
            String cmd = "Mmmmmmmmmy commmmmand1!!!!!";
            Response res = webClient.schedule("foobar", cmd, null, new Range(1, 0), new Range(256, 0));
            assert res instanceof ErrorResponse;

            maybeJob = JobQueue.pop();
            assert !maybeJob.isPresent();
        }

        {
            String[] files = {"http://example.com:234/foobar/test.tar.gz"};
            Response res = webClient.load("foobar", new LinkedList<>(), Arrays.asList(files));
            assert res instanceof LoadAppResponse;
            assert res.status().equals("ok");

            Optional<Applications.Application> app = Applications.get("foobar");
            assert app.isPresent();
            assert app.get().appName.equals("foobar");
        }
        maybeJob = JobQueue.pop();
        assert !maybeJob.isPresent();

        {
            // You know, these spaces are to be normalized
            String cmd = "Mmmmmmmmmy commmmmand1!!!!!";
            Response res = webClient.schedule("foobar", cmd, null, new Range(1, 0), new Range(200, 0));
            assert res instanceof ScheduleResponse;
            ScheduleResponse sres = (ScheduleResponse) res;
            assert sres.job.scheduled() != null;
            assert sres.job.id() >= 0;
            System.err.println(sres.job.scheduled());

            ListJobResponse listJobResponse = (ListJobResponse) webClient.list();
            assert listJobResponse.queue().size() == 1;

            maybeJob = JobQueue.pop();
            assert maybeJob.isPresent();
            assert maybeJob.get().cmd().equals(cmd);
            assert maybeJob.get().appid().equals("foobar");
        }
        webClient.unload("foobar");
        webClient.disconnect();

    }

    @Test
    public void runFail() throws Exception {
        JobQueue.clear();
        Optional<Job> maybeJob = JobQueue.pop();
        assert !maybeJob.isPresent();

        assert webClient.connect();

        {
            // Job request without app must fail
            String cmd = "Mmmmmmmmmy commmmmand1!!!!!";
            Job job = new Job(cmd, "", "", "", null, -1, 234, "", null, "foobar-nosuchapp", "name", new Range(1, 0), new Range(256, 0), true);
            Job done = webClient.run(job);
            assert done == null;

            maybeJob = JobQueue.pop();
            assert !maybeJob.isPresent();
        }
    }

    @Test
    public void kill() throws Exception {
        assert webClient.connect();
        KillResponse res = (KillResponse) webClient.kill(0);
        System.err.println(res.status());
        webClient.disconnect();
    }

    @Test
    public void watch() throws Exception {
        assert webClient.connect();
        webClient.startWatch((watchResponse) -> {
            assert watchResponse instanceof WatchResponse;
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
        assert "OK".equals(writer.toString());
    }

    @Test
    public void status() throws Exception {
        Job job = new Job("fooapp", "foocmd", null, new Range(12000, 0), new Range(12000, 0));
        JobQueue.push(job);
        assert webClient.connect();

        URL url = new URL("http://localhost:24301/status");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setDoOutput(true);
        Response res = mapper.readValue(conn.getInputStream(), Response.class);
        assert res instanceof StatusResponse;
        StatusResponse statusResponse = (StatusResponse) res;

        System.err.println(statusResponse.queueLength());
        assert statusResponse.queueLength() == 1;
        assert statusResponse.sessionLength() == 1;
        webClient.disconnect();
    }
}