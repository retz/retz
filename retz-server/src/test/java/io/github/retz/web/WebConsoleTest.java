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
import io.github.retz.cli.ClientCLIConfig;
import io.github.retz.db.Database;
import io.github.retz.protocol.*;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.MesosContainer;
import io.github.retz.scheduler.*;
import org.apache.mesos.Protos;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static spark.Spark.awaitInitialization;

public class WebConsoleTest {
    private WebConsole webConsole;
    private Client webClient;
    private ObjectMapper mapper;
    private ServerConfiguration config;
    private ClientCLIConfig cliConfig;
    private final List<String> BASE_ORDER_BY = Arrays.asList("id");

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

        // Non-TLS tests are not to be done, I believe when it works with TLS tests, it should work
        // on Non-TLS setup too. I believe his is because Sparkjava does not cleanly clear TLS setting in
        // Spark.stop(), because with retz.properties it succeeds alone, but fails when right after TLS tests.
        // TODO: investigate and report this to sparkjava
        InputStream in = Launcher.class.getResourceAsStream("/retz-tls.properties");
        Launcher.Configuration conf = new Launcher.Configuration(new ServerConfiguration(in));

        mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());

        RetzScheduler scheduler = new RetzScheduler(conf, frameworkInfo);
        config = conf.getServerConfig();
        webConsole = new WebConsole(config);
        WebConsole.set(scheduler, null);
        awaitInitialization();

        Database.getInstance().init(config);

        cliConfig = new ClientCLIConfig("src/test/resources/retz-tls-client.properties");
        System.err.println(config.authenticationEnabled());
        System.err.println(config.toString());
        webClient = Client.newBuilder(cliConfig.getUri())
                .setAuthenticator(cliConfig.getAuthenticator())
                .checkCert(!cliConfig.insecure())
                .build();
    }

    /**
     * Cleans up the test.
     *
     * @throws Exception if some errors were occurred
     */
    @After
    public void tearDown() throws Exception {
        webClient.close();
        webConsole.stop();
        Database.getInstance().clear();
        Database.getInstance().stop();
    }

    @Test
    public void version() {
        System.err.println(Client.VERSION_STRING);
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void list() throws Exception {
        ListJobResponse res = (ListJobResponse) webClient.list(64);
        assertTrue(res.queue().isEmpty());
        assertTrue(res.running().isEmpty());
        assertTrue(res.finished().isEmpty());
    }

    @Test
    public void loadApp() throws Exception {
        {
            String[] files = {"http://example.com:234/foobar/test.tar.gz"};
            Application app = new Application("foobar", new LinkedList<>(), new LinkedList<String>(), Arrays.asList(files),
                    Optional.empty(), Optional.empty(), config.getUser().keyId(),
                    0, new MesosContainer(), true);
            Response res = webClient.load(app);
            assertThat(res, instanceOf(LoadAppResponse.class));
            assertThat(res.status(), is("ok"));

            Optional<Application> app2 = Applications.get(app.getAppid());
            assertTrue(app2.isPresent());
            assertThat(app2.get().getAppid(), is(app.getAppid()));
        }

        {
            GetAppResponse res = (GetAppResponse) webClient.getApp("foobar");
            assertThat(res.status(), is("ok"));
            Application app = res.application();
            assertThat(app.getAppid(), is("foobar"));
        }
        webClient.unload("foobar");
    }

    @Test
    public void schedule() throws Exception {
        List<Job> maybeJob = JobQueue.findFit(BASE_ORDER_BY, new ResourceQuantity(10000, 10000, 0, 0, 0));
        assertTrue(maybeJob.isEmpty());

        {
            // Job request without app must fail
            String cmd = "Mmmmmmmmmy commmmmand1!!!!!";
            Response res = webClient.schedule(new Job("foobar", cmd, null, 1, 256));
            assertThat(res, instanceOf(ErrorResponse.class));

            maybeJob = JobQueue.findFit(BASE_ORDER_BY, new ResourceQuantity(1000, 10000, 0, 0, 0));
            assertTrue(maybeJob.isEmpty());

            GetJobResponse getJobResponse = (GetJobResponse) webClient.getJob(235561234);
            assertFalse(getJobResponse.job().isPresent());
        }

        {
            String[] files = {"http://example.com:234/foobar/test.tar.gz"};
            Application app = new Application("foobar", new LinkedList<>(), new LinkedList<String>(), Arrays.asList(files),
                    Optional.empty(), Optional.empty(), config.getUser().keyId(),
                    0, new MesosContainer(), true);
            Response res = webClient.load(app);
            System.err.println(">>>" + res.status());
            assertThat(res, instanceOf(LoadAppResponse.class));
            assertThat(res.status(), is("ok"));

            Optional<Application> app2 = Applications.get("foobar");
            assertTrue(app2.isPresent());
            assertThat(app2.get().getAppid(), is("foobar"));

            res = webClient.getApp("foobar");
            assertThat(res, instanceOf(GetAppResponse.class));
            GetAppResponse getAppResponse = (GetAppResponse) res;
            assertThat(getAppResponse.application().getAppid(), is("foobar"));
            assertThat(getAppResponse.application().getFiles().size(), is(1));
        }
        maybeJob = JobQueue.findFit(BASE_ORDER_BY, new ResourceQuantity(10000, 10000, 0, 0, 0));
        assertTrue(maybeJob.isEmpty());

        {
            // You know, these spaces are to be normalized
            String cmd = "Mmmmmmmmmy commmmmand1!!!!!";
            Response res = webClient.schedule(new Job("foobar", cmd, null, 1, 200));
            assertThat(res, instanceOf(ScheduleResponse.class));
            ScheduleResponse sres = (ScheduleResponse) res;
            assertNotNull(sres.job.scheduled());
            assertThat(sres.job.id(), is(greaterThanOrEqualTo(0)));
            System.err.println(sres.job.scheduled());

            ListJobResponse listJobResponse = (ListJobResponse) webClient.list(64);
            assertThat(listJobResponse.queue().size(), is(1));

            GetJobResponse getJobResponse = (GetJobResponse) webClient.getJob(sres.job.id());
            Assert.assertEquals(sres.job.cmd(), getJobResponse.job().get().cmd());

            maybeJob = JobQueue.findFit(BASE_ORDER_BY, new ResourceQuantity(10000, 10000, 0, 0, 0));
            assertFalse(maybeJob.isEmpty());
            assertThat(maybeJob.get(0).cmd(), is(cmd));
            assertThat(maybeJob.get(0).appid(), is("foobar"));

            ListFilesResponse listFilesResponse = (ListFilesResponse) webClient.listFiles(sres.job.id(), ListFilesRequest.DEFAULT_SANDBOX_PATH);
            assertTrue(listFilesResponse.entries().isEmpty());

            GetFileResponse getFileResponse = (GetFileResponse) webClient.getFile(sres.job.id(), "stdout", 0, 20000);
            assertFalse(getFileResponse.file().isPresent());
        }

        {
            Response res = webClient.getApp("no such app");
            assertThat(res, instanceOf(ErrorResponse.class));
        }
        webClient.unload("foobar");
    }

    @Test
    public void runFail() throws Exception {
        JobQueue.clear();
        List<Job> maybeJob = JobQueue.findFit(BASE_ORDER_BY, new ResourceQuantity(10000, 10000, 0, 0, 0));
        assertTrue(maybeJob.isEmpty());

        {
            // Job request without app must fail
            String cmd = "Mmmmmmmmmy commmmmand1!!!!!";
            Job job = new Job("foobar-nosuchapp", cmd, null, 1, 256);
            Job done = webClient.run(job);
            assertNull(done);

            maybeJob = JobQueue.findFit(BASE_ORDER_BY, new ResourceQuantity(10000, 10000, 0, 0, 0));
            assertTrue(maybeJob.isEmpty());
        }
    }

    @Test
    public void kill() throws Exception {
        Response res = webClient.kill(0);
        System.err.println(res.status());
    }

    @Test
    public void ping() throws IOException {
        Client c = Client.newBuilder(config.getUri())
                .setAuthenticator(config.getAuthenticator())
                .checkCert(!config.insecure())
                .build();
        assertTrue(c.ping());
    }

    @Test
    public void status() throws Exception {
        Application app = new Application("fooapp", Arrays.asList(), Arrays.asList(), Arrays.asList(),
                Optional.empty(), Optional.empty(), config.getUser().keyId(),
                0, new MesosContainer(), true);
        Database.getInstance().addApplication(app);
        Job job = new Job(app.getAppid(), "foocmd", null, 12000, 12000);
        JobQueue.push(job);

        URL url = new URL(config.getUri().toASCIIString() + "/status");
        System.err.println(url);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setDoOutput(true);
        Response res = mapper.readValue(conn.getInputStream(), Response.class);
        assertThat(res, instanceOf(StatusResponse.class));
        StatusResponse statusResponse = (StatusResponse) res;

        System.err.println(statusResponse.queueLength());
        assertThat(statusResponse.queueLength(), is(1));
        assertThat(statusResponse.sessionLength(), is(0));
    }
}
