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
package io.github.retz.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.retz.cli.ClientCLIConfig;
import io.github.retz.cli.TimestampHelper;
import io.github.retz.db.Database;
import io.github.retz.misc.ApplicationBuilder;
import io.github.retz.protocol.*;
import io.github.retz.protocol.data.*;
import io.github.retz.scheduler.*;
import org.apache.mesos.Protos;
import org.hamcrest.Matchers;
import org.junit.*;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static spark.Spark.awaitInitialization;

// These tests must pass regardless of any client/server communication configuration
@Ignore
public class WebConsoleCommonTests {
    private static final List<String> BASE_ORDER_BY = Arrays.asList("id");
    private Client webClient;
    private ObjectMapper mapper;
    private ServerConfiguration config;
    private ClientCLIConfig cliConfig;

    Launcher.Configuration makeConfig() throws Exception {
        throw new RuntimeException("This class shouldn't be tested");
    }

    ClientCLIConfig makeClientConfig() throws Exception {
        throw new RuntimeException("This class shouldn't be tested");
    }

    @Before
    public void setUp() throws Throwable {

        Protos.FrameworkInfo frameworkInfo = Protos.FrameworkInfo.newBuilder()
                .setUser("")
                .setName(RetzScheduler.FRAMEWORK_NAME)
                .build();

        // Non-TLS tests are not to be done, I believe when it works with TLS tests, it should work
        // on Non-TLS setup too. I believe his is because Sparkjava does not cleanly clear TLS setting in
        // Spark.stop(), because with retz.properties it succeeds alone, but fails when right after TLS tests.
        // TODO: investigate and report this to sparkjava
        Launcher.Configuration conf = makeConfig();

        mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());

        RetzScheduler scheduler = new RetzScheduler(conf, frameworkInfo);
        config = conf.getServerConfig();
        Database.getInstance().init(config);
        assertTrue(Database.getMigrator().isFinished());

        WebConsole.set(scheduler, null);
        WebConsole.start(config);
        awaitInitialization();

        cliConfig = makeClientConfig();
        System.err.println(config.authenticationEnabled());
        System.err.println(config.toString());
        webClient = Client.newBuilder(cliConfig.getUri())
                .setAuthenticator(cliConfig.getAuthenticator())
                .checkCert(!cliConfig.insecure())
                .setVerboseLog(true)
                .build();
    }

    @After
    public void tearDown() throws Exception {
        webClient.close();
        WebConsole.stop();
        Database.getInstance().clear();
        Database.getInstance().stop();
        // This is because Spark.stop() is impelemented in asynchronous way,
        // there are no way to wait for spark.Service.initialized become false.
        // If this sleep is too short, IllegalStateException("This must be done before route mapping has begun");
        // will be thrown.
        Thread.sleep(1024);
    }

    @Test
    public void version() throws IOException {
        System.err.println(Client.VERSION_STRING);
        assertTrue(webClient.ping());
    }

    @Test
    public void list() throws Exception {
        {
            ListJobResponse res = (ListJobResponse) webClient.list(Job.JobState.QUEUED, Optional.empty());
            assertTrue(res.jobs().isEmpty());
            assertFalse(res.more());
        }
        {
            ListJobResponse res = (ListJobResponse) webClient.list(Job.JobState.STARTING, Optional.empty());
            assertTrue(res.jobs().isEmpty());
            assertFalse(res.more());
        }
        {
            ListJobResponse res = (ListJobResponse) webClient.list(Job.JobState.FINISHED, Optional.empty());
            assertTrue(res.jobs().isEmpty());
            assertFalse(res.more());
        }
    }

    @Test
    public void loadApp() throws Exception {
        {
            String[] files = {"http://example.com:234/foobar/test.tar.gz"};
            Application app = new Application("foobar", Collections.emptyList(), Arrays.asList(files),
                    Optional.empty(), config.getUser().keyId(),
                    0, new MesosContainer(), true);
            Response res = webClient.load(app);
            assertThat(res, instanceOf(LoadAppResponse.class));
            assertThat(res.status(), Matchers.is("ok"));

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
        List<Job> maybeJob = JobQueue.findFit(BASE_ORDER_BY, new ResourceQuantity(10000, 10000, 0, 0, 0, 0));
        assertTrue(maybeJob.isEmpty());

        {
            // Job request without app must fail
            String cmd = "Mmmmmmmmmy commmmmand1!!!!!";
            Response res = webClient.schedule(new Job("foobar", cmd, null, 1, 256, 32));
            assertThat(res, instanceOf(ErrorResponse.class));

            maybeJob = JobQueue.findFit(BASE_ORDER_BY, new ResourceQuantity(1000, 10000, 0, 0, 0, 0));
            assertTrue(maybeJob.isEmpty());

            GetJobResponse getJobResponse = (GetJobResponse) webClient.getJob(235561234);
            assertFalse(getJobResponse.job().isPresent());
        }

        {
            String[] files = {"http://example.com:234/foobar/test.tar.gz"};
            Application app = new Application("foobar", Collections.emptyList(), Arrays.asList(files),
                    Optional.empty(), config.getUser().keyId(),
                    0, new MesosContainer(), true);
            Response res = webClient.load(app);
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
        maybeJob = JobQueue.findFit(BASE_ORDER_BY, new ResourceQuantity(10000, 10000, 0, 0, 0, 0));
        assertTrue(maybeJob.isEmpty());

        {
            // You know, these spaces are to be normalized
            String cmd = "Mmmmmmmmmy commmmmand1!!!!!";
            Response res = webClient.schedule(new Job("foobar", cmd, null, 1, 200, 32));
            assertThat(res, instanceOf(ScheduleResponse.class));
            ScheduleResponse sres = (ScheduleResponse) res;
            assertNotNull(sres.job.scheduled());
            assertThat(sres.job.id(), is(greaterThanOrEqualTo(0)));
            System.err.println(sres.job.scheduled());

            assertThat(ClientHelper.queue(webClient).size(), is(1));

            GetJobResponse getJobResponse = (GetJobResponse) webClient.getJob(sres.job.id());
            Assert.assertEquals(sres.job.cmd(), getJobResponse.job().get().cmd());

            maybeJob = JobQueue.findFit(BASE_ORDER_BY, new ResourceQuantity(10000, 10000, 0, 0, 0, 0));
            assertFalse(maybeJob.isEmpty());
            assertThat(maybeJob.get(0).cmd(), is(cmd));
            assertThat(maybeJob.get(0).appid(), is("foobar"));

            ListFilesResponse listFilesResponse = (ListFilesResponse) webClient.listFiles(sres.job.id(), ListFilesRequest.DEFAULT_SANDBOX_PATH);
            assertTrue(listFilesResponse.entries().isEmpty());

            Response response = webClient.getFile(sres.job.id(), "stdout", 0, 20000);
            GetFileResponse getFileResponse = (GetFileResponse) response;
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
        List<Job> maybeJob = JobQueue.findFit(BASE_ORDER_BY, new ResourceQuantity(10000, 10000, 0, 0, 0, 0));
        assertTrue(maybeJob.isEmpty());

        {
            // Job request without app must fail
            String cmd = "Mmmmmmmmmy commmmmand1!!!!!";
            Job job = new Job("foobar-nosuchapp", cmd, null, 1, 32, 256);
            Job done = webClient.run(job);
            assertNull(done);

            maybeJob = JobQueue.findFit(BASE_ORDER_BY, new ResourceQuantity(10000, 10000, 0, 0, 0, 0));
            assertTrue(maybeJob.isEmpty());
        }
    }

    @Test
    public void kill() throws Exception {
        {
            Response res = webClient.kill(0);
            assertThat(res, instanceOf(ErrorResponse.class));
            assertEquals("No such job: 0", res.status());
        }
        Application app = new ApplicationBuilder("app", config.getUser().keyId()).build();
        LoadAppResponse loadAppResponse = (LoadAppResponse) webClient.load(app);
        assertEquals("ok", loadAppResponse.status());
        {
            Job job = new Job("app", "sleep 1000", new Properties(), 1, 64, 0);
            ScheduleResponse scheduleResponse = (ScheduleResponse) webClient.schedule(job);
            KillResponse killResponse = (KillResponse) webClient.kill(scheduleResponse.job().id());
            assertEquals("ok", killResponse.status());
            GetJobResponse getJobResponse = (GetJobResponse) webClient.getJob(scheduleResponse.job().id());
            System.err.println(getJobResponse.job().get().pp());
            assertEquals(Job.JobState.KILLED, getJobResponse.job().get().state());
        }
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
        Application app = new Application("fooapp", Collections.emptyList(), Collections.emptyList(), Optional.empty(), config.getUser().keyId(),
                0, new MesosContainer(), true);
        Database.getInstance().addApplication(app);
        Job job = new Job(app.getAppid(), "foocmd", null, 12000, 12000, 12000);
        job.schedule(JobQueue.issueJobId(), TimestampHelper.now());
        JobQueue.push(job);
        StatusCache.updateUsedResources();

        try (Client c = Client.newBuilder(config.getUri())
                .setAuthenticator(config.getAuthenticator())
                .checkCert(!config.insecure())
                .build()) {
            Response res = c.status();
            assertThat(res, instanceOf(StatusResponse.class));
            StatusResponse statusResponse = (StatusResponse) res;

            System.err.println(statusResponse.queueLength());
            assertThat(statusResponse.queueLength(), is(1));
            assertThat(statusResponse.runningLength(), is(0));
        }
    }

    // Checks isolation between users.
    // All combination of client APIs must be safely excluded,
    // Any request to server imitation other users must fail.
    @Test
    public void isolation() throws Exception {
        // Prepare data
        Application app1 = new Application("app1", Collections.emptyList(), Collections.emptyList(),
                Optional.empty(), cliConfig.getUser().keyId(),
                0, new MesosContainer(), true);
        Job job1 = new Job("app1", "ls", new Properties(), 1, 32, 32);

        {
            Response res = webClient.load(app1);
            assertEquals("ok", res.status());
        }
        {
            Response res = webClient.schedule(job1);
            assertEquals("ok", res.status());
            ScheduleResponse scheduleResponse = (ScheduleResponse) res;
            job1 = scheduleResponse.job();
        }
        System.err.println("Job " + job1.id() + " has been scheduled");

        // Here comes a new challenger!!
        User charlie = new User("charlie", "snoops!", true, "Charlie the theif");
        Database.getInstance().addUser(charlie);

        ClientCLIConfig c2 = new ClientCLIConfig(cliConfig);
        c2.setUser(charlie);

        assertEquals("deadbeef", cliConfig.getUser().keyId());
        assertEquals("charlie", c2.getUser().keyId());

        try (Client client2 = Client.newBuilder(c2.getUri())
                .setAuthenticator(c2.getAuthenticator())
                .checkCert(!c2.insecure())
                .setVerboseLog(true)
                .build()) {

            {
                Response res = client2.listApp();
                assertEquals("ok", res.status());
                ListAppResponse listAppResponse = (ListAppResponse) res;
                assertTrue(listAppResponse.applicationList().isEmpty());
            }
            {
                Response res = client2.getApp("app1");
                assertThat(res, instanceOf(ErrorResponse.class));
            }
            {
                assertTrue(ClientHelper.finished(client2).isEmpty());
                assertTrue(ClientHelper.queue(client2).isEmpty());
                assertTrue(ClientHelper.running(client2).isEmpty());

            }
            { // Charlie tries to snoop Job info of Alice
                Response res = client2.getJob(job1.id());
                assertThat(res, instanceOf(GetJobResponse.class));
                GetJobResponse getJobResponse = (GetJobResponse) res;
                assertFalse(getJobResponse.job().isPresent());
                System.err.println(res.status());
            }
            { // Charlie tries to kill Alice's job
                Response res = client2.kill(job1.id());
                assertThat(res.status(), not(is("ok")));
                assertThat(res, instanceOf(ErrorResponse.class));

                GetJobResponse getJobResponse = (GetJobResponse) webClient.getJob(job1.id());
                assertThat(getJobResponse.job().get().state(), is(Job.JobState.QUEUED));
            }
            { // Charlie tries to snoop files in Alice's job sandbox
                Response res = client2.getFile(job1.id(), "stdout", 0, -1);
                assertThat(res, instanceOf(GetFileResponse.class));
                GetFileResponse getFileResponse = (GetFileResponse) res;
                assertFalse(getFileResponse.job().isPresent());
                assertFalse(getFileResponse.file().isPresent());
                System.err.println(res.status());
            }
            { // Charlie tries to snoop files in Alice's job sandbox
                Response res = client2.listFiles(job1.id(), ListFilesRequest.DEFAULT_SANDBOX_PATH);
                assertThat(res, instanceOf(ListFilesResponse.class));
                ListFilesResponse listFilesResponse = (ListFilesResponse) res;
                assertFalse(listFilesResponse.job().isPresent());
                assertTrue(listFilesResponse.entries().isEmpty());
                System.err.println(res.status());
            }
            {
                // Charlie tries to steal Alice's whole application
                Response res = client2.load(app1);
                assertThat(res, instanceOf(ErrorResponse.class));
                System.err.println(res.status());
            }
            { // Charlie tries to steal Alice's application name
                Application app2 = new Application("app1", Collections.emptyList(), Collections.emptyList(),
                        Optional.empty(), c2.getUser().keyId(),
                        0, new MesosContainer(), true);
                Response res = client2.load(app2);
                assertThat(res, instanceOf(ErrorResponse.class));
                System.err.println(res.status());
            }
            { // Charlie tries to be Alice
                System.err.println(cliConfig.getUser().keyId());
                Application app2 = new Application("app2", Collections.emptyList(), Collections.emptyList(),
                        Optional.empty(), cliConfig.getUser().keyId(),
                        0, new MesosContainer(), true);
                Response res = client2.load(app2);
                assertThat(res, instanceOf(ErrorResponse.class));
                System.err.println(res.status());
            }

            Job job2 = new Job("app1", "ls", new Properties(), 1, 32, 32);
            { // Charlie tries to steal Alice's applications
                Response res = client2.schedule(job2);
                assertThat(res, instanceOf(ErrorResponse.class));
                System.err.println(res.status());
            }
            { // Charlie tries to steal Alice's applications
                Job job3 = client2.run(job2);
                assertEquals(null, job3);
            }
        }
    }

    @Test
    public void disableUser() throws Exception {
        User user = config.getUser();
        List<String> e = Collections.emptyList();
        Application application = new Application("t", e, e, Optional.empty(),
                user.keyId(), 0, new MesosContainer(), true);

        String jar = "/build/libs/retz-admin-all.jar";
        String cfg = "/retz-persistent.properties";

        Client client = webClient;
        Response res;

        res = client.load(application);
        assertEquals("ok", res.status());

        res = client.schedule(new Job("t", "ls", new Properties(), 1, 32, 32));
        ScheduleResponse scheduleResponse = (ScheduleResponse) res;
        Job job1 = scheduleResponse.job();

        System.err.println("Disable user " + user.keyId());
        Database.getInstance().enableUser(user.keyId(), false);
        {
            Optional<User> u = Database.getInstance().getUser(user.keyId());
            assertFalse(u.get().enabled());
        }

        res = client.getJob(job1.id());
        System.err.println(res.status());
        Assert.assertThat(res, instanceOf(ErrorResponse.class));

        res = client.load(new Application("t2", e, e, Optional.empty(),
                user.keyId(), 0, new MesosContainer(), true));
        System.err.println(res.status());
        Assert.assertThat(res, instanceOf(ErrorResponse.class));

        res = client.schedule(new Job("t", "echo prohibited job", new Properties(), 1, 32, 32));
        System.err.println(res.status());
        Assert.assertThat(res, instanceOf(ErrorResponse.class));

        System.err.println("Enable user");
        Database.getInstance().enableUser(user.keyId(), true);

        res = client.getJob(job1.id());
        System.err.println(res.status());
        assertEquals("ok", res.status());

        res = client.load(new Application("t2", e, e, Optional.empty(),
                user.keyId(), 0, new MesosContainer(), true));
        System.err.println(res.status());
        assertEquals("ok", res.status());

        res = client.schedule(new Job("t", "echo okay job", new Properties(), 1, 32, 32));
        System.err.println(res.status());
        assertEquals("ok", res.status());
    }

    @Test
    public void getBinaryFile() throws Exception {
        Application app = new Application("fooapp", Collections.emptyList(), Collections.emptyList(), Optional.empty(), config.getUser().keyId(),
                0, new MesosContainer(), true);
        Database.getInstance().addApplication(app);
        Job job = new Job(app.getAppid(), "hoge", null, 1, 200, 32);
        Database.getInstance().safeAddJob(job);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            webClient.getBinaryFile(job.id(), "hoge +%/", out);
        } catch (FileNotFoundException e) {
            assertTrue(e.getMessage().endsWith("://localhost:9091/job/0/download?path=hoge+%2B%25%2F"));
        }
    }
}
