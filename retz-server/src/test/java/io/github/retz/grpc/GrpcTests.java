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
package io.github.retz.grpc;

import io.github.retz.cli.ClientCLIConfig;
import io.github.retz.db.Database;
import io.github.retz.protocol.data.*;
import io.github.retz.scheduler.Applications;
import io.github.retz.scheduler.JobQueue;
import io.github.retz.scheduler.Launcher;
import io.github.retz.scheduler.ServerConfiguration;
import io.github.retz.web.ClientHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.sun.org.apache.xerces.internal.util.PropertyState.is;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;
import static org.junit.Assert.*;

public class GrpcTests {
    private RetzServer server;
    private ServerConfiguration config;
    private int port;
    private ClientCLIConfig cliConfig;
    private static final List<String> BASE_ORDER_BY = Arrays.asList("id");

    @Before
    public void before() throws Throwable {
        {
            InputStream in = Launcher.class.getResourceAsStream("/retz-tls.properties");
            config = new ServerConfiguration(in);
            Database.getInstance().init(config);
            assertTrue(Database.getMigrator().isFinished());
            this.port = config.getGrpcURI().getPort();

            this.server = new RetzServer(config);
            this.server.start();
        }

        {
            InputStream in = Launcher.class.getResourceAsStream("/retz-grpc-client.properties");
            cliConfig = new ClientCLIConfig(in);
        }
    }

    @After
    public void after() {
        this.server.stop();
        this.server.blockUntilShutdown();
        Database.getInstance().stop();

        Database.getInstance().clear();
        Database.getInstance().stop();
        // This is because Spark.stop() is impelemented in asynchronous way,
        // there are no way to wait for spark.Service.initialized become false.
        // If this sleep is too short, IllegalStateException("This must be done before route mapping has begun");
        // will be thrown.
    }

    @Test
    public void ping() throws Exception {
        try (Client client = new Client(cliConfig)){
            assertTrue(client.ping());
        }
    }

    @Test
    public void list() {
        try (Client client = new Client(cliConfig)) {
            Job.JobState[] states = {
                    Job.JobState.QUEUED, Job.JobState.STARTING, Job.JobState.FINISHED,
                    Job.JobState.KILLED};
            for (Job.JobState state : states) {
                List<Job> jobs = client.listJobs(state, Optional.empty());
                assertTrue(jobs.isEmpty());
            }
        }
    }

    @Test
    public void loadApp() throws Exception {
        String[] files = {"http://example.com:234/foobar/test.tar.gz"};
        try (Client client = new Client(cliConfig)) {
            {
                Application app = new Application("foobar", Collections.emptyList(), Arrays.asList(files),
                        Optional.empty(), config.getUser().keyId(),
                        0, new MesosContainer(), true);
                client.loadApp(app);

                Optional<Application> app2 = Applications.get(app.getAppid());
                assertTrue(app2.isPresent());
                assertEquals(app2.get().getAppid(), app.getAppid());
            }
            {
                Application app = client.getApp("foobar");
                assertEquals(app.getAppid(), "foobar");
            }
        }
    }
    @Test
    public void schedule() throws Exception {
        List<Job> maybeJobs = JobQueue.findFit(BASE_ORDER_BY, new ResourceQuantity(10000, 10000, 0, 0, 0, 0));
        assertTrue(maybeJobs.isEmpty());

        try (Client client = new Client(cliConfig)){
            // Job request without app must fail
            String cmd = "Mmmmmmmmmy commmmmand1!!!!!";
            client.schedule(new Job("foobar", cmd, null, 1, 256, 32));

            maybeJobs = JobQueue.findFit(BASE_ORDER_BY, new ResourceQuantity(1000, 10000, 0, 0, 0, 0));
            assertTrue(maybeJobs.isEmpty());

            Optional<Job> maybeJob = client.getJob(234567567);
            assertFalse(maybeJob.isPresent());
        }

        try(Client client = new Client(cliConfig)){
            String[] files = {"http://example.com:234/foobar/test.tar.gz"};
            Application app = new Application("foobar", Collections.emptyList(), Arrays.asList(files),
                    Optional.empty(), config.getUser().keyId(),
                    0, new MesosContainer(), true);
            client.loadApp(app);

            Optional<Application> app2 = Applications.get("foobar");
            assertTrue(app2.isPresent());
            assertEquals(app2.get().getAppid(), "foobar");

            Application app3 = client.getApp("foobar");

            assertEquals("foobar", app3.getAppid());
            assertEquals(1, app3.getFiles().size());
        }
        maybeJobs = JobQueue.findFit(BASE_ORDER_BY, new ResourceQuantity(10000, 10000, 0, 0, 0, 0));
        assertTrue(maybeJobs.isEmpty());

        try(Client client = new Client(cliConfig)){
            // You know, these spaces are to be normalized
            String cmd = "Mmmmmmmmmy commmmmand1!!!!!";
            Optional<Job> maybeJob = client.schedule(new Job("foobar", cmd, null, 1, 200, 32));

            assertTrue(maybeJob.isPresent());
            int id = maybeJob.get().id();
            //assertThat(sres.job.id(), is(greaterThanOrEqualTo(0)));
            //System.err.println(sres.job.scheduled());

            maybeJobs = client.listJobs(Job.JobState.QUEUED, Optional.empty());
            assertEquals(1, maybeJobs.size());

            maybeJob = client.getJob(id);
            assertEquals(cmd, maybeJob.get().cmd());

            maybeJobs = JobQueue.findFit(BASE_ORDER_BY, new ResourceQuantity(10000, 10000, 0, 0, 0, 0));
            assertFalse(maybeJobs.isEmpty());
            assertEquals(cmd, maybeJobs.get(0).cmd());
            assertEquals("foobar", maybeJobs.get(0).appid());

            List<DirEntry> entries = client.listFiles(id, "$MESOS_SANDBOX");
            assertTrue(entries.isEmpty());

            OutputStream out = new ByteArrayOutputStream();
            long len = client.getFile(id, "stdout", out);
            assertTrue(len == 0);
            assertTrue(out.toString().isEmpty());
        }

        try(Client client = new Client(cliConfig)){
            Application app = client.getApp("no such app");
            assertNull(app);
        }
    }
/**

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
 **/
}
