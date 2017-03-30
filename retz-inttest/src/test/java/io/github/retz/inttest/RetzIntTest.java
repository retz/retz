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
package io.github.retz.inttest;

import io.github.retz.cli.TimestampHelper;
import io.github.retz.protocol.*;
import io.github.retz.protocol.data.*;
import io.github.retz.protocol.exception.JobNotFoundException;
import io.github.retz.web.Client;
import io.github.retz.web.ClientHelper;
import org.apache.commons.io.FilenameUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@Ignore
public class RetzIntTest extends IntTestBase {

    @Test
    public void listAppTest() throws Exception {
        URI uri = new URI("http://" + RETZ_HOST + ":" + RETZ_PORT);
        Client client = Client.newBuilder(uri)
                .setAuthenticator(config.getAuthenticator())
                .build();
        Response res = client.listApp();
        System.out.println(res.status());
        ListAppResponse response = (ListAppResponse) client.listApp();
        assertThat(response.status(), is("ok"));
        assertThat(response.applicationList().size(), greaterThanOrEqualTo(0));
        client.close();
    }

    public <E> void assertIncludes(List<E> list, E element) {
        assertThat(list, hasItem(element));
    }

    @Test
    public void runAppTest() throws Exception {
        URI uri = new URI("http://" + RETZ_HOST + ":" + RETZ_PORT);
        Client client = Client.newBuilder(uri)
                .setAuthenticator(config.getAuthenticator())
                .build();
        Application echoApp = new Application("echo-app", Arrays.asList(), Arrays.asList("file:///spawn_retz_server.sh"),
                Optional.empty(), "deadbeef", 0, new MesosContainer(), true);
        LoadAppResponse loadRes =
                (LoadAppResponse) client.load(echoApp);
        assertThat(loadRes.status(), is("ok"));

        ListAppResponse listRes = (ListAppResponse) client.listApp();
        assertThat(listRes.status(), is("ok"));
        List<String> appNameList = listRes.applicationList().stream().map(app -> app.getAppid()).collect(Collectors.toList());
        assertIncludes(appNameList, "echo-app");

        {
            String echoText = "hoge from echo-app via Retz";
            Job job = new Job("echo-app", "echo " + echoText, new Properties(), 2, 256, 32, 0, 1);
            Job runRes = client.run(job);
            assertThat(runRes.result(), is(RES_OK));
            assertThat(runRes.state(), is(Job.JobState.FINISHED));

            String toDir = "build/log/";
            // These downloaded files are not inspected now, useful for debugging test cases, maybe

            ClientHelper.getWholeFile(client, runRes.id(), "stdout", toDir);
            ClientHelper.getWholeFile(client, runRes.id(), "stderr", toDir);

            String actualText = catStdout(client, runRes);
            List<String> lines = Arrays.asList(actualText.split("\n"));
            assertThat(lines, hasItem(echoText));
            assertThat(lines, hasItem("Received SUBSCRIBED event"));
            assertThat(lines, hasItem("Received LAUNCH event"));
        }

        assertThat(ClientHelper.finished(client).size(), greaterThan(0));
        assertThat(ClientHelper.running(client).size(), is(0));
        assertThat(ClientHelper.queue(client).size(), is(0));

        {
            String echoText = "PPAP";
            Job job = new Job("echo-app", "mkdir -p a/b/c/d; echo " + echoText + " > a/b/c/e", new Properties(), 1, 32, 32);
            Job runRes = client.run(job);
            assertThat(runRes.result(), is(RES_OK));
            assertThat(runRes.state(), is(Job.JobState.FINISHED));

            Response response = client.getFile(runRes.id(), "a/b/c/e", 0, 1024);
            System.err.println(response.status());
            GetFileResponse getFileResponse = (GetFileResponse) response;
            assertEquals(echoText + "\n", getFileResponse.file().get().data());

            ListFilesResponse listFilesResponse = (ListFilesResponse) client.listFiles(runRes.id(), "a/b/c");
            assertEquals(2, listFilesResponse.entries().size());
            List<String> files = listFilesResponse.entries().stream().map(e -> FilenameUtils.getName(e.path())).collect(Collectors.toList());
            assertEquals("d", files.get(0));
            assertEquals("e", files.get(1));
        }
        UnloadAppResponse unloadRes = (UnloadAppResponse) client.unload("echo-app");
        assertThat(unloadRes.status(), is("ok"));

        client.close();
    }

    @Test
    public void killTest() throws Exception {
        URI uri = new URI("http://" + RETZ_HOST + ":" + RETZ_PORT);
        Client client = Client.newBuilder(uri)
                .setAuthenticator(config.getAuthenticator())
                .build();

        Application echoApp = new Application("echo-app", Arrays.asList(), Arrays.asList(),
                Optional.empty(), "deadbeef", 0, new MesosContainer(), true);
        Response response = client.load(echoApp);
        System.err.println(response.status());
        LoadAppResponse loadRes = (LoadAppResponse) response;
        assertThat(loadRes.status(), is("ok"));

        ListAppResponse listRes = (ListAppResponse) client.listApp();
        assertThat(listRes.status(), is("ok"));
        List<String> appNameList = listRes.applicationList().stream().map(app -> app.getAppid()).collect(Collectors.toList());
        assertIncludes(appNameList, "echo-app");
        Job job = new Job("echo-app", "sleep 60", new Properties(), 1, 32, 32, 0, 2);
        response = client.schedule(job);
        assertThat(response, instanceOf(ScheduleResponse.class));
        ScheduleResponse scheduleResponse = (ScheduleResponse) response;
        int id = scheduleResponse.job().id();
        System.err.println(id);

        {
            response = client.getJob(id);
            assertThat(response, instanceOf(GetJobResponse.class));
            GetJobResponse getJobResponse = (GetJobResponse) response;
            //depending on the timing, like if the test is relatively slow, this could be JobState.STARTED
            assertTrue(getJobResponse.job().isPresent());
            assertThat(getJobResponse.job().get().state(), isOneOf(Job.JobState.QUEUED, Job.JobState.STARTING));
        }
        {
            Thread.sleep(5000); // Make sure a job is sent to Mesos
            response = client.kill(id);
            System.err.println(response.status());
            assertThat(response, instanceOf(KillResponse.class));
        }
        {
            Thread.sleep(5000); // Make sure kill sent to Mesos
            response = client.getJob(id);
            assertThat(response, instanceOf(GetJobResponse.class));
            GetJobResponse getJobResponse = (GetJobResponse) response;
            assertThat(getJobResponse.job().get().state(), is(Job.JobState.KILLED));

            String taskId = getJobResponse.job().get().taskId();
            assertNotNull(taskId);
            URI mesos = new URI("http://" + RETZ_HOST + ":" + MESOS_PORT);
            String state = MesosFlakyClient.getTaskState(mesos, taskId);
            System.err.println(state);
            assertEquals("Chacking whether a job kill reached Mesos", state, "TASK_KILLED");
        }

        UnloadAppResponse unloadRes = (UnloadAppResponse) client.unload("echo-app");
        assertThat(unloadRes.status(), is("ok"));

        client.close();
    }

    @Test
    public void scheduleAppTest() throws Exception {
        URI uri = new URI("http://" + RETZ_HOST + ":" + RETZ_PORT);

        try (Client client = Client.newBuilder(uri)
                .setAuthenticator(config.getAuthenticator())
                .build()) {
            loadSimpleApp(client, "echo2");

            List<EchoJob> finishedJobs = new LinkedList<>();
            List<Integer> argvList = IntStream.rangeClosed(0, 32).boxed().collect(Collectors.toList());
            argvList.addAll(Arrays.asList(42, 63, 64, 127, 128, 151, 192, 255));
            int jobNum = argvList.size();
            List<EchoJob> echoJobs = scheduleEchoJobs(client, "echo2", "echo.sh ", argvList);

            assertThat(echoJobs.size(), is(jobNum));

            for (int i = 0; i < 16; i++) {
                List<EchoJob> toRemove = toRemove(client, echoJobs, true);

                if (!toRemove.isEmpty()) {
                    i = 0;
                }
                echoJobs.removeAll(toRemove);
                finishedJobs.addAll(toRemove);
                if (echoJobs.isEmpty()) {
                    break;
                }
                Thread.sleep(1000);


                System.err.println(TimestampHelper.now()
                        + ": Finished=" + ClientHelper.finished(client).size()
                        + ", Running=" + ClientHelper.running(client).size()
                        + ", Scheduled=" + ClientHelper.queue(client).size());
                for (Job finished : ClientHelper.finished(client)) {
                    assertThat(finished.retry(), is(0));
                }
            }
            assertThat(finishedJobs.size(), is(jobNum));

            assertThat(ClientHelper.finished(client).size(), greaterThanOrEqualTo(jobNum));
            assertThat(ClientHelper.running(client).size(), is(0));
            assertThat(ClientHelper.queue(client).size(), is(0));

            UnloadAppResponse unloadRes = (UnloadAppResponse) client.unload("echo2");
            assertThat(unloadRes.status(), is("ok"));
        }
    }

    private void loadSimpleApp(Client client, String appName) throws IOException {
        Application app = new Application(appName, Arrays.asList(),
                Arrays.asList(), Optional.empty(),
                "deadbeef", 0, new MesosContainer(), true);
        Response res = client.load(app);

        assertTrue(res.status(), res instanceof LoadAppResponse);
        LoadAppResponse loadRes = (LoadAppResponse) res;
        assertThat(loadRes.status(), is("ok"));

        ListAppResponse listRes = (ListAppResponse) client.listApp();
        assertThat(listRes.status(), is("ok"));
        List<String> appNameList = listRes.applicationList().stream().map(a -> a.getAppid()).collect(Collectors.toList());
        assertIncludes(appNameList, appName);
    }

    private List<EchoJob> scheduleEchoJobs(Client client, String appName, String cmdPrefix, List<Integer> argvList) throws IOException {
        List<EchoJob> echoJobs = new LinkedList<>();
        for (Integer i : argvList) {
            Job job = new Job(appName, cmdPrefix + i.toString(), new Properties(), 1, 32, 32, 0, 3);

            ScheduleResponse scheduleResponse = (ScheduleResponse) client.schedule(job);
            assertThat(scheduleResponse.status(), is("ok"));

            Job scheduledJob = scheduleResponse.job();
            echoJobs.add(new EchoJob(i.intValue(), scheduledJob));
        }
        return echoJobs;
    }

    private List<EchoJob> toRemove(Client client, List<EchoJob> echoJobs, boolean checkRetval) throws IOException, Exception {
        List<EchoJob> toRemove = new LinkedList<>();
        for (EchoJob echoJob : echoJobs) {
            Response response = client.getJob(echoJob.job.id());
            if (response instanceof GetJobResponse) {
                GetJobResponse getJobResponse = (GetJobResponse) client.getJob(echoJob.job.id());
                // System.err.println(echoJob.job.id() + " => " + getJobResponse.job().get().result());
                if (getJobResponse.job().isPresent()) {
                    if (getJobResponse.job().get().state() == Job.JobState.FINISHED
                            || getJobResponse.job().get().state() == Job.JobState.KILLED) {
                        toRemove.add(echoJob);

                        String actualText = catStdout(client, getJobResponse.job().get());
                        List<String> lines = Arrays.asList(actualText.split("\n"));
                        assertThat(lines, hasItem(Integer.toString(echoJob.argv)));
                        assertThat(lines, hasItem("Received SUBSCRIBED event"));
                        assertThat(lines, hasItem("Received LAUNCH event"));

                    } else if (checkRetval) {
                        assertNull("Unexpected return value for Job " + getJobResponse.job().get().result()
                                        + ", Message: " + getJobResponse.job().get().reason(),
                                getJobResponse.job().get().finished());
                    }
                }
            } else {
                ErrorResponse errorResponse = (ErrorResponse) response;
                System.err.println(echoJob.job.id() + ": " + errorResponse.status());
            }
        }
        return toRemove;
    }

    @Test
    public void scheduleAppTest2() throws Exception {
        URI uri = new URI("http://" + RETZ_HOST + ":" + RETZ_PORT);
        try (Client client = Client.newBuilder(uri)
                .setAuthenticator(config.getAuthenticator())
                .build()) {
            loadSimpleApp(client, "echo3");

            List<EchoJob> finishedJobs = new LinkedList<>();
            List<Integer> argvList = IntStream.rangeClosed(0, 32).boxed().collect(Collectors.toList());
            argvList.addAll(Arrays.asList(42, 63, 64, 127, 128, 151, 192, 255));
            int jobNum = argvList.size();
            List<EchoJob> echoJobs = scheduleEchoJobs(client, "echo3", "echo ", argvList);
            assertThat(echoJobs.size(), is(jobNum));

            for (int i = 0; i < 32; i++) {
                List<EchoJob> toRemove = toRemove(client, echoJobs, false);
                if (!toRemove.isEmpty()) {
                    i = 0;
                }
                echoJobs.removeAll(toRemove);
                finishedJobs.addAll(toRemove);
                if (echoJobs.isEmpty()) {
                    break;
                }
                Thread.sleep(1000);

                System.err.println(TimestampHelper.now()
                        + ": Finished=" + ClientHelper.finished(client).size()
                        + ", Running=" + ClientHelper.running(client).size()
                        + ", Scheduled=" + ClientHelper.queue(client).size());
                for (Job finished : ClientHelper.finished(client)) {
                    assertThat(finished.retry(), is(0));
                    assertThat(finished.state(), is(Job.JobState.FINISHED));
                    assertThat(finished.result(), is(RES_OK));
                }
            }
            assertThat(finishedJobs.size(), is(jobNum));

            assertThat(ClientHelper.finished(client).size(), greaterThanOrEqualTo(jobNum));
            assertThat(ClientHelper.running(client).size(), is(0));
            assertThat(ClientHelper.queue(client).size(), is(0));

            UnloadAppResponse unloadRes = (UnloadAppResponse) client.unload("echo3");
            assertThat(unloadRes.status(), is("ok"));
        }
    }

    @Test
    public void listFilesTest() throws Exception { //Regression test for #79 : https://github.com/retz/retz/issues/79
        URI uri = new URI("http://" + RETZ_HOST + ":" + RETZ_PORT);
        try (Client client = Client.newBuilder(uri)
                .setAuthenticator(config.getAuthenticator())
                .build()) {
            loadSimpleApp(client, "touch-many");
            {
                Job job = new Job("touch-many", "mkdir d++; touch d++/e++; touch d++/f", new Properties(), 1, 32, 32);
                job.addTags("tag1-listFilesTest");
                Job ran = client.run(job);
                Response response = client.listFiles(ran.id(), "d++");
                assertEquals("ok", response.status());
                ListFilesResponse listFilesResponse = (ListFilesResponse) response;
                assertEquals(2, listFilesResponse.entries().size());
                assertTrue(listFilesResponse.entries().get(0).path().endsWith("d++/e++"));
                assertTrue(listFilesResponse.entries().get(1).path().endsWith("d++/f"));
            }

            { //Test unicode paths
                Job job = new Job("touch-many", "mkdir d++; touch ε=ε=ε=ε=ε=ε= ◟⚫͈ω⚫̤◞", new Properties(), 1, 32, 32);
                job.addTags("tag1-listFilesTest", "multibytes");
                Job ran = client.run(job);

                Response response = client.listFiles(ran.id(), ListFilesRequest.DEFAULT_SANDBOX_PATH);
                ListFilesResponse listFilesResponse = (ListFilesResponse) response;

                for (DirEntry e : listFilesResponse.entries()) {
                    System.err.println(e.path());
                }

                assertEquals(5, listFilesResponse.entries().size());
                assertTrue(listFilesResponse.entries().get(0).path().endsWith("d++"));
                assertTrue(listFilesResponse.entries().get(1).path().endsWith("stderr"));
                assertTrue(listFilesResponse.entries().get(2).path().endsWith("stdout"));
                assertTrue(listFilesResponse.entries().get(3).path().endsWith("ε=ε=ε=ε=ε=ε="));
                assertTrue(listFilesResponse.entries().get(4).path().endsWith("◟⚫͈ω⚫̤◞"));
            }

            {
                Response response = client.list(Job.JobState.FINISHED, Optional.of("tag1-listFilesTest"));
                ListJobResponse listJobResponse = (ListJobResponse) response;
                assertEquals(2, listJobResponse.jobs().size());
                assertFalse(listJobResponse.more());
            }

            {
                Response response = client.list(Job.JobState.FINISHED, Optional.of("multibytes"));
                ListJobResponse listJobResponse = (ListJobResponse) response;
                assertEquals(1, listJobResponse.jobs().size());
                assertFalse(listJobResponse.more());
            }

            {
                Response response = client.list(Job.JobState.FINISHED, Optional.empty());
                ListJobResponse listJobResponse = (ListJobResponse) response;
                assertThat(listJobResponse.jobs().size(), greaterThanOrEqualTo(2));
            }
        }
    }

    @Test(expected = JobNotFoundException.class)
    public void jobNotFoundTest() throws Exception {
        URI uri = new URI("http://" + RETZ_HOST + ":" + RETZ_PORT);
        try (Client client = Client.newBuilder(uri)
                .setAuthenticator(config.getAuthenticator())
                .build()) {
            Response response = client.getJob(102345);
            System.err.println(response.status());
            GetJobResponse getJobResponse = (GetJobResponse) response;
            assertTrue(!getJobResponse.job().isPresent());
            ClientHelper.getWholeFile(client, 102345, "stdout", false, System.out);
        }
    }

    @Test(expected = FileNotFoundException.class)
    public void fileNotFoundTest() throws Exception {
        URI uri = new URI("http://" + RETZ_HOST + ":" + RETZ_PORT);
        try (Client client = Client.newBuilder(uri)
                .setAuthenticator(config.getAuthenticator())
                .build()) {
            {
                loadSimpleApp(client, "echo");
                Job job = new Job("echo", "echo 42", new Properties(), 1, 32, 32);
                Job ran = client.run(job);
                Response response = client.getFile(ran.id(), "non-existent-file", 0, -1);
                GetFileResponse getFileResponse = (GetFileResponse) response;
                assertTrue(getFileResponse.job().isPresent());
                assertTrue(!getFileResponse.file().isPresent());
                ClientHelper.getWholeFile(client, ran.id(), "no-such-file", false, System.out);
            }
        }
    }

    @Test
    public void tooMuchResourceRequested() throws Exception {
        URI uri = new URI("http://" + RETZ_HOST + ":" + RETZ_PORT);
        try (Client client = Client.newBuilder(uri)
                .setAuthenticator(config.getAuthenticator())
                .build()) {
            {
                loadSimpleApp(client, "echo");
                Job job = new Job("echo", "echo 42", new Properties(), 1024, 32, 32);
                Response response = client.schedule(job);
                // TODO: how do we indicate this is from WebConsole::schedule, ResourceQuantitiy#fits(job)?
                assertTrue(response instanceof ErrorResponse);
            }
        }
    }


    @Test
    public void userTest() throws Exception {
        System.err.println("Connecting to " + RETZ_HOST);
        // create-user, list-user, disable-user, enable-user, get-user
        String jar = "/build/libs/retz-admin-all.jar";
        String cfg = "/" + serverConfigFile;
        {
            String[] command = {"java", "-jar", jar, "-C", cfg, "list-user"};
            verifyCommand(command);
        }
        {
            String[] command = {"java", "-jar", jar, "-C", cfg, "create-user", "--info", "farboom"};
            verifyCommand(command);
        }
        {
            String[] command = {"java", "-jar", jar, "-C", cfg, "get-user", "-id", "deadbeef"};
            verifyCommand(command);
        }
        {
            String[] command = {"java", "-jar", jar, "-C", cfg, "disable-user", "-id", "deadbeef"};
            verifyCommand(command);
        }
        {
            String[] command = {"java", "-jar", jar, "-C", cfg, "enable-user", "-id", "deadbeef"};
            verifyCommand(command);
        }
        {
            String[] command = {"java", "-jar", jar, "-C", cfg, "get-user", "-id", "deadbeef"};
            verifyCommand(command);
        }
        {
            String[] command = {"java", "-jar", jar, "-C", cfg, "fail!"};
            verifyCommandFails(command, "ERROR");
        }
    }

    // No reason for this test to be in persistentTest, possibly could be in RetzIntTest
    @Test
    public void disableUser() throws Exception {
        User user = config.getUser();
        List<String> e = Arrays.asList();
        Application application = new Application("t", e, e, Optional.empty(),
                user.keyId(), 0, new MesosContainer(), true);
        URI uri = new URI("http://" + RETZ_HOST + ":" + RETZ_PORT);

        String jar = "/build/libs/retz-admin-all.jar";
        String cfg = "/" + serverConfigFile;

        try (Client client = Client.newBuilder(uri).setAuthenticator(config.getAuthenticator()).build()) {
            Response res;

            res = client.load(application);
            assertEquals("ok", res.status());

            res = client.schedule(new Job("t", "ls", new Properties(), 1, 32, 32));
            ScheduleResponse scheduleResponse = (ScheduleResponse) res;
            Job job1 = scheduleResponse.job();
            {
                System.err.println("Disable user " + user.keyId());
                String[] command = {"java", "-jar", jar, "-C", cfg, "disable-user", "-id", user.keyId()};
                verifyCommand(command);
            }

            res = client.getJob(job1.id());
            System.err.println(res.status());
            assertThat(res, instanceOf(ErrorResponse.class));

            res = client.load(new Application("t2", e, e, Optional.empty(),
                    user.keyId(), 0, new MesosContainer(), true));
            System.err.println(res.status());
            assertThat(res, instanceOf(ErrorResponse.class));

            res = client.schedule(new Job("t", "echo prohibited job", new Properties(), 1, 32, 32));
            System.err.println(res.status());
            assertThat(res, instanceOf(ErrorResponse.class));

            {
                String[] command = {"java", "-jar", jar, "-C", cfg, "enable-user", "-id", "deadbeef"};
                verifyCommand(command);
            }

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
    }

    protected void verifyCommand(String[] command) throws Exception {
        String result = container.system(command);
        System.err.println(String.join(" ", command) + " => " + result);
        assertFalse(result, result.contains("ERROR"));
        assertFalse(result, result.contains("Error"));
        assertFalse(result, result.contains("Exception"));
    }

    protected void verifyCommandFails(String[] command, String word) throws Exception {
        String result = container.system(command);
        System.err.println(String.join(" ", command) + " => " + result);
        assertTrue(result, result.contains(word));
    }

    private String catStdout(Client c, Job job) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ClientHelper.getWholeFile(c, job.id(), "stdout", true, out);
        return out.toString(String.valueOf(StandardCharsets.UTF_8));
    }

    static class EchoJob {
        int argv;
        Job job;

        EchoJob(int argv, Job job) {
            this.argv = argv;
            this.job = job;
        }
    }

}
