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

import io.github.retz.cli.FileConfiguration;
import io.github.retz.cli.TimestampHelper;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.MesosContainer;
import io.github.retz.protocol.data.User;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.*;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.*;

public class DatabaseTest {

    @Before
    public void before() throws Exception {
        InputStream in = Launcher.class.getResourceAsStream("/retz-tls.properties");

        FileConfiguration config = new FileConfiguration(in);
        Database.init(config);

        // DEFAULT_DATABASE_URL afaik
        //assertEquals("jdbc:h2:mem:retz-server;DB_CLOSE_DELAY=-1", config.getDatabaseURL());
    }

    @After
    public void after() throws Exception {
        Database.stop();
    }

    @Test
    public void user() throws Exception {
        Database.addUser(new User("cafebabe", "foobar", true));

        assertFalse(Database.getUser("non-pooh-bar").isPresent());
        assertTrue(Database.getUser("cafebabe").isPresent());
        assertEquals("foobar", Database.getUser("cafebabe").get().secret());

        User u = Database.createUser();
        assertTrue(Database.getUser(u.keyId()).isPresent());
        assertEquals(u.secret(), Database.getUser(u.keyId()).get().secret());

        for (User v : Database.allUsers()) {
            System.err.println(v.keyId() + "\t" + v.secret() + "\t" + v.enabled());
        }

        // Try to detect connection leak
        for (int i = 0; i < 4096; ++i) {
            assertTrue(Database.getUser("cafebabe").isPresent());
        }
    }

    @Test
    public void application() throws Exception {
        User u = Database.createUser();
        assertTrue(Database.getUser(u.keyId()).isPresent());
        assertEquals(u.secret(), Database.getUser(u.keyId()).get().secret());
        System.err.println("User " + u.keyId() + " created.");

        List<String> emptyList = new LinkedList<>();
        Application app = new Application("testapp", emptyList, emptyList, emptyList, Optional.empty(),
                Optional.of("unix-user"), u.keyId(), new MesosContainer(), true);
        boolean res = Database.addApplication(app);

        assertTrue(res);
        Application app2 = Database.getApplication(app.getAppid()).get();

        assertEquals(app.getAppid(), app2.getAppid());
        assertEquals(app.getOwner(), app2.getOwner());
        assertEquals(app.toString(), app2.toString());

        System.err.println(app2.toString());

        Application app3 = new Application("testapppo", emptyList, emptyList, emptyList, Optional.empty(),
                Optional.of("unix-user"), "charlie", new MesosContainer(), true);
        assertFalse(Database.addApplication(app3));

        Database.safeDeleteApplication(app.getAppid());

        assertFalse(Database.getApplication(app.getAppid()).isPresent());
    }

    @Test
    public void job() throws Exception {
        User u = Database.createUser();
        assertTrue(Database.getUser(u.keyId()).isPresent());
        assertEquals(u.secret(), Database.getUser(u.keyId()).get().secret());
        System.err.println("User " + u.keyId() + " created.");

        Application a = new Application("someapp", Arrays.asList(), Arrays.asList(), Arrays.asList(),
                Optional.empty(), Optional.empty(), u.keyId(), new MesosContainer(), true);
        Database.addApplication(a);

        int id = -1;
        {
            Job job = new Job(a.getAppid(), "uname -a", new Properties(), 1, 32);
            job.schedule(JobQueue.issueJobId(), TimestampHelper.now());
            Database.safeAddJob(job);

            assertEquals(1, Database.getLatestJobId());
            Job job2 = Database.getJob(job.id()).get();

            assertEquals(job.id(), job2.id());
            assertEquals(job.cmd(), job2.cmd());
            assertEquals(job.toString(), job2.toString());
            id = job.id();

            assertTrue(Database.getJob(job.id()).isPresent());
        }
        //System.err.println(job2.toString());
        {
            String taskId = "app-taskid-1";
            Optional<Job> maybeJob = Database.getJob(id);
            assertTrue(maybeJob.isPresent());
            for (Job j : Database.getAllJobs(u.keyId())) {
                System.err.println(j.id() + j.taskId() + j.state());
            }
            Database.setJobStarting(id, Optional.empty(), taskId);

            for (Job j : Database.getAllJobs(u.keyId())) {
                System.err.println(j.id() + j.taskId() + j.state());
            }
            assertTrue(Database.getJobFromTaskId(taskId).isPresent());
        }
    }

    @Test
    public void multiUsers() throws Exception {
        List<User> users = new LinkedList<>();
        for (int i = 0; i < 10; ++i) {
            users.add(Database.createUser());
        }
        assertEquals(10, users.size());

        List<Application> apps = new LinkedList<>();
        List<String> emptyList = new LinkedList<>();
        for (User u : users) {
            Application app = new Application(u.keyId(), emptyList, emptyList, emptyList, Optional.empty(),
                    Optional.of("unix-user"), u.keyId(), new MesosContainer(), true);
            Database.addApplication(app);
            apps.add(app);

            //    public Job(String appName, String cmd, Properties props, int cpu, int memMB) {

            Job job = new Job("uname -a",
                    TimestampHelper.now(),
                    null,
                    null,
                    new Properties(),
                    -1,
                    JobQueue.issueJobId(),
                    "my-url",
                    null,
                    5,
                    app.getAppid(),
                    u.keyId(),
                    1,
                    32,
                    0,
                    null,
                    false,
                    Job.JobState.QUEUED);
            Database.safeAddJob(job);
        }
        assertEquals(10, apps.size());

        assertThat(Database.getAllApplications().size(), greaterThanOrEqualTo(10));

        for (Application a : Database.getAllApplications()) {
            System.err.println(a.getAppid() + " " + a.getOwner());
        }
        for (int i = 0; i < 10; ++i) {
            System.err.println("user>> " + users.get(i).keyId());
            List<Application> ones = Database.getAllApplications(users.get(i).keyId());
            assertEquals(1, ones.size());
            assertEquals(users.get(i).keyId(), ones.get(0).getAppid());
            assertEquals(users.get(i).keyId(), ones.get(0).getOwner());
            List<Job> jobs = Database.getAllJobs(users.get(i).keyId());
            assertEquals(1, jobs.size());
            assertEquals(users.get(i).keyId(), jobs.get(0).appid());
            assertEquals(users.get(i).keyId(), jobs.get(0).name());
        }

        Database.deleteAllJob(Integer.MAX_VALUE);
    }
}
