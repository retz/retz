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
package io.github.retz.db;

import io.github.retz.cli.TimestampHelper;
import io.github.retz.protocol.data.*;
import io.github.retz.scheduler.AppJobPair;
import io.github.retz.scheduler.JobQueue;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class DatabaseTest {
    private Database db;

    @Before
    public void before() throws Exception {
        db = Database.getInstance();
        db.initOnMem("hogefoobar");

        // DEFAULT_DATABASE_URL afaik
        assertEquals("jdbc:h2:mem:hogefoobar;DB_CLOSE_DELAY=-1", db.databaseURL);
    }

    @After
    public void after() throws Exception {
        db.deleteAllProperties();
        db.stop();
    }

    @Test
    public void user() throws Exception {
        db.addUser(new User("cafebabe", "foobar", true, "test user"));

        assertFalse(db.getUser("non-pooh-bar").isPresent());
        assertTrue(db.getUser("cafebabe").isPresent());
        assertEquals("foobar", db.getUser("cafebabe").get().secret());

        {
            User u = db.createUser("test user user()");
            assertTrue(db.getUser(u.keyId()).isPresent());
            assertEquals(u.secret(), db.getUser(u.keyId()).get().secret());
        }

        for (User v : db.allUsers()) {
            System.err.println(v.keyId() + "\t" + v.secret() + "\t" + v.enabled());
        }

        // Try to detect connection leak
        for (int i = 0; i < 4096; ++i) {
            assertTrue(db.getUser("cafebabe").isPresent());
        }
        {
            User u = db.createUser("foo! R! U!");
            assertTrue(u.enabled());
            db.enableUser(u.keyId(), false);
            Optional<User> u2 = db.getUser(u.keyId());
            assertTrue(u2.isPresent());
            assertEquals(u.keyId(), u2.get().keyId());
            assertFalse(u2.get().enabled());
        }
    }

    @Test
    public void application() throws Exception {
        User u = db.createUser("test user");
        assertTrue(db.getUser(u.keyId()).isPresent());
        assertEquals(u.secret(), db.getUser(u.keyId()).get().secret());
        System.err.println("User " + u.keyId() + " created.");

        List<String> emptyList = new LinkedList<>();
        Application app = new Application("testapp", emptyList, emptyList,
                Optional.of("unix-user"), u.keyId(),
                128, new MesosContainer(), true);
        boolean res = db.addApplication(app);

        assertTrue(res);
        Application app2 = db.getApplication(app.getAppid()).get();

        assertEquals(app.getAppid(), app2.getAppid());
        assertEquals(app.getOwner(), app2.getOwner());
        assertEquals(app.toString(), app2.toString());

        System.err.println(app2.toString());

        Application app3 = new Application("testapppo", emptyList, emptyList,
                Optional.of("unix-user"), "charlie",
                128, new MesosContainer(), true);
        assertFalse(db.addApplication(app3));

        db.safeDeleteApplication(app.getAppid());

        assertFalse(db.getApplication(app.getAppid()).isPresent());
    }

    @Test
    public void job() throws Exception {
        db.validate();

        User u = db.createUser("test user");
        assertTrue(db.getUser(u.keyId()).isPresent());
        assertEquals(u.secret(), db.getUser(u.keyId()).get().secret());
        System.err.println("User " + u.keyId() + " created.");

        Application a = new Application("someapp", Arrays.asList(), Arrays.asList(),
                Optional.empty(), u.keyId(), 0, new MesosContainer(), true);
        db.addApplication(a);

        int id = -1;
        {
            Job job = new Job(a.getAppid(), "uname -a", new Properties(), 1, 32, 32);
            job.schedule(JobQueue.issueJobId(), TimestampHelper.now());

            db.safeAddJob(job);

            assertThat(db.getLatestJobId(), Matchers.greaterThanOrEqualTo(1));
            Job job2 = db.getJob(job.id()).get();

            assertEquals(job.id(), job2.id());
            assertEquals(job.cmd(), job2.cmd());
            assertEquals(job.toString(), job2.toString());
            id = job.id();

            assertTrue(db.getJob(job.id()).isPresent());

            assertEquals(1, db.countQueued());
            assertEquals(0, db.countRunning());
        }
        //System.err.println(job2.toString());
        {
            String taskId = "app-taskid-1";
            Optional<Job> maybeJob = db.getJob(id);
            assertTrue(maybeJob.isPresent());
            for (Job j : db.getAllJobs(u.keyId())) {
                System.err.println(j.id() + j.taskId() + j.state());
            }
            db.setJobStarting(id, Optional.empty(), taskId);

            for (Job j : db.getAllJobs(u.keyId())) {
                System.out.println(j.pp());
            }
            assertTrue(db.getJobFromTaskId(taskId).isPresent());
        }
        {
            Optional<AppJobPair> maybePair = db.getAppJob(id);
            assertTrue(maybePair.isPresent());
            AppJobPair pair = maybePair.get();
            assertEquals(id, pair.job().id());
            assertEquals(a.getAppid(), pair.application().getAppid());
        }

        assertEquals(0, db.countQueued());
        assertEquals(1, db.countRunning());

        {
            List<Job> jobs = db.listJobs(u.keyId(), Job.JobState.QUEUED, Optional.empty());
            assertEquals(0, jobs.size());
        }
        {
            List<Job> jobs = db.listJobs(u.keyId(), Job.JobState.STARTING, Optional.empty());
            assertEquals(1, jobs.size());
        }
        {
            List<Job> jobs = db.listJobs(u.keyId(), Job.JobState.STARTED, Optional.empty());
            assertEquals(0, jobs.size());
        }
        {
            List<Job> jobs = db.listJobs(u.keyId(), Job.JobState.FINISHED, Optional.empty());
            assertEquals(0, jobs.size());
        }
    }

    @Test
    public void multiUsers() throws Exception {
        List<User> users = new LinkedList<>();
        for (int i = 0; i < 10; ++i) {
            users.add(db.createUser("test user " + i));
        }
        assertEquals(10, users.size());

        List<Application> apps = new LinkedList<>();
        List<String> emptyList = new LinkedList<>();
        for (User u : users) {
            Application app = new Application(u.keyId(), emptyList, emptyList,
                    Optional.of("unix-user"), u.keyId(),
                    0, new MesosContainer(), true);
            db.addApplication(app);
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
                    0,
                    app.getAppid(),
                    u.keyId(),
                    new HashSet<>(),
                    new ResourceQuantity(1, 32, 0, 0, 0, 0),
                    Optional.empty(),
                    null,
                    Job.JobState.QUEUED);
            job.addTags(Arrays.asList("tag1", "tag2"));
            db.safeAddJob(job);

        }
        assertEquals(10, apps.size());

        assertThat(db.getAllApplications().size(), greaterThanOrEqualTo(10));

        for (Application a : db.getAllApplications()) {
            System.err.println(a.getAppid() + " " + a.getOwner());
        }
        for (int i = 0; i < 10; ++i) {
            System.err.println("user>> " + users.get(i).keyId());
            List<Application> ones = db.getAllApplications(users.get(i).keyId());
            assertEquals(1, ones.size());
            assertEquals(users.get(i).keyId(), ones.get(0).getAppid());
            assertEquals(users.get(i).keyId(), ones.get(0).getOwner());
            List<Job> jobs = db.getAllJobs(users.get(i).keyId());
            assertEquals(1, jobs.size());
            assertEquals(users.get(i).keyId(), jobs.get(0).appid());
            assertEquals(users.get(i).keyId(), jobs.get(0).name());
        }

        db.deleteAllJob(Integer.MAX_VALUE);
    }

    @Test
    public void testProps() {
        String frameworkId = "foorbartest....";
        assertFalse(db.getFrameworkId().isPresent());
        assertTrue(db.setFrameworkId(frameworkId));
        assertThat(db.getFrameworkId().get(), is(frameworkId));
    }

    @Test
    public void gc() {
        db.deleteOldJobs(1024);
    }
}
