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
package io.github.retz.planner;

import io.github.retz.cli.TimestampHelper;
import io.github.retz.db.Database;
import io.github.retz.planner.AppJobPair;
import io.github.retz.planner.Plan;
import io.github.retz.planner.Planner;
import io.github.retz.planner.PlannerFactory;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.MesosContainer;
import io.github.retz.scheduler.Applications;
import io.github.retz.scheduler.Launcher;
import io.github.retz.scheduler.RetzSchedulerTest;
import io.github.retz.scheduler.ServerConfiguration;
import org.apache.mesos.Protos;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.InputStream;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Ignore
public class PlannerTest {
    protected Planner planner;
    private Protos.FrameworkID fid;

    private static final String ANON_APPID = "anon";

    String makePlannerName() throws Exception {
        throw new RuntimeException("This class shouldn't be tested");
    }

    @Before
    public void before() throws Throwable {
        //planner = (Planner)new FIFOPlanner();
        fid = Protos.FrameworkID.newBuilder().setValue("dummy-frameworkid-qwerty").build();
        InputStream in = Launcher.class.getResourceAsStream("/retz.properties");
        Launcher.Configuration conf = new Launcher.Configuration(new ServerConfiguration(in));
        Database.getInstance().init(conf.getServerConfig());

        planner = PlannerFactory.create(makePlannerName(), conf.getServerConfig());
         Application anon =
                new Application(ANON_APPID, Collections.emptyList(), Collections.emptyList(),
                        Optional.empty(), conf.getServerConfig().getUser().keyId(),
                        0, new MesosContainer(), true);
        Applications.load(anon);
    }
    @After
    public void after() {
        Database.getInstance().clear();
        Database.getInstance().stop();
    }
    @Test
    public void noJobs() {
        {
            List<Protos.Offer> offers = new ArrayList<>();
            List<AppJobPair> jobs = new ArrayList<>();
            for (int i = 0; i < 8; ++i) {
                String uuid = UUID.randomUUID().toString();
                offers.add(RetzSchedulerTest.buildOffer(fid, i, uuid, 16, 512));
            }
            Plan p = planner.plan(offers, jobs, 0, "nobody");

            assertEquals(8, p.getOfferAcceptors().size());
            assertEquals(0, p.getToKeep().size());
            assertEquals(0, p.getToStock().size());
        }
        {
            List<Protos.Offer> offers = new ArrayList<>();
            List<AppJobPair> jobs = new ArrayList<>();
            for (int i = 0; i < 8; ++i) {
                String uuid = UUID.randomUUID().toString();
                offers.add(RetzSchedulerTest.buildOffer(fid, i, uuid, 16, 512));
            }
            Plan p = planner.plan(offers, jobs, 3, "nobody");

            assertEquals(5, p.getOfferAcceptors().size());
            assertEquals(0, p.getToKeep().size());
            assertEquals(3, p.getToStock().size());
        }
    }

    @Test
    public void twoJobs() {
        Optional<Application> app = Applications.get(ANON_APPID);

        List<Protos.Offer> offers = new ArrayList<>();
        List<AppJobPair> jobs = new ArrayList<>();
        for (int i = 0; i < 2; ++i) {
            String uuid = UUID.randomUUID().toString();
            offers.add(RetzSchedulerTest.buildOffer(fid, i, uuid, 16, 512));
            Job job =  new Job(ANON_APPID, "cmd", new Properties(), 16, 512, 256);
            job.schedule(i, TimestampHelper.now());
            jobs.add(new AppJobPair(app,job));
        }
        Plan p = planner.plan(offers, jobs, 0, "nobody");

        assertEquals(2, p.getOfferAcceptors().size());
        assertEquals(0, p.getToKeep().size());
        assertEquals(0, p.getToStock().size());
        for(int i = 0; i < 2; ++i) {
            System.err.println(i);
            assertTrue(!p.getOfferAcceptors().get(i).getJobs().isEmpty());
            Job job = p.getOfferAcceptors().get(i).getJobs().get(0);
            assertEquals(i, job.id());
            assertEquals("cmd", job.cmd());
            assertEquals(Job.JobState.STARTING, job.state());
        }
    }

    @Test
    public void eightJobs() {
        Optional<Application> app = Applications.get(ANON_APPID);
        assertTrue(app.isPresent());

        {
            List<Protos.Offer> offers = new ArrayList<>();
            List<AppJobPair> jobs = new ArrayList<>();
            for (int i = 0; i < 2; ++i) {
                String uuid = UUID.randomUUID().toString();
                offers.add(RetzSchedulerTest.buildOffer(fid, i, uuid, 16, 512));
            }
            for (int i = 0; i < 8; ++i) {
                Job job = new Job(ANON_APPID, "cmd", new Properties(), 4, 128, 0);
                job.schedule(i, TimestampHelper.now());
                jobs.add(new AppJobPair(app, job));

            }
            Plan p = planner.plan(offers, jobs, 0, "nobody");

            assertEquals(2, p.getOfferAcceptors().size());
            assertEquals(0, p.getToKeep().size());
            assertEquals(0, p.getToStock().size());
            for(int i = 0; i < 8; ++i) {
                assertTrue(! p.getOfferAcceptors().get(i / 4).getJobs().isEmpty());
                Job job = p.getOfferAcceptors().get(i / 4).getJobs().get(i % 4);
                assertEquals(i, job.id());
                assertEquals("cmd", job.cmd());
                assertEquals(Job.JobState.STARTING, job.state());
            }
        }
        {
            List<Protos.Offer> offers = new ArrayList<>();
            List<AppJobPair> jobs = new ArrayList<>();
            for (int i = 0; i < 4; ++i) {
                String uuid = UUID.randomUUID().toString();
                offers.add(RetzSchedulerTest.buildOffer(fid, i, uuid, 16, 512));
            }
            for (int i = 0; i < 8; ++i) {
                Job job = new Job(ANON_APPID, "cmd", new Properties(), 4, 128, 24);
                job.schedule(i, TimestampHelper.now());
                jobs.add(new AppJobPair(app, job));

            }
            Plan p = planner.plan(offers, jobs, 1, "nobody");

            assertEquals(3, p.getOfferAcceptors().size());
            assertEquals(0, p.getToKeep().size());
            assertEquals(1, p.getToStock().size());

            assertEquals(1, p.getOfferAcceptors().stream().filter(acceptor -> acceptor.getJobs().isEmpty()).count());
            for(int i = 0; i < 8; ++i) {
                assertTrue(! p.getOfferAcceptors().get(i / 4).getJobs().isEmpty());
                Job job = p.getOfferAcceptors().get(i / 4).getJobs().get(i % 4);
                System.err.println(i);
                assertEquals(i, job.id());
                assertEquals("cmd", job.cmd());
                assertEquals(Job.JobState.STARTING, job.state());
            }
        }
    }


}
