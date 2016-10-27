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
import io.github.retz.db.Database;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.MesosContainer;
import org.apache.mesos.Protos;
import org.h2.store.Data;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.sql.Time;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class PlannerTest {
    private Planner planner;
    private Protos.FrameworkID fid;

    private static final String ANON_APPID = "anon";


    @Before
    public void before() throws Exception {
        planner = (Planner)new NaivePlanner();
        fid = Protos.FrameworkID.newBuilder().setValue("dummy-frameworkid-qwerty").build();
        InputStream in = Launcher.class.getResourceAsStream("/retz.properties");
        Launcher.Configuration conf = new Launcher.Configuration(new FileConfiguration(in));
        Database.getInstance().init(conf.getFileConfig());

         Application anon =
                new Application(ANON_APPID, Arrays.asList(), Arrays.asList(), Arrays.asList(),
                        Optional.empty(), Optional.empty(), conf.getFileConfig().getUser().keyId(),
                        new MesosContainer(), true);
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
            List<Protos.Offer> offers = new LinkedList<>();
            List<AppJobPair> jobs = new LinkedList<>();
            for (int i = 0; i < 8; ++i) {
                String uuid = UUID.randomUUID().toString();
                offers.add(RetzSchedulerTest.buildOffer(fid, uuid, 16, 512));
            }
            Plan p = planner.plan(offers, jobs, 0);

            assertEquals(0, p.getOperations().size());
            assertEquals(0, p.getToBeAccepted().size());
            assertEquals(0, p.getToCancel().size());
            assertEquals(0, p.getToBeLaunched().size());
            assertEquals(8, p.getToDecline().size());
            assertEquals(0, p.getToStock().size());
        }
        {
            List<Protos.Offer> offers = new LinkedList<>();
            List<AppJobPair> jobs = new LinkedList<>();
            for (int i = 0; i < 8; ++i) {
                String uuid = UUID.randomUUID().toString();
                offers.add(RetzSchedulerTest.buildOffer(fid, uuid, 16, 512));
            }
            Plan p = planner.plan(offers, jobs, 3);

            assertEquals(0, p.getOperations().size());
            assertEquals(0, p.getToBeAccepted().size());
            assertEquals(0, p.getToCancel().size());
            assertEquals(0, p.getToBeLaunched().size());
            assertEquals(5, p.getToDecline().size());
            assertEquals(3, p.getToStock().size());
        }
    }

    @Test
    public void twoJobs() {
        Optional<Application> app = Applications.get(ANON_APPID);

        List<Protos.Offer> offers = new LinkedList<>();
        List<AppJobPair> jobs = new LinkedList<>();
        for (int i = 0; i < 2; ++i) {
            String uuid = UUID.randomUUID().toString();
            offers.add(RetzSchedulerTest.buildOffer(fid, uuid, 16, 512));
            Job job =  new Job(ANON_APPID, "cmd", new Properties(), 16, 512);
            job.schedule(i, TimestampHelper.now());
            jobs.add(new AppJobPair(app,job));
        }
        Plan p = planner.plan(offers, jobs, 0);

        assertEquals(2, p.getOperations().size());
        System.out.println(p.getOperations().get(0).getLaunch().getTaskInfosList().get(0).getTaskId().getValue());
        assertEquals("cmd", p.getOperations().get(0).getLaunch().getTaskInfosList().get(0).getCommand().getValue());
        assertEquals(2, p.getToBeAccepted().size());
        assertEquals(0, p.getToCancel().size());
        assertEquals(2, p.getToBeLaunched().size());
        for(int i = 0; i < 2; ++i) {
            assertEquals(i, p.getToBeLaunched().get(i).id());
            assertEquals("cmd", p.getToBeLaunched().get(i).cmd());
            assertEquals(Job.JobState.STARTING, p.getToBeLaunched().get(i).state());
        }
        assertEquals(0, p.getToDecline().size());
        assertEquals(0, p.getToStock().size());
    }

    @Test
    public void eightJobs() {
        Optional<Application> app = Applications.get(ANON_APPID);

        {
            List<Protos.Offer> offers = new LinkedList<>();
            List<AppJobPair> jobs = new LinkedList<>();
            for (int i = 0; i < 2; ++i) {
                String uuid = UUID.randomUUID().toString();
                offers.add(RetzSchedulerTest.buildOffer(fid, uuid, 16, 512));
            }
            for (int i = 0; i < 8; ++i) {
                Job job = new Job("boom", "cmd", new Properties(), 4, 128);
                job.schedule(i, TimestampHelper.now());
                jobs.add(new AppJobPair(app, job));

            }
            Plan p = planner.plan(offers, jobs, 0);

            assertEquals(8, p.getOperations().size());
            System.out.println(p.getOperations().get(0).getLaunch().getTaskInfosList().get(0).getTaskId().getValue());
            assertEquals("cmd", p.getOperations().get(0).getLaunch().getTaskInfosList().get(0).getCommand().getValue());
            assertEquals(2, p.getToBeAccepted().size());
            assertEquals(0, p.getToCancel().size());
            assertEquals(8, p.getToBeLaunched().size());
            for (int i = 0; i < 8; ++i) {
                assertEquals(i, p.getToBeLaunched().get(i).id());
                assertEquals("cmd", p.getToBeLaunched().get(i).cmd());
                assertEquals(Job.JobState.STARTING, p.getToBeLaunched().get(i).state());
            }
            assertEquals(0, p.getToDecline().size());
            assertEquals(0, p.getToStock().size());
        }
        {
            List<Protos.Offer> offers = new LinkedList<>();
            List<AppJobPair> jobs = new LinkedList<>();
            for (int i = 0; i < 4; ++i) {
                String uuid = UUID.randomUUID().toString();
                offers.add(RetzSchedulerTest.buildOffer(fid, uuid, 16, 512));
            }
            for (int i = 0; i < 8; ++i) {
                Job job = new Job("boom", "cmd", new Properties(), 4, 128);
                job.schedule(i, TimestampHelper.now());
                jobs.add(new AppJobPair(app, job));

            }
            Plan p = planner.plan(offers, jobs, 1);

            assertEquals(8, p.getOperations().size());
            System.out.println(p.getOperations().get(0).getLaunch().getTaskInfosList().get(0).getTaskId().getValue());
            assertEquals("cmd", p.getOperations().get(0).getLaunch().getTaskInfosList().get(0).getCommand().getValue());
            assertEquals(2, p.getToBeAccepted().size());
            assertEquals(0, p.getToCancel().size());
            assertEquals(8, p.getToBeLaunched().size());
            for (int i = 0; i < 8; ++i) {
                assertEquals(i, p.getToBeLaunched().get(i).id());
                assertEquals("cmd", p.getToBeLaunched().get(i).cmd());
                assertEquals(Job.JobState.STARTING, p.getToBeLaunched().get(i).state());
            }
            assertEquals(1, p.getToDecline().size());
            assertEquals(1, p.getToStock().size());
        }
    }


}
