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
import io.github.retz.planner.builtin.FIFOPlanner;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.MesosContainer;
import io.github.retz.scheduler.RetzSchedulerTest;
import org.apache.mesos.Protos;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class PlannerRegressionTest {
    private static final String APPID = "app-name";
    private Protos.FrameworkID fid;
    private Planner planner;

    @Before
    public void before() throws Throwable {
        fid = Protos.FrameworkID.newBuilder().setValue("dummy-frameworkid-qwerty").build();
        planner = new ExtensiblePlanner(new FIFOPlanner(), new Properties());
    }

    @Test(expected = AssertionError.class)
    public void duplicateOfferIds() throws Exception {
        Application app = new Application(APPID, Arrays.asList(), Arrays.asList(),
                Optional.of("unixUser"), "deadbeef", 3, new MesosContainer(), true);
        Random rand = new Random();
        {
            List<Protos.Offer> offers = new ArrayList<>();
            List<AppJobPair> jobs = new ArrayList<>();
            for (int i = 0; i < 10; ++i) {
                Job job = new Job(APPID, "cmd", new Properties(), 1, 32, 32, 1, 0);
                job.schedule(i, TimestampHelper.now());
                jobs.add(new AppJobPair(Optional.of(app), job));

            }
            for (int i = 0; i < 11; ++i) {
                String uuid = UUID.randomUUID().toString();
                int cpus = rand.nextInt(65536);
                int mem = rand.nextInt(65536);
                System.err.println(cpus + " " + mem);
                // Duplicate offer IDs should lead to assertion error in ExtensiblePlanner.plan
                offers.add(RetzSchedulerTest.buildOffer(fid.getValue(), "offer" + Integer.toString(i), uuid, cpus, mem));
                offers.add(RetzSchedulerTest.buildOffer(fid.getValue(), "offer" + Integer.toString(i), uuid, cpus, mem));
            }
            Plan plan = planner.plan(offers, jobs, 1, "nobody");

            int totalJobsToLaunch = plan.getOfferAcceptors().stream().mapToInt(offerAcceptor -> offerAcceptor.getJobs().size()).sum();

            // TODO: use hamcrest
            assertEquals(plan.getOfferAcceptors().size() + plan.getToStock().size(), offers.size());
            assertTrue(1 >= plan.getToStock().size());
            assertEquals(totalJobsToLaunch + plan.getToKeep().size(), jobs.size());
        }
    }
}
