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

import io.github.retz.cli.TimestampHelper;
import io.github.retz.mesos.Resource;
import io.github.retz.mesos.ResourceConstructor;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class NaivePlanner implements Planner {
    private static final Logger LOG = LoggerFactory.getLogger(NaivePlanner.class);

    // TODO: very naive packing, from left to right, no searching
    static void pack(List<Protos.Offer> offers, List<AppJobPair> appJobs, // Inputs
                     List<Protos.Offer.Operation> ops, List<Job> launch, // Outputs below
                     List<Job> spill,
                     List<Protos.Offer> accept,
                     List<Protos.Offer> remain) {
        for (Protos.Offer offer : offers) {
            Resource assigned = new Resource(0, 0, 0);
            Resource resource = ResourceConstructor.decode(offer.getResourcesList());

            while (!appJobs.isEmpty() &&
                    assigned.cpu() <= resource.cpu() &&
                    assigned.memMB() <= resource.memMB() &&
                    assigned.gpu() <= resource.gpu()) {
                AppJobPair appJob = appJobs.get(0);
                Job job = appJob.job();

                if (assigned.cpu() + job.cpu() <= resource.cpu() &&
                        assigned.memMB() + job.memMB() <= resource.memMB() &&
                        assigned.gpu() + job.gpu() <= resource.gpu()) {

                    String id = Integer.toString(job.id());
                    // Not using simple CommandExecutor to keep the executor lifecycle with its assets
                    // (esp ASAKUSA_HOME env)
                    Resource assign = new Resource(job.cpu(), job.memMB(), 0, 0, job.gpu());
                    TaskBuilder tb = new TaskBuilder()
                            .setResource(assign, offer.getSlaveId())
                            .setName("retz-" + appJob.application().getAppid() + "-name-" + job.name())
                            .setTaskId("retz-" + appJob.application().getAppid() + "-id-" + id)
                            .setCommand(job, appJob.application());
                    assigned.merge(assign);

                    Protos.TaskInfo task = tb.build();

                    Protos.Offer.Operation.Launch l = Protos.Offer.Operation.Launch.newBuilder()
                            .addTaskInfos(Protos.TaskInfo.newBuilder(task))
                            .build();

                    Protos.TaskID taskId = task.getTaskId();

                    ops.add(Protos.Offer.Operation.newBuilder()
                            .setType(Protos.Offer.Operation.Type.LAUNCH)
                            .setLaunch(l).build());
                    job.starting(taskId.getValue(), Optional.empty(), TimestampHelper.now());
                    launch.add(job);

                    LOG.info("Job {}(task {}) is to be ran as '{}' at Slave {}",
                            job.id(), taskId.getValue(), job.cmd(), offer.getSlaveId().getValue());
                    appJobs.remove(0);
                } else {
                    break;
                }
            }

            if (assigned.cpu() > 0) {
                // Some used;
                accept.add(offer);
            } else {
                remain.add(offer);
            }
        }
    }

    private static boolean resourceSufficient(Resource resource, List<AppJobPair> jobs) {
        Optional<Resource> needs = jobs.stream().map(appjob -> new Resource(appjob.job().cpu(), appjob.job().memMB(), 0, 0, appjob.job().gpu()))
                .reduce((lhs, rhs) -> {
                    lhs.merge(rhs);
                    return lhs;
                });
        return needs.isPresent() &&
                needs.get().cpu() <= resource.cpu() &&
                needs.get().memMB() <= resource.memMB() &&
                needs.get().gpu() <= resource.gpu();
    }

    @Override
    public List<AppJobPair> filter(List<Job> jobs, List<Job> cancel, boolean useGPU) {
        // TODO: better splitter
        // Not using GPU or (using GPU and GPU enabled)
        List<Job> run = jobs.stream().filter(job -> job.gpu() == 0 || useGPU).collect(Collectors.toList());
        cancel.addAll(jobs.stream()
                .filter(job -> job.gpu() > 0 && !useGPU) // Using GPU && GPU not enabled
                .map(job -> {
                    String reason = String.format("Job (%d@%s) requires %d GPUs while this Retz Scheduler is not capable of using GPU resources. Try setting retz.gpu=true at retz.properties.",
                            job.id(), job.appid(), job.gpu());
                    job.killed(TimestampHelper.now(), Optional.empty(), reason); // Database to be updated later, after plan accepted
                    return job;
                }).collect(Collectors.toList()));

        List<AppJobPair> appJobs = run.stream().map(job -> {
            Optional<Application> app = Applications.get(job.appid());
            return new AppJobPair(app, job);
        }).collect(Collectors.toList());

        cancel.addAll(appJobs.stream()
                .filter(appJobPair -> !appJobPair.hasApplication())
                .map(appJobPair -> {
                    Job job = appJobPair.job();
                    String reason = String.format("Applicaion '%s' of Job (%d@%s) does not exist in DB", job.appid(), job.id());
                    job.killed(TimestampHelper.now(), Optional.empty(), reason); // Database to be updated later, after plan accepted
                    return job;
                }).collect(Collectors.toList()));

        return appJobs.stream().filter(appJobPair -> appJobPair.hasApplication()).collect(Collectors.toList());
    }

    // Calculate BEST plan ever, this method apparently must be PURE.
    // INPUT: offers currently this Retz instance has
    // INPUT: jobs - candidates for task launch, most likely chosen from database or else
    @Override
    public Plan plan(List<Protos.Offer> offers, List<AppJobPair> jobs, int maxStock) {
        Resource total = new Resource(0, 0, 0, 0, 0);
        for (Protos.Offer offer : offers) {
            total.merge(ResourceConstructor.decode(offer.getResourcesList()));
        }
        if (!resourceSufficient(total, jobs)) {

            List<Protos.Offer> toStock;
            List<Protos.OfferID> toDecline;
            if (offers.size() < maxStock) {
                toStock = offers;
                toDecline = Arrays.asList();
            } else {
                toStock = offers.subList(0, maxStock);
                toDecline = offers.subList(maxStock, offers.size()).stream().map(offer -> offer.getId()).collect(Collectors.toList());
            }
            return new Plan(Arrays.asList(), // Operations
                    Arrays.asList(), // To be launched
                    Arrays.asList(), // To be cancelled
                    Arrays.asList(), // Resources to be accepted
                    toDecline, toStock);
        }


        List<Job> cancel = new LinkedList<>();

        // This is ugly, ugh, but I want to keep pack() PURE.
        List<Protos.Offer.Operation> ops = new LinkedList<>();
        List<Job> launch = new LinkedList<>();
        List<Job> spill = new LinkedList<>();
        List<Protos.Offer> accept = new LinkedList<>();
        List<Protos.Offer> remain = new LinkedList<>();
        pack(offers, jobs, ops, launch, spill, accept, remain);

        List<Protos.Offer> toStock;
        List<Protos.OfferID> toDecline;
        if (remain.size() < maxStock) {
            toStock = remain;
            toDecline = Arrays.asList();
        } else {
            toStock = remain.subList(0, maxStock);
            toDecline = remain.subList(maxStock, remain.size()).stream().map(offer -> offer.getId()).collect(Collectors.toList());
        }
        spill.addAll(cancel);
        return new Plan(ops, launch, cancel,
                accept.stream().map(offer -> offer.getId()).collect(Collectors.toList()),
                toDecline, toStock);
    }
}
