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
import io.github.retz.planner.spi.Attribute;
import io.github.retz.planner.spi.Offer;
import io.github.retz.planner.spi.Resource;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.scheduler.Applications;
import io.github.retz.scheduler.TaskBuilder;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class ExtensiblePlanner implements Planner {
    private static final Logger LOG = LoggerFactory.getLogger(ExtensiblePlanner.class);

    private final io.github.retz.planner.spi.Planner extension;

    public ExtensiblePlanner(io.github.retz.planner.spi.Planner e, Properties p) throws Throwable {
        extension = e;
        extension.initialize(p);
        //TODO: orderBy validation
    }

    @Override
    public List<String> orderBy() {
        return extension.orderBy();
    }

    @Override
    public List<AppJobPair> filter(List<Job> jobs, List<Job> keep, boolean useGPU) {
        extension.setUseGpu(useGPU);

        // TODO: better splitter
        // Not using GPU or (using GPU and GPU enabled)
        List<Job> run = jobs.stream().filter(job -> extension.filter(job)).collect(Collectors.toList());
        keep.addAll(jobs.stream()
                .filter(job -> !extension.filter(job)) // Using GPU && GPU not enabled
                .map(job -> {
                    String reason = String.format("Job (%d@%s) requires %d GPUs while this Retz Scheduler is not capable of using GPU resources. Try setting retz.gpu=true at retz.properties.",
                            job.id(), job.appid(), job.resources().getGpu());
                    job.killed(TimestampHelper.now(), Optional.empty(), reason); // Database to be updated later, after plan accepted
                    return job;
                }).collect(Collectors.toList()));

        List<AppJobPair> appJobs = run.stream().map(job -> {
            Optional<Application> app = Applications.get(job.appid());
            return new AppJobPair(app, job);
        }).collect(Collectors.toList());

        keep.addAll(appJobs.stream()
                .filter(appJobPair -> !appJobPair.hasApplication())
                .map(appJobPair -> {
                    Job job = appJobPair.job();
                    String reason = String.format("Applicaion '%s' of Job (%d@%s) does not exist in DB", job.appid(), job.id());
                    job.killed(TimestampHelper.now(), Optional.empty(), reason); // Database to be updated later, after plan accepted
                    return job;
                }).collect(Collectors.toList()));

        return appJobs.stream().filter(appJobPair -> appJobPair.hasApplication()).collect(Collectors.toList());
    }

    @Override
    public Plan plan(List<Protos.Offer> offers, List<AppJobPair> appJobPairs, int maxStock, String unixUser) {
        extension.setMaxStock(maxStock);

        Map<String, Offer> mapOffers = new LinkedHashMap<>();
        for (Protos.Offer offer : offers) {
            // maybe TODO: salvage more properties from Protos.Offer to include in spi.Offer
            List<Attribute> attrs = new AttributeBuilder(offer.getAttributesList()).build();
            if (LOG.isDebugEnabled()) {
                for (Attribute attr : attrs) {
                    LOG.debug("Attribute: {} @{}", attr, offer.getId().getValue());
                }
            }
            Resource resource = ResourceConstructor.decode(offer.getResourcesList());
            Offer newOffer = new Offer(offer.getId().getValue(), resource, attrs);
            mapOffers.put(offer.getId().getValue(), newOffer);
        }

        List<Job> jobs = appJobPairs.stream().map(appJobPair -> appJobPair.job()).collect(Collectors.toList());

        io.github.retz.planner.spi.Plan p = extension.plan(mapOffers, jobs);
        List<Job> keep = p.getToKeep();

        List<OfferAcceptor> acceptors = new ArrayList<>();
        List<Protos.Offer> toStock = new ArrayList<>();

        for (Protos.Offer offer : offers) {
            if (p.getOfferIdsToStock().contains(offer.getId().getValue())) {
                toStock.add(offer);
                continue;
            }

            OfferAcceptor acceptor = new OfferAcceptor(offer);
            if (p.getJobSpecs().containsKey(offer.getId().getValue())) {
                List<Job> jobs1 = p.getJobSpecs().get(offer.getId().getValue());

                Resource resource = ResourceConstructor.decode(offer.getResourcesList());
                int last = resource.lastPort();
                for (Job job : jobs1) {
                    System.err.println(job.appid());
                    Application application = Applications.get(job.appid()).get();
                    TaskBuilder builder = new TaskBuilder();
                    String id = Integer.toString(job.id());

                    Protos.TaskInfo taskInfo = builder.setName(job.name())
                            .setResource(resource.cut(job.resources(), last), offer.getSlaveId())
                            .setCommand(job, application, unixUser)
                            .setName("retz-" + application.getAppid() + "-name-" + job.name())
                            .setTaskId("retz-" + application.getAppid() + "-id-" + id)
                            .build();

                    Protos.TaskID taskId = taskInfo.getTaskId();
                    job.starting(taskId.getValue(), Optional.empty(), TimestampHelper.now());

                    acceptor.addTask(taskInfo, job);
                }
            }
            acceptors.add(acceptor);
        }

        return new Plan(acceptors, keep, toStock);
    }
}
