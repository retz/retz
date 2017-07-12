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

import io.github.retz.planner.ResourceConstructor;
import io.github.retz.planner.spi.Resource;
import io.github.retz.protocol.data.Job;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class OfferAcceptor {
    private final Protos.SlaveID slaveID;
    private final List<Protos.Offer> offers = new ArrayList<>();
    private final List<Protos.Offer.Operation> operations = new ArrayList<>();
    private final List<Job> jobs = new ArrayList<>();

    public OfferAcceptor(Protos.Offer offer) {
        this.slaveID = Objects.requireNonNull(offer.getSlaveId());
        this.offers.add(offer);
    }

    public void addOffer(Protos.Offer offer) {
        if (!this.slaveID.getValue().equals(offer.getSlaveId().getValue())) {
            throw new AssertionError("It's bug: adding " + offer + " to " + this.slaveID.getValue());
        }
        offers.add(offer);
    }

    public void addTask(Protos.TaskInfo task, Job job) {
        Protos.Offer.Operation.Launch l = Protos.Offer.Operation.Launch.newBuilder()
                .addTaskInfos(Protos.TaskInfo.newBuilder(task))
                .build();

        operations.add(Protos.Offer.Operation.newBuilder()
                .setType(Protos.Offer.Operation.Type.LAUNCH)
                .setLaunch(l).build());

        jobs.add(job);
    }

    public Resource totalResource() {
        Resource resource = new Resource(0, 0, 0);
        for (Protos.Offer offer : offers) {
            resource.merge(ResourceConstructor.decode(offer.getResourcesList()));
        }
        return resource;
    }

    public void acceptOffers(SchedulerDriver driver, Protos.Filters filters) {
        driver.acceptOffers(offers.stream().map(offer -> offer.getId()).collect(Collectors.toList()),
                operations, filters);
    }

    public int declineOffer(SchedulerDriver driver, Protos.Filters filters) {
        for (Protos.Offer offer : offers) {
            driver.declineOffer(offer.getId(), filters);
        }
        return offers.size();
    }

    public List<Protos.Offer> getOffers() {
        return offers;
    }

    public Protos.SlaveID getSlaveID() {
        return slaveID;
    }

    public List<Job> getJobs() {
        return jobs;
    }

    public void verify() {
        for (Protos.Offer offer : offers) {
            if (!offer.getSlaveId().getValue().equals(slaveID.getValue())) {
                throw new AssertionError("Different Slave ID in single offer acceptor (" +
                        slaveID.getValue() + "):\t" + offer.getSlaveId().getValue());
            }
        }
        if (operations.size() != jobs.size()) {
            throw new AssertionError("Operations and jobs doesn't match in size: "+ operations.size() + "/" + jobs.size());
        }
    }

    @Override
    public String toString() {
        return new StringBuilder("OfferAcceptor{")
                .append("slaveId=").append(slaveID.getValue())
                .append(",\noffers=[")
                .append(offers.stream().map((offer) -> offer.toString()).collect(Collectors.joining(",")))
                .append("],\noperations=[")
                .append(operations.stream().map((op) -> op.toString()).collect(Collectors.joining(",")))
                .append("],\njobs=[")
                .append(jobs.stream().map((job) -> job.pp()).collect(Collectors.joining(",\n")))
                .append("]}")
                .toString();
    }
}
