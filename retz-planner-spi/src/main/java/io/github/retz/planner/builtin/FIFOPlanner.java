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
package io.github.retz.planner.builtin;

import io.github.retz.planner.spi.Offer;
import io.github.retz.planner.spi.Plan;
import io.github.retz.planner.spi.Planner;
import io.github.retz.planner.spi.Resource;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.ResourceQuantity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class FIFOPlanner implements Planner {

    private static final Logger LOG = LoggerFactory.getLogger(FIFOPlanner.class);
    private final List<String> ORDER_BY = Arrays.asList("id");

    private boolean useGpu;
    private int maxStock;

    public FIFOPlanner() {
    }

    @Override
    public void initialize(Properties p) {
    }

    @Override
    public void setUseGpu(boolean useGpu) {
        this.useGpu = useGpu;
    }

    @Override
    public void setMaxStock(int maxStock) {
        this.maxStock = maxStock;
    }

    @Override
    public List<String> orderBy() {
        return ORDER_BY;
    }

    @Override
    public boolean filter(Job job) {
        if (job.resources().getGpu() > 0 && !useGpu) {
            // The job requires GPU while this planner configuration does not allow any GPU jobs
            return false;
        }
        return true;
    }

    @Override
    public Plan plan(Map<String, Offer> offers, List<Job> jobs) {

        Plan plan = new Plan();
        List<Job> queue = new LinkedList<>(jobs);
        for (Map.Entry<String, Offer> entry : offers.entrySet()) {
            ResourceQuantity total = new ResourceQuantity();
            while (!queue.isEmpty() && entry.getValue().resource().cpu() - total.getCpu() > 0) {
                Job job = queue.get(0);
                ResourceQuantity temp = total.copy(total);
                temp.add(job.resources());
                if (entry.getValue().resource().toQuantity().fits(temp)) {
                    plan.setJob(entry.getKey(), job);
                    queue.remove(0);
                    total.add(job.resources());
                } else {
                    break;
                }
            }
            if (! plan.getJobSpecs().containsKey(entry.getKey())
                    && plan.getOfferIdsToStock().size() < maxStock) {
                // No jobs found for this offer
                plan.addStock(entry.getKey());
            }
        }
        if (!queue.isEmpty()) {
            plan.addKeep(queue);
        }
        LOG.debug("Plan => {}", plan);
        return plan;
    }
}