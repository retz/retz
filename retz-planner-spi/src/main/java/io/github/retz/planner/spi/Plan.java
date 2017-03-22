/**
 * Retz
 * Copyright (C) 2016-2017 Nautilus Technologies, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.retz.planner.spi;

import io.github.retz.protocol.data.Job;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Plan {
    private Map<String, List<Job>> jobSpecs = new HashMap<>();
    private List<Job> toKeep = new LinkedList<>();
    private List<String> offerIdsToStock = new LinkedList<>();

    public Plan() {
    }

    public void setJob(String offerId, Job job) {
        List<Job> jobs = jobSpecs.getOrDefault(offerId, new LinkedList<>());
        jobs.add(job);
        if (!jobSpecs.containsKey(offerId)) {
            jobSpecs.put(offerId, jobs);
        }
    }

    public void addStock(String offerId) {
        offerIdsToStock.add(offerId);
    }

    public void addKeep(List<Job> jobs) {
        toKeep.addAll(jobs);
    }

    public Map<String, List<Job>> getJobSpecs() {
        return jobSpecs;
    }

    public List<Job> getToKeep() {
        return toKeep;
    }

    public List<String> getOfferIdsToStock() {
        return offerIdsToStock;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{").append("jobSpecs={");
        for (Map.Entry<String, List<Job>> entry : jobSpecs.entrySet()) {
            sb.append(entry.getKey()).append("={")
                    .append(String.join(",",
                            entry.getValue().stream().map(j -> j.pp()).collect(Collectors.toList())))
                    .append("},");
        }
        sb.append("}, toKeep={");
        sb.append(String.join(", ", toKeep.stream().map(j -> j.pp()).collect(Collectors.toList())));
        sb.append("}, offerIdsToStock={");
        sb.append(String.join(", ", offerIdsToStock));
        sb.append("}}");
        return sb.toString();
    }
}


