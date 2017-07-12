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

import io.github.retz.protocol.data.Job;
import org.apache.mesos.Protos;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Plan {
    private List<OfferAcceptor> offerAcceptors;
    private List<Job> toKeep;
    private List<Protos.Offer> toStock;

    public Plan(List<OfferAcceptor> offerAcceptors,
         List<Job> toKeep,
         List<Protos.Offer> toStock) {
        this.offerAcceptors = Objects.requireNonNull(offerAcceptors);
        this.toKeep = Objects.requireNonNull(toKeep);
        this.toStock = Objects.requireNonNull(toStock);
    }

    public List<OfferAcceptor> getOfferAcceptors() {
        return offerAcceptors;
    }

    public List<Job> getToKeep() {
        return toKeep;
    }

    public List<Protos.Offer> getToStock() {
        return toStock;
    }

    @Override
    public String toString() {
        return new StringBuilder("plan={")
                .append("\nofferAcceptors=[")
                .append(offerAcceptors.stream().map((oa) -> oa.toString()).collect(Collectors.joining(",\n")))
                .append("],\ntoKeep=[")
                .append(toKeep.stream().map((job) -> job.pp()).collect(Collectors.joining(",\n")))
                .append("],\ntoStock=[")
                .append(toStock.stream().map((offer) -> offer.toString()).collect(Collectors.joining(",\n")))
                .append("]}")
                .toString();
    }
}
