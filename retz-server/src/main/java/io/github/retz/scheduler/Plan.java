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

import io.github.retz.protocol.data.Job;
import org.apache.mesos.Protos;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Plan {
    private List<Protos.Offer.Operation> operations;
    private List<Job> toBeLaunched;
    private List<Job> toCancel;
    private List<Protos.OfferID> toBeAccepted;
    private List<Protos.OfferID> toDecline;
    private List<Protos.Offer> toStock;

    Plan(List<Protos.Offer.Operation> operations,
         List<Job> toBeLaunched,
         List<Job> toCancel,
         List<Protos.OfferID> toBeAccepted,
         List<Protos.OfferID> toDecline,
         List<Protos.Offer> toStock) {
        this.operations = Objects.requireNonNull(operations);
        this.toBeLaunched = Objects.requireNonNull(toBeLaunched);
        this.toCancel = Objects.requireNonNull(toCancel);
        this.toBeAccepted = Objects.requireNonNull(toBeAccepted);
        this.toDecline = Objects.requireNonNull(toDecline);
        this.toStock = Objects.requireNonNull(toStock);
    }

    public List<Protos.Offer.Operation> getOperations() {
        return operations;
    }
    public List<Protos.OfferID> getToBeAccepted() {
        return toBeAccepted;
    }
    public List<Job> getToBeLaunched() {
        return toBeLaunched;
    }
    public List<Job> getToCancel() {
        return toCancel;
    }
    public List<Protos.OfferID> getToDecline() {
        return toDecline;
    }
    public Map<String, Protos.Offer> getToStock() {
        Map<String, Protos.Offer> map = new HashMap<>();
        for (Protos.Offer o : toStock) {
            map.put(o.getId().getValue(), o);
        }
        return map;
    }
}
