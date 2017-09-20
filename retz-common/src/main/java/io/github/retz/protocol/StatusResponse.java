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
package io.github.retz.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.retz.protocol.data.ResourceQuantity;

import java.util.Objects;
import java.util.Optional;

public class StatusResponse extends Response {
    private int queueLength;
    private int runningLength;
    private ResourceQuantity totalUsed;
    private int numSlaves; // TODO: use this value in report
    private int offers;
    private ResourceQuantity totalOffered;
    private Optional<String> master;
    private int stanchionQueueLength;
    private final String serverVersion;

    @JsonCreator
    public StatusResponse(@JsonProperty("queueLength") int queueLength,
                          @JsonProperty("runningLength") int runningLength,
                          @JsonProperty("totalUsed") ResourceQuantity totalUsed,
                          @JsonProperty("numSlaves") int numSlaves,
                          @JsonProperty("offers") int offers,
                          @JsonProperty("totalOffered") ResourceQuantity totalOffered,
                          @JsonProperty("master") Optional<String> master,
                          @JsonProperty("stanchionQueueLength") int stanchionQueueLength,
                          @JsonProperty("serverVersion") String serverVersion) {
        this.queueLength = queueLength;
        this.runningLength = runningLength;
        this.totalUsed = totalUsed;
        this.numSlaves = numSlaves;
        this.offers = offers;
        this.totalOffered = totalOffered;
        this.master = (master == null) ? Optional.empty() : master;
        this.stanchionQueueLength = stanchionQueueLength;
        this.serverVersion = serverVersion;
    }

    public StatusResponse(String serverVersion) {
        this.ok();
        this.serverVersion = serverVersion;
        totalOffered = new ResourceQuantity();
        totalUsed = new ResourceQuantity();
        this.master = Optional.empty();
    }

    @JsonGetter("queueLength")
    public int queueLength() {
        return queueLength;
    }

    @JsonGetter("runningLength")
    public int runningLength() {
        return runningLength;
    }

    @JsonGetter("totalUsed")
    public ResourceQuantity totalUsed() {
        return totalUsed;
    }

    @JsonGetter("numSlaves")
    public int numSlaves() {
        return numSlaves;
    }

    @JsonGetter("offers")
    public int offers() {
        return offers;
    }

    @JsonGetter("totalOffered")
    public ResourceQuantity totalOffered() {
        return totalOffered;
    }

    @JsonGetter("master")
    public Optional<String> master() {
        return master;
    }

    @JsonGetter("stanchionQueueLength")
    public int stanchionQueueLength() {
        return stanchionQueueLength;
    }

    @JsonGetter("serverVersion")
    public String serverVersion() {
        return serverVersion;
    }

    public void setOffers(int size, ResourceQuantity offered) {
        this.offers = size;
        this.totalOffered = offered;
    }

    public void setUsedResources(int queueLength, int runningLength, ResourceQuantity totalUsed) {
        this.queueLength = queueLength;
        this.runningLength = runningLength;
        this.totalUsed = totalUsed;
    }

    public void setStanchionQueueLength(int stanchionQueueLength) {
        this.stanchionQueueLength = stanchionQueueLength;
    }

    public void setMaster(String master) {
        this.master = Optional.of(master);
    }

    public void voidMaster() {
        this.master = Optional.empty();
    }
}
