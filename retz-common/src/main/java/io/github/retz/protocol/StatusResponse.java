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
package io.github.retz.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class StatusResponse extends Response {
    private int queueLength;
    private int runningLength;
    private int numSlaves; // TODO: use this value in report
    private int watcherLength;
    private int sessionLength;
    private int offers;
    private int totalCPUoffered;
    private int totalMEMoffered;
    private int totalGPUoffered;

    @JsonCreator
    public StatusResponse(@JsonProperty("queueLength") int queueLength,
                          @JsonProperty("runningLength") int runningLength,
                          @JsonProperty("numSlaves") int numSlaves,
                          @JsonProperty("watcherLength") int watcherLength,
                          @JsonProperty("sessionLength") int sessionLength,
                          @JsonProperty("offers") int offers,
                          @JsonProperty("totalCPUoffered") int totalCPUoffered,
                          @JsonProperty("totalMEMoffered") int totalMEMoffered,
                          @JsonProperty("totalGPUoffered") int totalGPUoffered) {
        this.queueLength=queueLength;
        this.runningLength=runningLength;
        this.numSlaves=numSlaves;
        this.watcherLength=watcherLength;
        this.sessionLength=sessionLength;
        this.offers = offers;
        this.totalCPUoffered = totalCPUoffered;
        this.totalMEMoffered = totalMEMoffered;
        this.totalGPUoffered = totalGPUoffered;
    }

    public StatusResponse() {
        this.ok();
    }

    @JsonGetter("queueLength")
    public int queueLength() {
        return queueLength;
    }

    @JsonGetter("runningLength")
    public int runningLength() {
        return runningLength;
    }

    @JsonGetter("numSlaves")
    public int numSlaves() {
        return numSlaves;
    }

    @JsonGetter("watcherLength")
    public int watcherLength() {
        return watcherLength;
    }

    @JsonGetter("sessionLength")
    public int sessionLength() {
        return sessionLength;
    }

    @JsonGetter("offers")
    public int offers() {
        return offers;
    }

    @JsonGetter("totalCPUoffered")
    public int totalCPUoffered() {
        return totalCPUoffered;
    }

    @JsonGetter("totalMEMoffered")
    public int totalMEMoffered() {
        return totalMEMoffered;
    }

    @JsonGetter("totalGPUoffered")
    public int totalGPUoffered() {
        return totalGPUoffered;
    }

    public void setStatus(int queueLength, int runningLength) {
        this.queueLength = queueLength;
        this.runningLength = runningLength;
    }

    public void setStatus2(int watcherLength, int sessionLength) {
        this.watcherLength = watcherLength;
        this.sessionLength = sessionLength;
    }

    public void setOfferStats(int size, int cpu, int mem, int gpu) {
        this.offers = size;
        this.totalCPUoffered = cpu;
        this.totalMEMoffered = mem;
        this.totalGPUoffered = gpu;
    }
}
