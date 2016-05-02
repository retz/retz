/**
 *    Retz
 *    Copyright (C) 2016 Nautilus Technologies, KK.
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
package com.asakusafw.retz.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

public class StatusResponse extends Response {
    private int queueLength;
    private int runningLength;
    private int numSlaves;
    private int watcherLength;
    private int sessionLength;

    @JsonCreator
    public StatusResponse(@JsonProperty("queueLength") int queueLength,
                          @JsonProperty("runningLength") int runningLength,
                          @JsonProperty("numSlaves") int numSlaves,
                          @JsonProperty("watcherLength") int watcherLength,
                          @JsonProperty("sessionLength") int sessionLength) {
        this.queueLength=queueLength;
        this.runningLength=runningLength;
        this.numSlaves=numSlaves;
        this.watcherLength=watcherLength;
        this.sessionLength=sessionLength;
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

    public void setStatus(int queueLength, int numSlaves, int runningLength) {
        this.queueLength = queueLength;
        this.numSlaves = numSlaves;
        this.runningLength = runningLength;
    }

    public void setStatus2(int watcherLength, int sessionLength) {
        this.watcherLength = watcherLength;
        this.sessionLength = sessionLength;
    }
}
