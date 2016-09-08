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
import io.github.retz.protocol.data.Job;

import java.util.List;

public class ListJobResponse extends Response {
    private List<Job> queue;
    private List<Job> running;
    private List<Job> finished;

    @JsonCreator
    public ListJobResponse(@JsonProperty("queue") List<Job> queue,
                           @JsonProperty("running") List<Job> running,
                           @JsonProperty("finished") List<Job> finished) {
        this.queue = queue;
        this.running = running;
        this.finished = finished;
    }
    @JsonGetter("queue")
    public List<Job> queue() {
        return queue;
    }

    @JsonGetter("running")
    public List<Job> running() {
        return running;
    }

    @JsonGetter("finished")
    public List<Job> finished() {
        return finished;
    }
}
