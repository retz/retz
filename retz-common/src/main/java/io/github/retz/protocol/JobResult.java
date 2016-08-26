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

public class JobResult {
    private String taskId;
    private String finished;
    private int result = -1;
    private String reason;

    @JsonCreator
    public JobResult(@JsonProperty(value = "taskId", required = true) String taskId,
                     @JsonProperty(value = "result", required = true) int result,
                     @JsonProperty(value = "finished", required = true) String finished,
                     @JsonProperty("reason") String reason){
        this.taskId = taskId;
        this.finished = finished;
        this.result = result;
        this.reason = reason;
    }

    @JsonGetter("taskId")
    public String taskId() {
        return taskId;
    }

    @JsonGetter("finished")
    public String finished() {
        return finished;
    }

    @JsonGetter("result")
    public int result() {
        return result;
    }

    @JsonGetter("reason")
    public String reason() {
        return reason;
    }
}
