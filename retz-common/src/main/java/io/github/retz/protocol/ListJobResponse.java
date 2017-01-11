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
import io.github.retz.protocol.data.Job;

import java.util.List;

public class ListJobResponse extends Response {
    public static final int MAX_JOB_NUMBER = 1024;

    private List<Job> jobs;
    private boolean more = false; // there are more than MAX_JOB_NUMBER jobs

    @JsonCreator
    public ListJobResponse(@JsonProperty("jobs") List<Job> jobs,
                           @JsonProperty("more") boolean more) {
        this.jobs = jobs;
        this.more = more;
    }

    @JsonGetter("jobs")
    public List<Job> jobs() {
        return jobs;
    }

    @JsonGetter("more")
    public boolean more() {
        return more;
    }
}
