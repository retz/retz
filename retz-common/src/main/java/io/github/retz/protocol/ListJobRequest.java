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

import java.util.Objects;
import java.util.Optional;

public class ListJobRequest extends Request {
    private Job.JobState state;
    private Optional<String> tag;

    @JsonCreator
    public ListJobRequest(@JsonProperty(value = "state", required = true) Job.JobState state,
                          @JsonProperty("tag") Optional<String> tag) {
        this.state = Objects.requireNonNull(state);
        this.tag = tag;
    }

    @JsonGetter("state")
    public Job.JobState state() {
        return state;
    }

    @JsonGetter("tag")
    public Optional<String> tag() {
        return tag;
    }

    @Override
    public String resource() {
        return "/jobs";
    }

    @Override
    public String method() {
        return POST;
    }

    @Override
    public boolean hasPayload() {
        return true;
    }

    public static String resourcePattern() {
        return "/jobs";
    }
}
