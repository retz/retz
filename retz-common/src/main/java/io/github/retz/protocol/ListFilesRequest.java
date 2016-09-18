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

import java.util.Objects;

public class ListFilesRequest extends Request {
    public static final String DEFAULT_SANDBOX_PATH = "$MESOS_SANDBOX";

    private int id;
    private String path;

    @JsonCreator
    public ListFilesRequest(@JsonProperty(value = "id", required = true) int id,
                            @JsonProperty(value = "path", required = true) String dir) {
        this.id = id;
        this.path = Objects.requireNonNull(dir);
    }

    @JsonGetter("id")
    public int id() {
        return id;
    }

    @JsonGetter("path")
    public String path() {
        return path;
    }

    @Override
    public String resource() {
        StringBuilder builder = new StringBuilder("/job/")
                .append(id)
                .append("/path/").append(path);
        return builder.toString();
    }

    @Override
    public String method() {
        return GET;
    }

    @Override
    public boolean hasPayload() {
        return false;
    }

    public static String resourcePattern() {
        return "/job/:id/path/:path";
    }
}
