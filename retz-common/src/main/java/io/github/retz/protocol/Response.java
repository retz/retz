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

import com.fasterxml.jackson.annotation.*;

@JsonTypeInfo(property = "command",
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
        @JsonSubTypes.Type(name = "list", value = ListJobResponse.class),
        @JsonSubTypes.Type(name = "schedule", value = ScheduleResponse.class),
        @JsonSubTypes.Type(name = "get-job", value = GetJobResponse.class),
        @JsonSubTypes.Type(name = "kill", value = KillResponse.class),
        @JsonSubTypes.Type(name = "watch", value = WatchResponse.class),
        @JsonSubTypes.Type(name = "load-app", value = LoadAppResponse.class),
        @JsonSubTypes.Type(name = "unload-app", value = UnloadAppResponse.class),
        @JsonSubTypes.Type(name = "list-app", value = ListAppResponse.class),
        @JsonSubTypes.Type(name = "error", value = ErrorResponse.class),
        @JsonSubTypes.Type(name = "status", value = StatusResponse.class)
})
public abstract class Response {
    private String status; // Probably ENUM?

    @JsonCreator
    public Response (@JsonProperty("status") String status) {
        this.status = status;
    }
    public Response () {
    }
    @JsonGetter
    public String status() {
        return status;
    }
    public void status(String status) {
        this.status = status;
    }
    public void ok() {
        status = "ok";
    }
}
