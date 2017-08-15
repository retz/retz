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
package io.github.retz.protocol.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class Application {
    private String appid;
    private List<String> largeFiles;
    private List<String> files;
    // User is just a String that specifies Unix user
    private Optional<String> user;
    // Owner is Retz access key
    private String owner;
    private int gracePeriod = 0; // if 0, no KillPolicy will be added to CommandInfo

    private Container container;
    private boolean enabled;

    @JsonCreator
    public Application(@JsonProperty(value = "appid", required = true) String appid,
                       @JsonProperty("largeFiles") List<String> largeFiles,
                       @JsonProperty("files") List<String> files,
                       @JsonProperty("user") Optional<String> user,
                       @JsonProperty(value = "owner", required = true) String owner,
                       @JsonProperty("gracePeriod") int gracePeriod,
                       @JsonProperty("container") Container container,
                       @JsonProperty("enabled") boolean enabled) {
        this.appid = Objects.requireNonNull(appid);
        this.largeFiles = (largeFiles == null) ? Collections.emptyList() : largeFiles;
        this.files = (files == null) ? Collections.emptyList() : files;
        this.owner = Objects.requireNonNull(owner);
        this.user = (user == null) ? Optional.empty() : user;
        if (gracePeriod > 0) {
            if (gracePeriod > 1024) {
                throw new IllegalArgumentException("Too large value for grace period: " + gracePeriod);
            }
            this.gracePeriod = gracePeriod;
        }
        this.container = (container != null) ? container : new MesosContainer();
        this.enabled = enabled;
    }

    @JsonGetter("appid")
    public String getAppid() {
        return appid;
    }

    @JsonGetter("largeFiles")
    public List<String> getLargeFiles() {
        return largeFiles;
    }

    @JsonGetter("files")
    public List<String> getFiles() {
        return files;
    }

    @JsonGetter("user")
    public Optional<String> getUser() {
        return user;
    }

    @JsonGetter("owner")
    public String getOwner() {
        return owner;
    }

    @JsonGetter("gracePeriod")
    public int getGracePeriod() {
        return gracePeriod;
    }

    @JsonGetter("container")
    public Container container() {
        return container;
    }

    @JsonGetter("enabled")
    public boolean enabled() {
        return enabled;
    }

    public void setContainer(Container c) {
        container = Objects.requireNonNull(c);
    }

    @Override
    public String toString() {
        String maybeUser = user.orElse("");
        String enabledStr = "";

        if (!enabled) {
            enabledStr = "(disabled)";
        }
        String maybeGracePriod = "";
        if (gracePeriod > 0) {
            maybeGracePriod = "gracePeriod=" + gracePeriod;
        }
        return String.format("Application%s name=%s: owner=%s %s %s container=%s: files=%s/%s",
                enabledStr,
                getAppid(),
                owner,
                maybeUser,
                maybeGracePriod,
                container().getClass().getSimpleName(),
                String.join(",", getFiles()),
                String.join(",", getLargeFiles()));
    }

    public String pp() {
        StringBuffer b = new StringBuffer("{");

        b.append("appid=").append(appid)
                .append(",largeFiles=[").append(String.join(",", largeFiles)).append("]")
                .append(",files=[").append(String.join(",", files)).append("]")
                .append(",user=").append(user)
                .append(",owner=").append(owner)
                .append(",gracePeriod=").append(gracePeriod)
                .append(",container=").append(container)
                .append(",enabled=").append(enabled);

        return b.append("}").toString();
    }
}

