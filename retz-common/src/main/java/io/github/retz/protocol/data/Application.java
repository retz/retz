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
package io.github.retz.protocol.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class Application {
    private String appid;
    private List<String> persistentFiles;
    private List<String> largeFiles;
    private List<String> files;
    private Optional<Integer> diskMB;
    // User is just a String that specifies Unix user
    private Optional<String> user;
    // Owner is Retz access key
    private String owner;

    private Container container;
    private boolean enabled;

    @JsonCreator
    public Application(@JsonProperty(value = "appid", required = true) String appid,
                       @JsonProperty("persistentFiles") List<String> persistentFiles,
                       @JsonProperty("largeFiles") List<String> largeFiles,
                       @JsonProperty("files") List<String> files,
                       @JsonProperty("diskMB") Optional<Integer> diskMB,
                       @JsonProperty("user") Optional<String> user,
                       @JsonProperty(value = "owner", required = true) String owner,
                       @JsonProperty("container") Container container,
                       @JsonProperty("enabled") boolean enabled) {
        this.appid = Objects.requireNonNull(appid);
        this.persistentFiles = persistentFiles;
        this.largeFiles = (largeFiles == null) ? Arrays.asList() : largeFiles;
        this.files = (files == null) ? Arrays.asList() : files;
        this.diskMB = diskMB;
        this.owner = Objects.requireNonNull(owner);
        this.user = user;
        this.container = (container != null) ? container : new MesosContainer();
        this.enabled = enabled;
    }

    @JsonGetter("appid")
    public String getAppid() {
        return appid;
    }

    @JsonGetter("persistentFiles")
    public List<String> getPersistentFiles() {
        return persistentFiles;
    }

    @JsonGetter("largeFiles")
    public List<String> getLargeFiles() {
        return largeFiles;
    }

    @JsonGetter("files")
    public List<String> getFiles() {
        return files;
    }

    @JsonGetter("diskMB")
    public Optional<Integer> getDiskMB() {
        return diskMB;
    }

    @JsonGetter("user")
    public Optional<String> getUser() {
        return user;
    }

    @JsonGetter("owner")
    public String getOwner() {
        return owner;
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
        String maybeUser = "";
        if (user.isPresent()) {
            maybeUser = "user=" + user.get();
        }
        String enabledStr = "";
        if (!enabled) {
            enabledStr = "(disabled)";
        }
        return String.format("Application%s name=%s: owner=%s %s container=%s: files=%s/%s",
                enabledStr,
                getAppid(),
                owner,
                maybeUser,
                container().getClass().getSimpleName(),
                String.join(",", getFiles()),
                String.join(",", getLargeFiles()));
    }
}

