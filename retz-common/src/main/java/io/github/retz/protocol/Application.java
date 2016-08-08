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
package io.github.retz.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Optional;

public class Application {
    private String appid;
    private List<String> persistentFiles;
    private List<String> files;
    private Optional<Integer> diskMB;

    @JsonCreator
    public Application(@JsonProperty(value = "appid", required = true) String appid,
                       @JsonProperty("persistentFiles") List<String> persistentFiles,
                       @JsonProperty("files") List<String> files,
                       @JsonProperty("diskMB") Optional<Integer> diskMB) {
        this.appid = appid;
        this.persistentFiles = persistentFiles;
        this.files = files;
        this.diskMB = diskMB;
    }

    @JsonGetter("appid")
    public String getAppid() {
        return appid;
    }

    @JsonGetter("persistentFiles")
    public List<String> getPersistentFiles() {
        return persistentFiles;

    }

    @JsonGetter("files")
    public List<String> getFiles() {
        return files;
    }

    @JsonGetter("diskMB")
    public Optional<Integer> getDiskMB() {
        return diskMB;
    }

    public String toVolumeId() {
        return Application.toVolumeId(getAppid());
    }

    public static String toVolumeId(String appId) {
        // http://mesos.apache.org/documentation/latest/persistent-volume/
        // "Volume IDs must be unique per role on each agent. However, it is strongly recommended
        // that frameworks use globally unique volume IDs, to avoid potential confusion between
        // volumes on different agents that use the same volume ID. Note also that the agent ID
        // where a volume resides might change over time."
        StringBuilder sb = new StringBuilder()
                .append("retz-")
                .append(appId);
        return sb.toString();
    }
}

