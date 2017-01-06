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

import java.util.Objects;
import java.util.Properties;

public class DockerVolume {

    private String driver; // Docker volume driver name
    private String containerPath; // Volume target path in sandbox
    public enum Mode {
        RO,
        RW
    }
    private Mode mode;
    private String name; // Volume name to mount

    private Properties options;

    @JsonCreator
    public DockerVolume(@JsonProperty(value = "driver", required = true) String driver,
                        @JsonProperty(value = "containerPath", required = true) String containerPath,
                        @JsonProperty("mode") Mode mode,
                        @JsonProperty(value = "name", required = true) String name,
                        @JsonProperty("options") Properties options) {
        this.driver = Objects.requireNonNull(driver);
        this.containerPath = Objects.requireNonNull(containerPath);
        this.mode = (mode == null) ? Mode.RW : mode;
        this.name = Objects.requireNonNull(name);
        this.options = (options == null)? new Properties() : options;
    }

    @JsonGetter("driver")
    public String driver() {
        return driver;
    }

    @JsonGetter("containerPath")
    public String containerPath() {
        return containerPath;
    }

    @JsonGetter("mode")
    public Mode mode() {
        return mode;
    }

    @JsonGetter("name")
    public String name() {
        return name;
    }

    @JsonGetter("options")
    public Properties options() {
        return options;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder()
                .append(this.getClass().getSimpleName()).append(":{")
                .append("driver=").append(driver)
                .append(",containerPath=").append(containerPath)
                .append(",mode=").append(mode)
                .append(",name=").append(name)
                .append(",options=").append(options)
                .append("}");
        return builder.toString();
    }
}
