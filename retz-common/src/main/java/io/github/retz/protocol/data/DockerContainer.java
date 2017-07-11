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
import java.util.stream.Collectors;

public class DockerContainer extends Container {

    private String image;
    private List<DockerVolume> volumes;

    @JsonCreator
    public DockerContainer(@JsonProperty(value = "image", required = true) String image,
                           @JsonProperty("volumes") List<DockerVolume> volumes) {
        this.image = Objects.requireNonNull(image);
        this.volumes = (volumes == null) ? Collections.emptyList() : volumes;
    }

    @JsonGetter("image")
    public String image() {
        return image;
    }

    @JsonGetter("volumes")
    public List<DockerVolume> volumes() {
        return volumes;
    }

    public String pp() {
        StringBuilder buffer = new StringBuilder("DockerContainer{")
                .append("image=").append(image)
                .append("volumes=[");

        buffer.append(volumes.stream().map(v -> v.toString()).collect(Collectors.joining(",")));

        return buffer.append("]}").toString();
    }
}
