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
package io.github.retz.planner.spi;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Offer {
    private String id;
    private Resource resource;
    private List<Attribute> attributes;
    // TODO: add several more in mesos.proto 'message Offer'

    public Offer(String id, Resource resource) {
        this(id, resource, Arrays.asList());
    }

    public Offer(String id, Resource resource, List<Attribute> attributes) {
        this.id = Objects.requireNonNull(id);
        this.resource = Objects.requireNonNull(resource);
        this.attributes = Objects.requireNonNull(attributes);
    }

    public String id() {
        return id;
    }

    public Resource resource() {
        return resource;
    }

    public List<Attribute> attributes() {
        return attributes;
    }
}
