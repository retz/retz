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

import io.github.retz.protocol.data.Range;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * POJO of Mesos attribute described at: http://mesos.apache.org/documentation/latest/attributes-resources/
 **/
public class Attribute {
    public enum Type {
        SCALAR,
        RANGES,
        SET,
        TEXT
    }

    private String name;
    private Type t;
    private Object holder;

    public Attribute(String name, Type t, Object o) {
        this.name = Objects.requireNonNull(name);
        this.t = Objects.requireNonNull(t);
        this.holder = Objects.requireNonNull(o);
    }

    public boolean isScalar() {
        return t == Type.SCALAR;
    }

    public boolean isRanges() {
        return t == Type.RANGES;
    }

    public boolean isSet() {
        return t == Type.SET;
    }

    public boolean isText() {
        return t == Type.TEXT;
    }

    public String name() {
        return name;
    }

    public double asScalar() {
        return ((Double) holder).doubleValue();
    }

    public List<Range> asRanges() {
        return (List<Range>) holder;
    }

    public Set<String> asSet() {
        return (Set<String>) holder;
    }

    public String asText() {
        return (String) holder;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder()
                .append("attr{name=")
                .append(name)
                .append(", type=").append(t)
                .append(", value=").append(holder)
                .append("}");
        return builder.toString();
    }
}
