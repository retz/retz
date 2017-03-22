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
package io.github.retz.planner;

import io.github.retz.planner.spi.Attribute;
import io.github.retz.protocol.data.Range;
import org.apache.mesos.Protos;

import java.util.LinkedList;
import java.util.List;

public class AttributeBuilder {
    private List<Attribute> list = new LinkedList<>();

    public AttributeBuilder(List<Protos.Attribute> attrs) {
        for (Protos.Attribute attr : attrs) {

            Attribute.Type t;
            Object o;
            if (attr.hasScalar()) {
                t = Attribute.Type.SCALAR;
                o = Double.valueOf(attr.getScalar().getValue());
            } else if (attr.hasRanges()) {
                t = Attribute.Type.RANGES;
                List<Range> ranges = new LinkedList<>();
                for (Protos.Value.Range range : attr.getRanges().getRangeList()) {
                    Range r = new Range(range.getBegin(), range.getEnd());
                    ranges.add(r);
                }
                o = ranges;
            } else if (attr.hasSet()) {
                t = Attribute.Type.SET;
                o = attr.getSet();
            } else if (attr.hasText()) {
                t = Attribute.Type.TEXT;
                o = attr.getText();
            } else {
                throw new AssertionError("attribute has unknown type: " + attr.getType());
            }
            Attribute a = new Attribute(attr.getName(), t, o);
            list.add(a);
        }
    }

    public List<Attribute> build() {
        return list;
    }
}
