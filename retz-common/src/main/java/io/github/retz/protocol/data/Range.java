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

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class Range {
    // Inclusive
    public long min;
    public long max;

    @JsonCreator
    public Range(@JsonProperty(value = "min", required = true) long min,
                 @JsonProperty("max") long max) {
        this.min = min;
        if (max == 0) {
            this.max = Long.MAX_VALUE;
        } else if (max < min) {
            throw new IllegalArgumentException();
        } else {
            this.max = max;
        }
    }

    @JsonGetter
    public long getMin() {
        return min;
    }

    @JsonGetter
    public long getMax() {
        return max;
    }

    public boolean overlap(Range rhs) {
        return (this.min - rhs.max) * (this.max - rhs.min) <= 0;
    }

    // this -- rhs
    public List<Range> subtract(Range rhs) {
        if (!this.overlap(rhs)) {
            return Arrays.asList(this);
        }
        if (rhs.getMin() < this.getMin()) {
            if (rhs.getMax() < this.getMax()) {
                // rhs.min <= this.min <= rhs.max <=!!= this.max
                return Arrays.asList(new Range(rhs.getMax() + 1, this.getMax()));
            } else if (rhs.getMax() == this.getMax()) {
                return Collections.emptyList();
            } else {
                // rhs.min <= this.min <=!!= this.max <= rhs.max
                return Arrays.asList();
            }
        } else {
            if (rhs.getMax() < this.getMax()) {
                // this.min <!! rhs.min < rhs.max <=!!= this.max
                if (this.getMin() == rhs.getMin()) {
                    return Arrays.asList(new Range(rhs.getMax() + 1, this.getMax()));
                }
                return Arrays.asList(new Range(this.getMin(), rhs.getMin() - 1),
                        new Range(rhs.getMax() + 1, this.getMax()));
            } else if (rhs.getMax() == this.getMax()) {
                if (this.getMin() == rhs.getMin()) {
                    return Collections.emptyList();
                }
                return Arrays.asList(new Range(this.getMin(), rhs.getMin() - 1));
            } else {
                // this.min < rhs.min < this.max < rhs.max
                return Arrays.asList(new Range(this.getMin(), rhs.getMin() - 1));
            }
        }
    }

    public static Range parseRange(String s, String dflt) {
        if (s == null) {
            return parseRange(dflt);
        }
        return parseRange(s);
    }
    public static Range parseRange(String s) {
        String[] pair = s.split("-", 2);
        if (pair.length == 1) {
            //int i = Integer.parseInt(pair[0]);
            long l = Long.parseLong(pair[0]);
            return new Range(l, l);
        }
        long max = 0;
        try {
            max = Long.parseLong(pair[1]);
        } catch (NumberFormatException e) {
        }
        long min = 0;
        try {
            min = Long.parseLong(pair[0]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Minimal value of a range must not be absent" + s);
        }
        return new Range(min, max);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append(min).append("-");
        if (max < Long.MAX_VALUE) {
            b.append(max);
        }
        return b.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Range range = (Range) o;

        if (min != range.min) {
            return false;
        }
        return max == range.max;
    }

    @Override
    public int hashCode() {
        int result = (int) (min ^ (min >>> 32));
        result = 31 * result + (int) (max ^ (max >>> 32));
        return result;
    }
}
