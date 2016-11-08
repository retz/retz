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

public class Range {
    public int min;
    public int max;

    @JsonCreator
    public Range(@JsonProperty(value = "min", required = true) int min,
                 @JsonProperty("max") int max) {
        this.min = min;
        if (max == 0) {
            this.max = Integer.MAX_VALUE;
        } else if (max < min){
            throw new IllegalArgumentException();
        } else {
            this.max = max;
        }
    }

    @JsonGetter
    public int getMin() {
        return min;
    }

    @JsonGetter
    public int getMax() {
        return max;
    }

    public boolean overlap(Range rhs) {
        return (this.min - rhs.max) * (this.max - rhs.min) <= 0;
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
            int i = Integer.parseInt(pair[0]);
            return new Range(i, i);
        }
        int max = 0;
        try {
            max = Integer.parseInt(pair[1]);
        } catch (NumberFormatException e) {
        }
        int min = 0;
        try {
            min = Integer.parseInt(pair[0]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Minimal value of a range must not be absent" + s);
        }
        return new Range(min, max);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append(min).append("-");
        if (max < Integer.MAX_VALUE) {
            b.append(max);
        }
        return b.toString();
    }
}
