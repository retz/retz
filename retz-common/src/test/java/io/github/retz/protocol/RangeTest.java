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
package io.github.retz.protocol;

import io.github.retz.protocol.data.Range;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RangeTest {

    @Test
    public void parseTest() {
        Range r;

        r = Range.parseRange("2-20");
        assertEquals(2, r.getMin());
        assertEquals(20, r.getMax());

        r = Range.parseRange("2-");
        assertEquals(2, r.getMin());
        assertEquals(Long.MAX_VALUE, r.getMax());

        int[] list = {2, 3, 4, 5, 10, 1024};
        for (int i : list) {
            r = Range.parseRange(Integer.toString(i));
            assertEquals(i, r.getMin());
            assertEquals(i, r.getMax());
        }

        r = Range.parseRange("0-0");
        assertEquals(0, r.getMin());
        assertEquals(Long.MAX_VALUE, r.getMax());

        r = Range.parseRange("2-0");
        assertEquals(2, r.getMin());
        assertEquals(Long.MAX_VALUE, r.getMax());
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseExceptionTest() {
        Range.parseRange("-3");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseExceptionTest2() {
        Range.parseRange("2--23");
    }


}
