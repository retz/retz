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
package io.github.retz.cli;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Time;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TimestampHelperTest {

    @Test
    public void timestamp() throws Exception {
        System.err.println(TimestampHelper.now());
        for (int i = 0; i < 1000; i++) {
            String t = TimestampHelper.now();
            String p = TimestampHelper.past(42);
            assertEquals(0, TimestampHelper.diffMillisec(t, t));
            // Two timestamps taken within 10ms
            assertTrue(Math.abs(TimestampHelper.diffMillisec(t, p) - 42*1000) < 10);
            System.err.println(t + " - " + p + " = " + TimestampHelper.diffMillisec(t, p));
        }
    }
}
