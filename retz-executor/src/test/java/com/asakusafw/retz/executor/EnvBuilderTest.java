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
package com.asakusafw.retz.executor;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class EnvBuilderTest {
    @Test
    public void repTest() {
        EnvBuilder eb = new EnvBuilder(1, 2, "pv");
        eb.put("a", "foobar-$RETZ_CPU-foobar");
        eb.put("b", "foobar-$RETZ_MEM-foobar");
        eb.put("c", "foobar-$RETZ_PVNAME-foobar");
        Map<String, String> env = eb.build();

        Assert.assertEquals(env.get("a"), "foobar-1-foobar");
        Assert.assertEquals(env.get("b"), "foobar-2-foobar");
        Assert.assertEquals(env.get("c"), "foobar-pv-foobar");
    }
}
