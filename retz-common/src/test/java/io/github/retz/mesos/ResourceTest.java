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
package io.github.retz.mesos;

import io.github.retz.protocol.data.Range;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ResourceTest {
    @Test
    public void cutTest() {
        {
            List<Range> ports = Arrays.asList(new Range(31000, 32000));
            Resource resource = new Resource(1, 32, 0, 0, ports);
            Resource cut = resource.cut(1, 32, 0, 100, 0);
            assertEquals(100, cut.portAmount());
        }
        {
            List<Range> ports = Arrays.asList(new Range(31000, 32000));
            Resource resource = new Resource(1, 32, 0, 0, ports);
            Resource cut = resource.cut(1, 32, 0, 1, 0);
            System.err.println(cut);
            assertEquals(1, cut.portAmount());
        }
    }
}
