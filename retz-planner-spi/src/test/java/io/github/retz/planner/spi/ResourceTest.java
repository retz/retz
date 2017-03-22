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

import io.github.retz.planner.spi.Resource;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.Range;
import io.github.retz.protocol.data.ResourceQuantity;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

    @Test
    public void quantity() {
        {
            Job job = new Job("x", "a", new Properties(), 1, 32, 0, 0, 0);
            ResourceQuantity resourceQuantity = new ResourceQuantity(10, 1024, 0, 1, 1024,0);
            assertTrue(resourceQuantity.fits(job));
        }
        {
            Job job = new Job("x", "a", new Properties(), 20243, 65536, 235452, 0, 319);
            ResourceQuantity resourceQuantity = new ResourceQuantity(22827, 65536, 65536, 36372, 4165008, 0);
            assertTrue(resourceQuantity.fits(job));
        }
    }
}
