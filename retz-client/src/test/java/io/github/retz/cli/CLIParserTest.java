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
package io.github.retz.cli;

import io.github.retz.protocol.data.Job;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CLIParserTest {

    @Test
    public void pairParserTest() {
        String[] envs = {"k=v", "a=b", "FOOBAR='LOOK_AT ME DONT'", "wat", "empty=", "=not"};
        Properties props = SubCommand.parseKeyValuePairs(Arrays.asList(envs));
        for (Map.Entry<Object, Object> e : props.entrySet()) {
            System.err.println(e.getKey() + " = " + e.getValue());
        }
        assertEquals(4, props.size());
        assertEquals("v", props.getProperty("k"));
        assertEquals("b", props.getProperty("a"));
        assertEquals("'LOOK_AT ME DONT'", props.getProperty("FOOBAR"));
        assertEquals(null, props.getProperty("wat"));
        assertEquals("", props.getProperty("empty"));
        assertEquals(null, props.getProperty(""));
        assertEquals(null, props.getProperty("not"));
        assertEquals(null, props.getProperty("other foobar"));
    }

    @Test
    public void pairParserTest2() {
        String[] envs = {"ASAKUSA_M3BP_ARGS=\"--engine-conf com.asakusafw.m3bp.output.buffer.size=209 --engine-conf com.asakusafw.m3bp.output.buffer.flush=0.6 --engine-conf com.asakusafw.m3bp.thread.max=\\$RETZ_CPU\""};
        Properties props = SubCommand.parseKeyValuePairs(Arrays.asList(envs));
        assertEquals(1, props.size());
        assertEquals(
                "\"--engine-conf com.asakusafw.m3bp.output.buffer.size=209 --engine-conf com.asakusafw.m3bp.output.buffer.flush=0.6 --engine-conf com.asakusafw.m3bp.thread.max=\\$RETZ_CPU\"",
                props.getProperty("ASAKUSA_M3BP_ARGS"));

    }


    @Test
    public void statesParserTest() {
        CommandList cmd = new CommandList();
        Set<Job.JobState> states = cmd.parseStates("QUEUED,STARTING,STARTED");
        assertTrue(states.contains(Job.JobState.QUEUED));
        assertTrue(states.contains(Job.JobState.STARTING));
        assertTrue(states.contains(Job.JobState.STARTED));
        assertFalse(states.contains(Job.JobState.FINISHED));
        assertFalse(states.contains(Job.JobState.KILLED));
    }
}
