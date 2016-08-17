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
package io.github.retz.scheduler;

import io.github.retz.protocol.Job;
import io.github.retz.protocol.Range;
import org.junit.Test;

import java.util.Optional;

public class JobQueueTest {
    @Test
    public void q() throws InterruptedException {
        Job job = new Job("a", "b", null, new Range(1000, 0), new Range(100000000, 0));
        JobQueue.push(job);
        Optional<Job> job2 = JobQueue.pop();
        assert job2.isPresent();
        assert job2.get().appid().equals(job.appid());
    }
}
