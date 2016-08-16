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
package io.github.retz.cli;

import org.junit.Assert;
import org.junit.Test;

public class TimestampHelperTest {

    @Test
    public void timestamp() throws Exception {
        for (int i = 0; i < 1000; i++) {
            String t = TimestampHelper.now();
            Assert.assertEquals(0, TimestampHelper.diffSec(t, t));
        }
    }
}
