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
package io.github.retz.web;

import io.github.retz.auth.ProtocolTester;
import io.github.retz.scheduler.RetzScheduler;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Created by kuenishi on 16/12/12.
 */
public class VersionCheckTest {
    @Test
    public void current() {
        ProtocolTester tester = new ProtocolTester(RetzScheduler.HTTP_SERVER_NAME, Client.VERSION_STRING);
        assertTrue(tester.validate());
    }
}
