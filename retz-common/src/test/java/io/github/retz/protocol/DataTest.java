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


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.retz.protocol.data.DirEntry;
import io.github.retz.protocol.data.FileContent;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class DataTest {

    @Test
    public void longTest() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        { // Test GH#81 https://github.com/retz/retz/issues/81
            String d = "{\"gid\":\"1000\",\"mode\":\"-rw-r--r--\",\"mtime\":1479109579.0,\"nlink\":1,\"path\":\"\\/var\\/lib\\/mesos\\/...\\/results\\/99.bin\",\"size\":5122002944,\"uid\":\"1000\"}";
            DirEntry entry = mapper.readValue(d, DirEntry.class);
            assertEquals(5122002944L, entry.size());
        }

        {
            String f = "{\"data\": \"deadbeef\", \"offset\":999999999999}";
            FileContent content = mapper.readValue(f, FileContent.class);
            assertEquals(999999999999L, content.offset());
        }
    }
}
