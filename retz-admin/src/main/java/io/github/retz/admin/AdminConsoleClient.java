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
package io.github.retz.admin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.j256.simplejmx.client.JmxClient;
import io.github.retz.bean.AdminConsoleMXBean;
import io.github.retz.protocol.data.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class AdminConsoleClient implements AdminConsoleMXBean, AutoCloseable {
    static final Logger LOG = LoggerFactory.getLogger(AdminConsoleClient.class);

    private JmxClient client;
    private ObjectName objectName;
    private ObjectMapper mapper;

    public AdminConsoleClient(JmxClient client) throws MalformedObjectNameException {
        this.client = Objects.requireNonNull(client);
        objectName = new ObjectName("io.github.retz.scheduler:type=AdminConsole");
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new Jdk8Module());
    }

    @Override
    public boolean enableUser(String id, boolean enabled) {
        try {
            return (boolean) client.invokeOperation(objectName, "enableUser", id, enabled);
        } catch (Exception o) {
            return false;
        }
    }


    @Override
    public List<String> getUsage(String keyId, String start, String end) {
        try {
            String[] jsons = (String[]) client.invokeOperation(objectName, "getUsage", keyId, start, end);
            return Arrays.asList(jsons);
        } catch (Exception o) {
            return Arrays.asList();
        }
    }

    @Override
    public List<String> listUser() {
        try {
            String[] jsons = (String[]) client.invokeOperation(objectName, "listUser");
            return Arrays.asList(jsons);
        } catch (Exception o) {
            return Arrays.asList();
        }
    }

    public User createUserAsObject() throws IOException {
        String s = createUser();
        return mapper.readValue(s, User.class);
    }

    @Override
    public String createUser() {
        try {
            return (String) client.invokeOperation(objectName, "createUser");
        } catch (Exception o) {
            return "{}";
        }
    }

    public User getUserAsObject(String keyId) throws IOException {
        String s = getUser(keyId);
        return mapper.readValue(s, User.class);
    }

    @Override
    public String getUser(String keyId) {
        try {
            return (String) client.invokeOperation(objectName, "getUser", keyId);
        } catch (Exception o) {
            return "{}";
        }
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
