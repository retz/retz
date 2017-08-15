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
package io.github.retz.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProtocolTester {
    static final Logger LOG = LoggerFactory.getLogger(ProtocolTester.class);

    private String serverVersion;
    private String clientVersion;

    public ProtocolTester(String s, String c) {
        serverVersion = Objects.requireNonNull(s);
        clientVersion = Objects.requireNonNull(c);
    }

    public boolean validate() {
        Version server = new Version(serverVersion);
        Version client = new Version(clientVersion);
        LOG.debug("Testing protocol compatibility: {}=>{} / {}=>{}",
                serverVersion, server, clientVersion, client);
        return validate(server, client);
    }

    public void test() {
        if (!validate()) {
            LOG.warn("Client and server version does not match: {} and {}", serverVersion, clientVersion);
        }
    }

    boolean validate(Version server, Version client) {
        if (server.equals(client)) {
            return true;
        }
        return server.minorMatch(client);
    }

    static class Version {
        //static Pattern versionPattern = Pattern.compile("\\d+\\.\\d+\\.\\d+(-SNAPSHOT)?");
        static Pattern versionPattern = Pattern.compile("\\d+\\.\\d+\\.\\d+");
        static Pattern snapshotPattern = Pattern.compile("-SNAPSHOT");
        int major;
        int minor;
        int patch;
        boolean isSnapshot;

        Version(String v) {
            Matcher mv = versionPattern.matcher(v);
            if (!mv.find()) {
                throw new RuntimeException("No version match for " + v);
            }
            //System.out.println("found => " + mv.group());
            String[] versions = mv.group().split("\\.");
            major = Integer.parseInt(versions[0]);
            minor = Integer.parseInt(versions[1]);
            patch = Integer.parseInt(versions[2]);
            Matcher ms = snapshotPattern.matcher(v);
            isSnapshot = ms.find();
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder()
                    .append(major).append(".")
                    .append(minor).append(".")
                    .append(patch);
            if (isSnapshot) {
                b.append("-SNAPSHOT");
            }
            return b.toString();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Version) {
                Version rhs = (Version) obj;
                return this.major == rhs.major &&
                        this.minor == rhs.minor &&
                        this.patch == rhs.patch &&
                        this.isSnapshot == rhs.isSnapshot;
            }
            return super.equals(obj);
        }

        @Override
        public int hashCode() {
            // TODO: using summation might be dodgy code
            return Integer.hashCode(major) + Integer.hashCode(minor) + Integer.hashCode(patch) + Boolean.hashCode(isSnapshot);
        }

        // Newer servers would support older clients;
        public boolean minorMatch(Version rhs) {
            return this.major == rhs.major && this.minor == rhs.minor;
        }
    }
}
