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

import java.util.Optional;

public class AuthHeader {

    private static final String REALM = "Retz-auth-v1";
    public static final String AUTHORIZATION = "Authorization";

    public String key;
    public String signature;
    public AuthHeader(String k, String s) {
        key = k;
        signature = s;
    }
    public String key() {
        return key;
    }
    public String signature() {
        return signature;
    }

    // Just do inverse of buildHeaderValue
    public static Optional<AuthHeader> parseHeaderValue(String line) {
        if (line == null) {
            return Optional.empty();
        }
        String[] a = line.split(" ");
        if (a.length != 2) {
            return Optional.empty();
        } else if (! a[0].equals(REALM)) {
            return Optional.empty();
        }
        String[] b = a[1].split(":");
        if (b.length != 2) {
            return Optional.empty();
        }
        AuthHeader authHeader = new AuthHeader(b[0], b[1]);
        return Optional.of(authHeader);
    }

    public String buildHeader(){
        return new StringBuilder()
                .append(REALM).append(" ")
                .append(key).append(":")
                .append(signature)
                .toString();
    }

}