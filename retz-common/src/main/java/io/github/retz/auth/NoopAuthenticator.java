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

public class NoopAuthenticator implements Authenticator {
    private final String KEY;

    public NoopAuthenticator(String key) {
        this.KEY = key;
    }

    @Override
    public boolean authenticate(String verb, String md5, String date, String resource, String key, String sign) {
        return true;
    }

    @Override
    public String signature(String verb, String md5, String date, String resource) {
        return "all-okay";
    }

    @Override
    public String string2sign(String verb, String md5, String date, String resource) {
        return "<nothing to sign>";
    }

    @Override
    public AuthHeader header(String verb, String md5, String date, String resource) {
        return new AuthHeader(KEY, "all-okay");
    }

    @Override
    public String getKey() {
        return KEY;
    }
}
