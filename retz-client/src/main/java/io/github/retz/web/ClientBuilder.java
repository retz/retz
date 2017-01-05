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
package io.github.retz.web;

import io.github.retz.auth.Authenticator;

import java.net.URI;

public class ClientBuilder {

    private Authenticator authenticator = null;
    private boolean checkCert = true;
    private URI uri;
    private boolean verbose;

    protected ClientBuilder(URI uri) {
        this.uri = uri;
    }

    public ClientBuilder setAuthenticator(Authenticator authenticator) throws IllegalArgumentException {
        this.authenticator = authenticator;
        return this;
    }

    public ClientBuilder checkCert(boolean checkCert) {
        this.checkCert = checkCert;
        return this;
    }

    public ClientBuilder setVerboseLog(boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    public Client build() {
        if (authenticator != null) {
            Client c = new Client(uri, authenticator, checkCert);
            c.setVerboseLog(this.verbose);
            return c;
        } else {
            throw new IllegalArgumentException("Authenticator (retz.access.key) must be set");
        }
    }
}
