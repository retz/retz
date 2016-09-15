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

import io.github.retz.auth.Authenticator;

import java.net.URI;
import java.util.Objects;
import java.util.Optional;

public class ClientBuilder {

    private Optional<Authenticator> authenticatorOptional = Optional.empty();
    private boolean checkCert = true;
    private boolean authenticationEnabled = true;
    private URI uri;

    protected ClientBuilder(URI uri){
        this.uri = uri;
    }

    public ClientBuilder setAuthenticator(Authenticator authenticator) throws IllegalArgumentException {
        authenticatorOptional = Optional.ofNullable(authenticator);
        return this;
    }

    public ClientBuilder checkCert(boolean checkCert) {
        this.checkCert = checkCert;
        return this;
    }

    public ClientBuilder enableAuthentication(boolean authenticationEnabled) {
        this.authenticationEnabled = authenticationEnabled;
        return this;
    }
    public Client build() {
        if (authenticationEnabled) {
            if (authenticatorOptional.isPresent()) {
                return new Client(uri, authenticatorOptional, checkCert);
            } else {
                throw new IllegalArgumentException("When authentication is enabled, key and secret must be present.");
            }
        }
        return new Client(uri, Optional.empty(), checkCert);
    }
}
