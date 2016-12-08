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
package io.github.retz.protocol.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class User {
    private String keyId;
    private String secret;
    private boolean enabled;
    private String info;

    @JsonCreator
    public User(@JsonProperty(value = "key_id", required = true) String keyId,
                @JsonProperty(value = "secret", required = true) String secret,
                @JsonProperty(value = "enabled", required = true) boolean enabled,
                @JsonProperty(value = "info") String info) {
        this.keyId = Objects.requireNonNull(keyId);
        this.secret = Objects.requireNonNull(secret);
        this.enabled = enabled;
        this.info = info;
    }

    @JsonGetter("key_id")
    public String keyId(){
        return keyId;
    }

    @JsonGetter("secret")
    public String secret() {
        return secret;
    }

    @JsonGetter("enabled")
    public boolean enabled() {
        return enabled;
    }

    @JsonGetter("info")
    public String info() {
        return info;
    }
}
