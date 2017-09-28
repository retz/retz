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

public final class Protocol {
    // Protocol version x.y.z
    // if z does not match: a few API does not work
    // if y does not match: most API does not work
    // if x does not match: ALL API does not work
    public static final String PROTOCOL_VERSION = "0.2.1";

    private Protocol() {
        throw new UnsupportedOperationException();
    }
}
