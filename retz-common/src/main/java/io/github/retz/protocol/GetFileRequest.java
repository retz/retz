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
package io.github.retz.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

public class GetFileRequest extends Request {
    private int id;
    private String file;
    private int offset;
    private int length;

    @JsonCreator
    public GetFileRequest(@JsonProperty(value = "id", required = true) int id,
                          @JsonProperty(value = "file", required = true) String file,
                          @JsonProperty(value = "offset") int offset,
                          @JsonProperty(value = "length") int length) {
        this.id = id;
        this.file = Objects.requireNonNull(file);
        this.offset = offset;
        this.length = length;
    }

    @JsonGetter("id")
    public int id() {
        return id;
    }

    @JsonGetter("file")
    public String file() {
        return file;
    }

    @JsonGetter("offset")
    public int offset() {
        return offset;
    }

    @JsonGetter("length")
    public int length() {
        return length;
    }

    @Override
    public String resource() {
        StringBuilder builder = new StringBuilder("/job/")
                .append(id)
                .append("/file/").append(file)
                .append("?offset=").append(offset)
                .append("&length=").append(length);

        return builder.toString();
    }

    @Override
    public String method() {
        return GET;
    }

    @Override
    public boolean hasPayload() {
        return false;
    }

    public static String resourcePattern() {
        return "/job/:id/file/:file";
    }
}
