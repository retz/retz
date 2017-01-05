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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.Objects;
import java.util.Optional;

public class GetFileRequest extends Request {
    private int id;
    private String file;
    private long offset;
    private long length;

    @JsonCreator
    public GetFileRequest(@JsonProperty(value = "id", required = true) int id,
                          @JsonProperty(value = "file", required = true) String file,
                          @JsonProperty(value = "offset") long offset,
                          @JsonProperty(value = "length") long length) {
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
    public long offset() {
        return offset;
    }

    @JsonGetter("length")
    public long length() {
        return length;
    }

    @Override
    public String resource() {
        String encodedFile = file;
        try {
            encodedFile = URLEncoder.encode(file, "UTF-8");
        } catch (UnsupportedEncodingException e) {
        }

        StringBuilder builder = new StringBuilder("/job/")
                .append(id)
                .append("/file")
                .append("?path=").append(encodedFile)
                .append("&offset=").append(offset)
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
        return "/job/:id/file";
    }
}
