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
package io.github.retz.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Authenticator {
    private static final Logger LOG = LoggerFactory.getLogger(Authenticator.class);

    public static final String AUTHORIZATION = "Authorization";

    private final String KEY;
    //private final String SECRET;
    private final SecretKeySpec SECRET_KEY_SPEC;

    private static final String REALM = "Retz-auth-v1";
    private static final String ALGORITHM = "HmacSHA256";

    public Authenticator(String key, String secret) {
        KEY = key;
        SECRET_KEY_SPEC = new SecretKeySpec(secret.getBytes(UTF_8), ALGORITHM);
    }

    // @var sign: signature value of authentication header, which should be provided as
    // Authorization: Retz-auth-v1 AccessKey:Signature
    //    Signature = Base64( HMAC-SHA1( YourAccessSecret, UTF-8-Encoding-Of( StringToSign ) ) );
    //
    // StringToSign = HTTP-Verb + "\n" +
    // Content-MD5 + "\n" +
    // Date + "\n" +
    // Resource;
    //
    // Resource = [ /job/<job-id> | /app/<appname> | /jobs | /apps | /u/<accesskey> | ... ]
    // Note that it does not require parameters like ?a=b, or #foobar
    public boolean authenticate(String verb, String md5, String date, String resource,
                                String key, String sign) {

        if (!KEY.equals(key)) {
            return false;
        }
        String generatedSignature = signature(verb, md5, date, resource);
        // TODO: check whether timestamp is within 10 minutes from now or not
        LOG.debug("Generated: {} ?= Given {}", generatedSignature, sign);
        return generatedSignature.equals(sign);
    }

    public String buildHeaderValue(String verb, String md5, String date, String resource) {
        String signature = signature(verb, md5, date, resource);
        return new StringBuilder()
                .append(REALM).append(" ")
                .append(KEY).append(":")
                .append(signature)
                .toString();
    }

    // Just do inverse of buildHeaderValue
    public static Optional<AuthHeaderValue> parseHeaderValue(String line) {
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
        AuthHeaderValue authHeaderValue = new AuthHeaderValue(b[0], b[1]);
        return Optional.of(authHeaderValue);
    }

    public String signature(String verb, String md5, String date, String resource) {
        String string2sign = string2sign(verb, md5, date, resource);
        LOG.debug("String2sign: {}", string2sign);
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(SECRET_KEY_SPEC);

            byte[] mac_bytes = mac.doFinal(string2sign.getBytes(UTF_8));

            return Base64.getEncoder().withoutPadding().encodeToString(mac_bytes);

        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError(ALGORITHM + " does not exist");
        } catch (InvalidKeyException e) {
            throw new AssertionError(SECRET_KEY_SPEC.getFormat() + " is wrong");
        }
    }

    public String string2sign(String verb, String md5, String date, String resource) {
        return new StringBuilder()
                .append(verb).append("\n")
                .append(md5).append("\n")
                .append(date).append("\n")
                .append(resource).append("\n")
                .toString();
    }

    public String getKey() {
        return KEY;
    }

    @Override
    public String toString() {
        return KEY + ":" + SECRET_KEY_SPEC.toString();
    }

    public static class AuthHeaderValue {
        public String key;
        public String signature;
        public AuthHeaderValue(String k, String s) {
            key = k;
            signature = s;
        }
        public String key() {
            return key;
        }
        public String signature() {
            return signature;
        }
    }
}
