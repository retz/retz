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
import java.security.Signature;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

public class HmacSHA256Authenticator implements Authenticator {
    private static final Logger LOG = LoggerFactory.getLogger(HmacSHA256Authenticator.class);

    private static final String REALM = "Retz-auth-v1";
    private static final String ALGORITHM = "HmacSHA256";

    static Mac mac = null;

    private final String KEY;
    private final SecretKeySpec SECRET_KEY_SPEC;

    static {
        try {
            Date start = Calendar.getInstance().getTime();
            mac = Mac.getInstance(ALGORITHM);
            Date end = Calendar.getInstance().getTime();
            LOG.info("javax.crypto.Mac instance with {} initialized in {} ms.",
                    ALGORITHM, end.getTime() - start.getTime());
        } catch (NoSuchAlgorithmException e) {
            LOG.error(e.toString(), e);
            System.exit(-1);
        }
    }
    public HmacSHA256Authenticator(String key, String secret) {
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

    public String signature(String verb, String md5, String date, String resource) {
        String string2sign = string2sign(verb, md5, date, resource);
        LOG.debug("String2sign: {}", string2sign);
        try {
            if (HmacSHA256Authenticator.mac == null) {
                throw new RuntimeException("Authenticator is null");
            }
            // TODO: when this becomes bottleneck at server side, make this TLS
            synchronized (HmacSHA256Authenticator.mac) {
                HmacSHA256Authenticator.mac.init(SECRET_KEY_SPEC);
                byte[] mac_bytes = mac.doFinal(string2sign.getBytes(UTF_8));
                return Base64.getEncoder().withoutPadding().encodeToString(mac_bytes);
            }
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

    @Override
    public String getKey() {
        return KEY;
    }

    @Override
    public String toString() {
        return KEY + ":" + SECRET_KEY_SPEC.toString();
    }

    public AuthHeader header(String verb, String md5, String date, String resource) {
        String signature = signature(verb, md5, date, resource);
        return new AuthHeader(KEY, signature);
    }
}
