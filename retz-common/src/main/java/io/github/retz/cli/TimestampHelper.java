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
package io.github.retz.cli;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TimestampHelper {
    // Use ISO8601-like extended format
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    public static String now() {
        synchronized (DATE_FORMAT) {
            return DATE_FORMAT.format(nowDate());
        }
    }

    public static String past(int seconds) {
        return past(Calendar.getInstance().getTime().getTime(), seconds);
    }

    // Returns duration in second
    public static long diffMillisec(String lhs, String rhs) throws ParseException {
        synchronized (DATE_FORMAT) {
            Date l = DATE_FORMAT.parse(lhs);
            Date r = DATE_FORMAT.parse(rhs);
            // Date#getTime returns timestamp since 1970 in milliseconds
            return l.getTime() - r.getTime();
        }
    }

    static Date nowDate() {
        return Calendar.getInstance().getTime();
    }

    static String now(Date date) {
        synchronized (DATE_FORMAT) {
            return DATE_FORMAT.format(date);
        }
    }

    static String past(long timestampMsec, int seconds) {
        synchronized (DATE_FORMAT) {
            return DATE_FORMAT.format(new Date(timestampMsec - seconds * 1000));
        }
    }
}
