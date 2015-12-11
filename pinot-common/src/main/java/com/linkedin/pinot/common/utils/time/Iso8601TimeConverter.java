/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.utils.time;

import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.linkedin.pinot.common.data.TimeGranularitySpec;

public class Iso8601TimeConverter implements TimeConverter {

  private static DateTimeFormatter parser = ISODateTimeFormat.dateTimeNoMillis();
  private TimeGranularitySpec incoming;
  private TimeGranularitySpec outgoing;

  public Iso8601TimeConverter(TimeGranularitySpec incoming, TimeGranularitySpec outgoing) {
    this.incoming = incoming;
    this.outgoing = outgoing;
  }

  @Override
  public Object convert(Object incomingTime) {
    long incomingInLong = parser.parseMillis(incomingTime.toString());
    switch (outgoing.getTimeType()) {
      case MILLISECONDS:
        return incomingInLong;
      case MICROSECONDS:
        return TimeUnit.MILLISECONDS.toMicros(incomingInLong);
      case SECONDS:
        return TimeUnit.MILLISECONDS.toSeconds(incomingInLong);
      case MINUTES:
        return TimeUnit.MILLISECONDS.toMinutes(incomingInLong);
      case HOURS:
        return TimeUnit.MILLISECONDS.toHours(incomingInLong);
      case DAYS:
        return TimeUnit.MILLISECONDS.toDays(incomingInLong);
      default:
        return -1;
    }
  }

  @Override
  public DateTime getDataTimeFrom(Object o) {
    long incoming = -1;
    if (o instanceof Integer) {
      incoming = ((Integer) o).longValue();
    } else {
      incoming = (Long) o;
    }
    switch (outgoing.getTimeType()) { 
      case MILLISECONDS:
        return new DateTime(incoming);
      case MICROSECONDS:
        long millisFromMicro = TimeUnit.MICROSECONDS.toMillis(incoming);
        return new DateTime(millisFromMicro);
      case SECONDS:
        long millisFromSec = TimeUnit.SECONDS.toMillis(incoming);
        return new DateTime(millisFromSec);
      case MINUTES:
        long millisFromMinutes = TimeUnit.MINUTES.toMillis(incoming);
        return new DateTime(millisFromMinutes);
      case HOURS:
        long millisFromHours = TimeUnit.HOURS.toMillis(incoming);
        return new DateTime(millisFromHours);
      case DAYS:
        long millisFromDays = TimeUnit.DAYS.toMillis(incoming);
        return new DateTime(millisFromDays);
      default:
        return null;
    }
  }
}
