/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.data;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests the conversion of a {@link TimeFieldSpec} to an equivalent {@link DateTimeFieldSpec}
 */
public class DateTimeFieldSpecUtilsTest {

  @Test
  public void testConversionFromTimeToDateTimeSpec() {
    TimeFieldSpec timeFieldSpec;
    DateTimeFieldSpec expectedDateTimeFieldSpec;
    DateTimeFieldSpec actualDateTimeFieldSpec;

    /* 1] only incoming */

    // incoming epoch millis
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "incoming"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("incoming", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // incoming epoch hours
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.INT, TimeUnit.HOURS, "incoming"));
    expectedDateTimeFieldSpec = new DateTimeFieldSpec("incoming", DataType.INT, "1:HOURS:EPOCH", "1:HOURS");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // Simple date format
    timeFieldSpec = new TimeFieldSpec(
        new TimeGranularitySpec(DataType.INT, TimeUnit.DAYS, "SIMPLE_DATE_FORMAT:yyyyMMdd", "incoming"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("incoming", DataType.INT, "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:DAYS");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // simple date format STRING
    timeFieldSpec = new TimeFieldSpec(
        new TimeGranularitySpec(DataType.STRING, TimeUnit.DAYS, "SIMPLE_DATE_FORMAT:yyyy-MM-dd hh-mm-ss", "incoming"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("incoming", DataType.STRING, "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd hh-mm-ss", "1:DAYS");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // time unit size
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, 5, TimeUnit.MINUTES, "incoming"));
    expectedDateTimeFieldSpec = new DateTimeFieldSpec("incoming", DataType.LONG, "5:MINUTES:EPOCH", "5:MINUTES");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // transform function
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.INT, TimeUnit.HOURS, "incoming"));
    timeFieldSpec.setTransformFunction("toEpochHours(timestamp)");
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("incoming", DataType.INT, "1:HOURS:EPOCH", "1:HOURS", null, "toEpochHours(timestamp)");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    /* 2] incoming + outgoing */

    // same incoming and outgoing
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "time"),
        new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "time"));
    expectedDateTimeFieldSpec = new DateTimeFieldSpec("time", DataType.LONG, "1:HOURS:EPOCH", "1:HOURS");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // same incoming and outgoing - simple date format
    timeFieldSpec =
        new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.DAYS, "SIMPLE_DATE_FORMAT:yyyyMMdd", "time"),
            new TimeGranularitySpec(DataType.LONG, TimeUnit.DAYS, "SIMPLE_DATE_FORMAT:yyyyMMdd", "time"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("time", DataType.LONG, "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:DAYS");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // millis to hours
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "incoming"),
        new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "outgoing"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("outgoing", DataType.LONG, "1:HOURS:EPOCH", "1:HOURS", null, "toEpochHours(incoming)");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // millis to bucketed minutes
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "incoming"),
        new TimeGranularitySpec(DataType.LONG, 10, TimeUnit.MINUTES, "outgoing"));
    expectedDateTimeFieldSpec = new DateTimeFieldSpec("outgoing", DataType.LONG, "10:MINUTES:EPOCH", "10:MINUTES", null,
        "toEpochMinutesBucket(incoming, 10)");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // days to millis
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.INT, TimeUnit.DAYS, "incoming"),
        new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "outgoing"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("outgoing", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS", null,
            "fromEpochDays(incoming)");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // bucketed minutes to millis
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, 5, TimeUnit.MINUTES, "incoming"),
        new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "outgoing"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("outgoing", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS", null,
            "fromEpochMinutesBucket(incoming, 5)");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // hours to days
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.INT, TimeUnit.HOURS, "incoming"),
        new TimeGranularitySpec(DataType.INT, TimeUnit.DAYS, "outgoing"));
    expectedDateTimeFieldSpec = new DateTimeFieldSpec("outgoing", DataType.INT, "1:DAYS:EPOCH", "1:DAYS", null,
        "toEpochDays(fromEpochHours(incoming))");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // minutes to hours
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.MINUTES, "incoming"),
        new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "outgoing"));
    expectedDateTimeFieldSpec = new DateTimeFieldSpec("outgoing", DataType.LONG, "1:HOURS:EPOCH", "1:HOURS", null,
        "toEpochHours(fromEpochMinutes(incoming))");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // bucketed minutes to days
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, 10, TimeUnit.MINUTES, "incoming"),
        new TimeGranularitySpec(DataType.LONG, TimeUnit.DAYS, "outgoing"));
    expectedDateTimeFieldSpec = new DateTimeFieldSpec("outgoing", DataType.LONG, "1:DAYS:EPOCH", "1:DAYS", null,
        "toEpochDays(fromEpochMinutesBucket(incoming, 10))");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // seconds to bucketed minutes
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.SECONDS, "incoming"),
        new TimeGranularitySpec(DataType.LONG, 5, TimeUnit.MINUTES, "outgoing"));
    expectedDateTimeFieldSpec = new DateTimeFieldSpec("outgoing", DataType.LONG, "5:MINUTES:EPOCH", "5:MINUTES", null,
        "toEpochMinutesBucket(fromEpochSeconds(incoming), 5)");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // simple date format to millis
    timeFieldSpec = new TimeFieldSpec(
        new TimeGranularitySpec(DataType.LONG, TimeUnit.DAYS, "SIMPLE_DATE_FORMAT:yyyyMMdd", "incoming"),
        new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "outgoing"));
    try {
      Schema.convertToDateTimeFieldSpec(timeFieldSpec);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }

    // hours to simple date format
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "incoming"),
        new TimeGranularitySpec(DataType.INT, TimeUnit.HOURS, "SIMPLE_DATE_FORMAT:yyyyMMddhh", "outgoing"));
    try {
      Schema.convertToDateTimeFieldSpec(timeFieldSpec);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }
  }

  @DataProvider(name = "schemaDataProvider")
  public Object[][] schemaDataProvider()
      throws IOException {
    List<Object[]> inputs = new ArrayList<>();
    inputs.addAll(constructSchemaObjects());
    inputs.addAll(fromSchemaFiles());
    return inputs.toArray(new Object[0][]);
  }

  private List<Object[]> constructSchemaObjects() {
    List<Object[]> inputs = new ArrayList<>();
    // 1] ------------- Schema w/ TimeFieldSpec w/o DateTimeFieldSpec
    // incoming only
    Schema pinotSchema1 = new Schema();
    pinotSchema1.addField(new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "incoming")));
    inputs.add(new Object[]{pinotSchema1, Lists.newArrayList(
        new DateTimeFieldSpec("incoming", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"))});

    // incoming + outgoing
    Schema pinotSchema2 = new Schema();
    pinotSchema2.addField(new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "incoming"),
        new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "outgoing")));
    DateTimeFieldSpec outgoing = new DateTimeFieldSpec("outgoing", DataType.LONG, "1:HOURS:EPOCH", "1:HOURS");
    outgoing.setTransformFunction("toEpochHours(incoming)");
    inputs.add(new Object[]{pinotSchema2, Lists.newArrayList(outgoing)});

    // 2] ---------- Schema w/o TimeFieldSpec w/ DateTimeFieldSpec
    // single
    Schema pinotSchema3 = new Schema();
    pinotSchema3.addField(new DateTimeFieldSpec("time", DataType.LONG, "1:MILLISECONDS:EPOCH", "15:MINUTES"));
    inputs.add(new Object[]{pinotSchema3, Lists.newArrayList(
        new DateTimeFieldSpec("time", DataType.LONG, "1:MILLISECONDS:EPOCH", "15:MINUTES"))});

    // multiple
    Schema pinotSchema4 = new Schema();
    pinotSchema4.addField(new DateTimeFieldSpec("time1", DataType.LONG, "1:MILLISECONDS:EPOCH", "15:MINUTES"));
    pinotSchema4.addField(
        new DateTimeFieldSpec("time2", DataType.STRING, "5:MINUTES:SIMPLE_DATE_FORMAT:EEE MMM dd HH:mm:ss z yyyy",
            "5:MINUTES"));
    inputs.add(new Object[]{pinotSchema4, Lists.newArrayList(
        new DateTimeFieldSpec("time1", DataType.LONG, "1:MILLISECONDS:EPOCH", "15:MINUTES"),
        new DateTimeFieldSpec("time2", DataType.STRING, "5:MINUTES:SIMPLE_DATE_FORMAT:EEE MMM dd HH:mm:ss z yyyy",
            "5:MINUTES"))});

    // 3] ------------- Schema w/ TimeFieldSpec w/ DateTimeFieldSpec
    // single
    Schema pinotSchema5 = new Schema();
    pinotSchema5
        .addField(new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "original_time")));
    pinotSchema5.addField(new DateTimeFieldSpec("new_time", DataType.LONG, "1:HOURS:EPOCH", "1:HOURS"));
    inputs.add(new Object[]{pinotSchema5, Lists.newArrayList(
        new DateTimeFieldSpec("original_time", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"),
        new DateTimeFieldSpec("new_time", DataType.LONG, "1:HOURS:EPOCH", "1:HOURS"))});

    // multiple
    Schema pinotSchema6 = new Schema();
    pinotSchema6
        .addField(new DateTimeFieldSpec("new_time1", DataType.LONG, "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:DAYS"));
    pinotSchema6
        .addField(new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "original_time")));
    DateTimeFieldSpec newTime2 = new DateTimeFieldSpec("new_time2", DataType.LONG, "1:HOURS:EPOCH", "1:HOURS");
    newTime2.setTransformFunction("toEpochHours(original_time)");
    pinotSchema6.addField(newTime2);
    DateTimeFieldSpec expectedNewTime2 = new DateTimeFieldSpec("new_time2", DataType.LONG, "1:HOURS:EPOCH", "1:HOURS");
    expectedNewTime2.setTransformFunction("toEpochHours(original_time)");
    inputs.add(new Object[]{pinotSchema6, Lists.newArrayList(
        new DateTimeFieldSpec("new_time1", DataType.LONG, "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:DAYS"),
        new DateTimeFieldSpec("original_time", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"),
        expectedNewTime2)});

    // 4] ----------- Schema w/o TimeFieldSpec w/o DateTimeFieldSpec
    Schema pinotSchema7 = new Schema();
    inputs.add(new Object[]{pinotSchema7, Lists.newArrayList()});

    return inputs;
  }

  private List<Object[]> fromSchemaFiles()
      throws IOException {
    List<Object[]> inputs = new ArrayList<>();

    // only timeFieldSpec, w/ incoming
    URL url = ClassLoader.getSystemResource("schema_read_test/schema_with_timefieldspec_only_incoming.json");
    Schema pinotSchema1 = Schema.fromFile(new File(url.getFile()));
    inputs.add(new Object[]{pinotSchema1, Lists.newArrayList(
        new DateTimeFieldSpec("time", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"))});

    // only timeFieldSpec w/ incoming + outgoing
    url = ClassLoader.getSystemResource("schema_read_test/schema_with_timefieldspec_only_incoming_outgoing.json");
    Schema pinotSchema2 = Schema.fromFile(new File(url.getFile()));
    DateTimeFieldSpec outgoing = new DateTimeFieldSpec("outgoing", DataType.LONG, "1:HOURS:EPOCH", "1:HOURS");
    outgoing.setTransformFunction("toEpochHours(incoming)");
    inputs.add(new Object[]{pinotSchema2, Lists.newArrayList(outgoing)});

    // only dateTimeFieldSpec
    url = ClassLoader.getSystemResource("schema_read_test/schema_with_datetimefieldspec_only.json");
    Schema pinotSchema3 = Schema.fromFile(new File(url.getFile()));
    DateTimeFieldSpec time2 = new DateTimeFieldSpec("time2", DataType.LONG, "1:SECONDS:EPOCH", "15:MINUTES");
    time2.setTransformFunction("toEpochSeconds(time1)");
    DateTimeFieldSpec time1 = new DateTimeFieldSpec("time1", DataType.LONG, "1:MILLISECONDS:EPOCH", "15:MINUTES");
    inputs.add(new Object[]{pinotSchema3, Lists.newArrayList(time2, time1)});

    // timeFieldSpec before dateTimeFieldSpec
    url = ClassLoader.getSystemResource("schema_read_test/schema_with_timefieldspec_and_datetimefieldspec.json");
    Schema pinotSchema4 = Schema.fromFile(new File(url.getFile()));
    DateTimeFieldSpec original =
        new DateTimeFieldSpec("original", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS");
    DateTimeFieldSpec newTime1 = new DateTimeFieldSpec("new_time_1", DataType.LONG, "1:HOURS:EPOCH", "1:HOURS");
    newTime1.setTransformFunction("toEpochHours(original)");
    DateTimeFieldSpec newTime2 =
        new DateTimeFieldSpec("new_time_2", DataType.STRING, "1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMddHH", "1:HOURS");
    inputs.add(new Object[]{pinotSchema4, Lists.newArrayList(original, newTime1, newTime2)});

    // dateTimeFieldSpec before timeFieldSpec
    url = ClassLoader.getSystemResource("schema_read_test/schema_with_datetimefieldspec_and_timefieldspec.json");
    Schema pinotSchema5 = Schema.fromFile(new File(url.getFile()));
    DateTimeFieldSpec outgoing1 = new DateTimeFieldSpec("outgoing", DataType.LONG, "1:SECONDS:EPOCH", "1:SECONDS");
    outgoing1.setTransformFunction("toEpochSeconds(incoming)");
    DateTimeFieldSpec newTime11 = new DateTimeFieldSpec("new_time_1", DataType.LONG, "1:HOURS:EPOCH", "1:HOURS");
    DateTimeFieldSpec newTime22 =
        new DateTimeFieldSpec("new_time_2", DataType.STRING, "1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMddHH", "1:HOURS");
    inputs.add(new Object[]{pinotSchema5, Lists.newArrayList(newTime11, newTime22, outgoing1)});

    return inputs;
  }

  @Test(dataProvider = "schemaDataProvider")
  public void testSchemaReadWrite(Schema schema, List<DateTimeFieldSpec> expectedDateTimeFieldSpecs) {
    // read the schema - shouldn't see timeFieldSpec
    Assert.assertEquals(schema.getDateTimeNames().size(), expectedDateTimeFieldSpecs.size());
    for (int i = 0; i < expectedDateTimeFieldSpecs.size(); i++) {
      DateTimeFieldSpec actualDateTimeFieldSpec = schema.getDateTimeFieldSpecs().get(i);
      DateTimeFieldSpec expectedDateTimeFieldSpec = expectedDateTimeFieldSpecs.get(i);
      Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);
      Assert.assertEquals(schema.getFieldSpecFor(actualDateTimeFieldSpec.getName()), expectedDateTimeFieldSpec);
      Assert.assertEquals(schema.getDateTimeSpec(actualDateTimeFieldSpec.getName()), expectedDateTimeFieldSpec);
    }
    // write the schema - shouldn't write timeFieldSpec
    ObjectNode schemaJson = schema.toJsonObject();
    Assert.assertNull(schemaJson.get("timeFieldSpec"));
    if (schema.getDateTimeNames().size() > 0) {
      Assert.assertNotNull(schemaJson.get("dateTimeFieldSpecs"));
    } else {
      Assert.assertNull(schemaJson.get("dateTimeFieldSpecs"));
    }

    String schemaString = schema.toSingleLineJsonString();
    Assert.assertFalse(schemaString.contains("timeFieldSpec"));
    if (schema.getDateTimeNames().size() > 0) {
      Assert.assertTrue(schemaString.contains("dateTimeFieldSpecs"));
    } else {
      Assert.assertFalse(schemaString.contains("dateTimeFieldSpecs"));
    }
  }
}
