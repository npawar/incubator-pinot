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
package org.apache.pinot.common.data;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests for TimeFieldSpec to DateTimeFieldSpec conversion
 */
public class TimeToDateTimeFieldSpecTest {
  private File baseDir;

  @BeforeClass
  public void setup()
      throws IOException {
    baseDir = new File(FileUtils.getTempDirectory(), "timeFieldSpecDeserTest");
    FileUtils.deleteDirectory(baseDir);
  }

  /**
   * Tests the conversion of a {@link TimeFieldSpec} to an equivalent {@link DateTimeFieldSpec}
   */
  @Test
  public void testConversionFromTimeToDateTimeSpec() {
    TimeFieldSpec timeFieldSpec;
    DateTimeFieldSpec expectedDateTimeFieldSpec;
    DateTimeFieldSpec actualDateTimeFieldSpec;

    /* 1] only incoming */

    // incoming epoch millis
    timeFieldSpec =
        new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "incoming"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("incoming", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // incoming epoch hours
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.HOURS, "incoming"));
    expectedDateTimeFieldSpec = new DateTimeFieldSpec("incoming", FieldSpec.DataType.INT, "1:HOURS:EPOCH", "1:HOURS");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // Simple date format
    timeFieldSpec = new TimeFieldSpec(
        new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "SIMPLE_DATE_FORMAT:yyyyMMdd", "incoming"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("incoming", FieldSpec.DataType.INT, "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:DAYS");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // simple date format STRING
    timeFieldSpec = new TimeFieldSpec(
        new TimeGranularitySpec(FieldSpec.DataType.STRING, TimeUnit.DAYS, "SIMPLE_DATE_FORMAT:yyyy-MM-dd hh-mm-ss",
            "incoming"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("incoming", FieldSpec.DataType.STRING, "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd hh-mm-ss",
            "1:DAYS");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // time unit size
    timeFieldSpec =
        new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.LONG, 5, TimeUnit.MINUTES, "incoming"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("incoming", FieldSpec.DataType.LONG, "5:MINUTES:EPOCH", "5:MINUTES");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // transform function
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.HOURS, "incoming"));
    timeFieldSpec.setTransformFunction("toEpochHours(timestamp)");
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("incoming", FieldSpec.DataType.INT, "1:HOURS:EPOCH", "1:HOURS", null,
            "toEpochHours(timestamp)");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    /* 2] incoming + outgoing */

    // same incoming and outgoing
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.HOURS, "time"),
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.HOURS, "time"));
    expectedDateTimeFieldSpec = new DateTimeFieldSpec("time", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // same incoming and outgoing - simple date format
    timeFieldSpec = new TimeFieldSpec(
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.DAYS, "SIMPLE_DATE_FORMAT:yyyyMMdd", "time"),
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.DAYS, "SIMPLE_DATE_FORMAT:yyyyMMdd", "time"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("time", FieldSpec.DataType.LONG, "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:DAYS");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // millis to hours
    timeFieldSpec =
        new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "incoming"),
            new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.HOURS, "outgoing"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("outgoing", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS", null,
            "toEpochHours(incoming)");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // millis to bucketed minutes
    timeFieldSpec =
        new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "incoming"),
            new TimeGranularitySpec(FieldSpec.DataType.LONG, 10, TimeUnit.MINUTES, "outgoing"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("outgoing", FieldSpec.DataType.LONG, "10:MINUTES:EPOCH", "10:MINUTES", null,
            "toEpochMinutesBucket(incoming, 10)");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // days to millis
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "incoming"),
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "outgoing"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("outgoing", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS", null,
            "fromEpochDays(incoming)");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // bucketed minutes to millis
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.LONG, 5, TimeUnit.MINUTES, "incoming"),
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "outgoing"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("outgoing", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS", null,
            "fromEpochMinutesBucket(incoming, 5)");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // hours to days
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.HOURS, "incoming"),
        new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "outgoing"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("outgoing", FieldSpec.DataType.INT, "1:DAYS:EPOCH", "1:DAYS", null,
            "toEpochDays(fromEpochHours(incoming))");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // minutes to hours
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MINUTES, "incoming"),
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.HOURS, "outgoing"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("outgoing", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS", null,
            "toEpochHours(fromEpochMinutes(incoming))");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // bucketed minutes to days
    timeFieldSpec =
        new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.LONG, 10, TimeUnit.MINUTES, "incoming"),
            new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.DAYS, "outgoing"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("outgoing", FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS", null,
            "toEpochDays(fromEpochMinutesBucket(incoming, 10))");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // seconds to bucketed minutes
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.SECONDS, "incoming"),
        new TimeGranularitySpec(FieldSpec.DataType.LONG, 5, TimeUnit.MINUTES, "outgoing"));
    expectedDateTimeFieldSpec =
        new DateTimeFieldSpec("outgoing", FieldSpec.DataType.LONG, "5:MINUTES:EPOCH", "5:MINUTES", null,
            "toEpochMinutesBucket(fromEpochSeconds(incoming), 5)");
    actualDateTimeFieldSpec = Schema.convertToDateTimeFieldSpec(timeFieldSpec);
    Assert.assertEquals(actualDateTimeFieldSpec, expectedDateTimeFieldSpec);

    // simple date format to millis
    timeFieldSpec = new TimeFieldSpec(
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.DAYS, "SIMPLE_DATE_FORMAT:yyyyMMdd", "incoming"),
        new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "outgoing"));
    try {
      Schema.convertToDateTimeFieldSpec(timeFieldSpec);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }

    // hours to simple date format
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.HOURS, "incoming"),
        new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.HOURS, "SIMPLE_DATE_FORMAT:yyyyMMddhh", "outgoing"));
    try {
      Schema.convertToDateTimeFieldSpec(timeFieldSpec);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }
  }

  /**
   * Tests that schema deserialization treats TimeFieldSpec as a DateTimeFieldSpec
   */
  @Test(dataProvider = "schemaDeserializationDataProvider")
  public void testTimeFieldSpecDeserialization(String schemaString, List<DateTimeFieldSpec> expectedDateTimeFieldSpecs)
      throws IOException {

    File schemaFile = new File(baseDir, "schemaTest");
    FileUtils.deleteQuietly(schemaFile);
    FileUtils.writeStringToFile(schemaFile, schemaString);
    InputStream schemaStream = new ByteArrayInputStream(schemaString.getBytes());
    // deserialize the schema input
    List<Schema> schemas = Lists.newArrayList(Schema.fromString(schemaString), Schema.fromFile(schemaFile),
        Schema.fromInputSteam(schemaStream));
    FileUtils.deleteQuietly(schemaFile);

    for (Schema schema : schemas) {
      Assert.assertEquals(schema.getDateTimeFieldSpecs().size(), expectedDateTimeFieldSpecs.size());
      for (int i = 0; i < expectedDateTimeFieldSpecs.size(); i++) {
        DateTimeFieldSpec expectedDateTimeSpec = expectedDateTimeFieldSpecs.get(i);
        DateTimeFieldSpec actualDateTimeSpec = schema.getDateTimeFieldSpecs().get(i);
        Assert.assertEquals(actualDateTimeSpec, expectedDateTimeSpec);
        Assert.assertEquals(schema.getFieldSpecMap().get(actualDateTimeSpec.getName()), actualDateTimeSpec);
        Assert.assertEquals(schema.getDateTimeNames().get(i), actualDateTimeSpec.getName());
        Assert.assertEquals(schema.getDateTimeSpec(actualDateTimeSpec.getName()), actualDateTimeSpec);
        Assert.assertEquals(schema.getFieldSpecFor(actualDateTimeSpec.getName()), actualDateTimeSpec);
        Assert.assertEquals(schema.getSpecForTimeColumn(actualDateTimeSpec.getName()), actualDateTimeSpec);
      }

      // serialize the Schema
      ObjectNode schemaJson = schema.toJsonObject();
      Assert.assertNull(schemaJson.get("timeFieldSpec"));
      Assert.assertNotNull(schemaJson.get("dateTimeFieldSpecs"));
      String newSchemaString = schema.toSingleLineJsonString();
      Assert.assertFalse(newSchemaString.contains("timeFieldSpec"));
      Assert.assertTrue(newSchemaString.contains("dateTimeFieldSpecs"));
      newSchemaString = schema.toPrettyJsonString();
      Assert.assertFalse(newSchemaString.contains("timeFieldSpec"));
      Assert.assertTrue(newSchemaString.contains("dateTimeFieldSpecs"));
    }
  }

  /**
   Testing following schemas
   * 1. contains only TimeFieldSpec with incomingGranularitySpec
   * 2. contains only TimeFieldSpec with incoming and outgoingGranularitySpec
   * 3. contains only 1 DateTimeFieldSpec
   * 4. contains 1 DateTimeFieldSpec and 1 TimeFieldSpec in that order
   * 5. contains 1 TimeFieldSpec and 1 DateTimeFieldSpec in that order
   * 6. contains only multiple DateTimeFieldSpec
   * 7. contains 1 TimeFieldSpec and multiple DateTimeFieldSpec in that order
   * 8. contains multiple DateTimeFieldSpec and 1 TimeFieldSpec in that order
   * 9. repeat column name in TimeFieldSpec and DateTimeFieldSpec
   */
  @DataProvider(name = "schemaDeserializationDataProvider")
  public Object[][] schemaDeserializationDataProvider() {
    List<Object[]> inputs = new ArrayList<>();

    // 1. only TimeFieldSpec (only incoming)
    String schemaString =
        "{\"schemaName\":\"mySchema\",\"dimensionFieldSpecs\":[{\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"timeFieldSpec\":{\"incomingGranularitySpec\":{\"dataType\":\"LONG\",\"timeType\":\"MILLISECONDS\",\"name\":\"millisSinceEpoch\"}}}";
    inputs.add(new Object[]{schemaString, Lists.newArrayList(
        new DateTimeFieldSpec("millisSinceEpoch", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"))});

    // 2. only TimeFieldSpec (incoming + outgoing)
    schemaString =
        "{\"schemaName\":\"mySchema\"," + "\"dimensionFieldSpecs\":[{\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
            + "\"timeFieldSpec\":{\"incomingGranularitySpec\":{\"dataType\":\"LONG\",\"timeType\":\"MILLISECONDS\",\"name\":\"millisSinceEpoch\"},"
            + "\"outgoingGranularitySpec\":{\"dataType\":\"LONG\",\"timeType\":\"HOURS\",\"name\":\"hoursSinceEpoch\"}}}";
    DateTimeFieldSpec dt1 =
        new DateTimeFieldSpec("hoursSinceEpoch", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS");
    dt1.setTransformFunction("toEpochHours(millisSinceEpoch)");
    inputs.add(new Object[]{schemaString, Lists.newArrayList(dt1)});

    // 3. only 1 DateTimeFieldSpec
    schemaString = "{\"schemaName\":\"mySchema\",\"dimensionFieldSpecs\":[{\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
        + "\"dateTimeFieldSpecs\":[{\"name\":\"secondsSinceEpoch\",\"dataType\":\"LONG\",\"format\":\"1:SECONDS:EPOCH\",\"granularity\":\"15:MINUTES\",\"transformFunction\":\"toEpochSeconds(millisSinceEpoch)\"}]}";
    DateTimeFieldSpec dt2 =
        new DateTimeFieldSpec("secondsSinceEpoch", FieldSpec.DataType.LONG, "1:SECONDS:EPOCH", "15:MINUTES");
    dt2.setTransformFunction("toEpochSeconds(millisSinceEpoch)");
    inputs.add(new Object[]{schemaString, Lists.newArrayList(dt2)});

    // 4. 1 DateTimeFieldSpec 1 TimeFieldSpec
    schemaString = "{\"schemaName\":\"mySchema\",\"dimensionFieldSpecs\":[{\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
        + "\"dateTimeFieldSpecs\":[{\"name\":\"secondsSinceEpoch\",\"dataType\":\"LONG\",\"format\":\"1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMddHH\",\"granularity\":\"1:HOURS\",\"transformFunction\":\"toDateTime(incoming,'yyyyMMddHH')\"}],"
        + "\"timeFieldSpec\":{\"incomingGranularitySpec\":{\"dataType\":\"LONG\",\"timeType\":\"MILLISECONDS\",\"name\":\"incoming\"},"
        + "\"outgoingGranularitySpec\":{\"dataType\":\"LONG\",\"timeType\":\"SECONDS\",\"name\":\"outgoing\"}}}";
    DateTimeFieldSpec dt3 =
        new DateTimeFieldSpec("secondsSinceEpoch", FieldSpec.DataType.LONG, "1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMddHH",
            "1:HOURS");
    dt3.setTransformFunction("toDateTime(incoming,'yyyyMMddHH')");
    DateTimeFieldSpec dt4 = new DateTimeFieldSpec("outgoing", FieldSpec.DataType.LONG, "1:SECONDS:EPOCH", "1:SECONDS");
    dt4.setTransformFunction("toEpochSeconds(incoming)");
    inputs.add(new Object[]{schemaString, Lists.newArrayList(dt3, dt4)});

    // 5. 1 TimeFieldSpec 1 DateTimeFieldSpec
    schemaString = "{\"schemaName\":\"mySchema\",\"dimensionFieldSpecs\":[{\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
        + "\"timeFieldSpec\":{\"incomingGranularitySpec\":{\"dataType\":\"LONG\",\"timeType\":\"MILLISECONDS\",\"name\":\"incoming\"}},"
        + "\"dateTimeFieldSpecs\":[{\"name\":\"millisSinceEpoch\",\"dataType\":\"LONG\",\"format\":\"1:MILLISECONDS:EPOCH\",\"granularity\":\"1:HOURS\"}]}";
    inputs.add(new Object[]{schemaString, Lists.newArrayList(
        new DateTimeFieldSpec("incoming", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"),
        new DateTimeFieldSpec("millisSinceEpoch", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS"))});

    // 6. multiple DateTimeFieldSpec
    schemaString = "{\"schemaName\":\"mySchema\",\"dimensionFieldSpecs\":[{\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
        + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"LONG\",\"format\":\"1:HOURS:EPOCH\",\"granularity\":\"1:HOURS\"},"
        + "{\"name\":\"dt2\",\"dataType\":\"STRING\",\"format\":\"1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMddHH\",\"granularity\":\"1:HOURS\"}]}";
    inputs.add(new Object[]{schemaString, Lists.newArrayList(
        new DateTimeFieldSpec("dt1", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS"),
        new DateTimeFieldSpec("dt2", FieldSpec.DataType.STRING, "1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMddHH", "1:HOURS"))});

    // 7. 1 TimeFieldSpec multiple DateTimeFieldSpec
    schemaString = "{\"schemaName\":\"mySchema\",\"dimensionFieldSpecs\":[{\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
        + "\"timeFieldSpec\":{\"incomingGranularitySpec\":{\"dataType\":\"LONG\",\"timeType\":\"MILLISECONDS\",\"name\":\"incoming\"},"
        + "\"outgoingGranularitySpec\":{\"dataType\":\"LONG\",\"timeType\":\"SECONDS\",\"name\":\"outgoing\"}},"
        + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"LONG\",\"format\":\"1:HOURS:EPOCH\",\"granularity\":\"1:HOURS\"},"
        + "{\"name\":\"dt2\",\"dataType\":\"STRING\",\"format\":\"1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMddHH\",\"granularity\":\"1:HOURS\"}]}";
    DateTimeFieldSpec dt5 = new DateTimeFieldSpec("outgoing", FieldSpec.DataType.LONG, "1:SECONDS:EPOCH", "1:SECONDS");
    dt5.setTransformFunction("toEpochSeconds(incoming)");
    inputs.add(new Object[]{schemaString, Lists.newArrayList(dt5,
        new DateTimeFieldSpec("dt1", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS"),
        new DateTimeFieldSpec("dt2", FieldSpec.DataType.STRING, "1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMddHH", "1:HOURS"))});

    // 8. multiple DateTimeFieldSpec 1 TimeFieldSpec
    schemaString = "{\"schemaName\":\"mySchema\",\"dimensionFieldSpecs\":[{\"name\":\"dim1\",\"dataType\":\"STRING\"}],"
        + "\"dateTimeFieldSpecs\":[{\"name\":\"dt1\",\"dataType\":\"LONG\",\"format\":\"1:HOURS:EPOCH\",\"granularity\":\"1:HOURS\"},"
        + "{\"name\":\"dt2\",\"dataType\":\"STRING\",\"format\":\"1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMddHH\",\"granularity\":\"1:HOURS\"}],"
        + "\"timeFieldSpec\":{\"incomingGranularitySpec\":{\"dataType\":\"LONG\",\"timeType\":\"MILLISECONDS\",\"name\":\"incoming\"}}}";
    inputs.add(new Object[]{schemaString, Lists.newArrayList(
        new DateTimeFieldSpec("dt1", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS"),
        new DateTimeFieldSpec("dt2", FieldSpec.DataType.STRING, "1:HOURS:SIMPLE_DATE_FORMAT:yyyyMMddHH", "1:HOURS"),
        new DateTimeFieldSpec("incoming", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"))});

    return inputs.toArray(new Object[0][]);
  }

  public void testTimeFieldSpecSerialization() {

  }

  @AfterClass
  public void cleanup() {
    FileUtils.deleteQuietly(baseDir);
  }
}
