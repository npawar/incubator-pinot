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
package org.apache.pinot.plugin.segmentwriter.filebased;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.plugin.inputformat.genericrow.GenericRowRecordReader;
import org.apache.pinot.plugin.segmentwriter.common.SegmentWriterConstants;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.ingestion.segment.writer.SegmentWriter;


public class SegmentWriterTester {

  public static void main(String[] args)
      throws IOException, URISyntaxException {
    SegmentWriter writer = new FileBasedSegmentWriter();
    Map<String, Object> props = new HashMap<>();
    props.put(SegmentWriterConstants.CONTROLLER_URI_PROP, "http://localhost:9000");
    props.put(SegmentWriterConstants.TABLE_NAME_WITH_TYPE_PROP, "segmentWriter");
    PinotConfiguration conf = new PinotConfiguration(props);
    writer.init(conf);

    GenericRow row1 = getGenericRow("foo");
    GenericRow row2 = getGenericRow("foobar");
    writer.collect(row1);
    writer.collect(row2);
    writer.flush();
    writer.close();
  }


  private static GenericRow getGenericRow(String aString) {
    GenericRow row = new GenericRow();
    row.putValue("aString", aString);
    row.putValue("anInt", 100);
    row.putValue("aLong", 1614297600000L);
    row.putValue("aDouble", 10.5);
    row.putValue("aFloat", 2.0);
    row.putValue("aBoolean", true);
    row.putValue("aBytes", "foo".getBytes(StandardCharsets.UTF_8));
    List<String> stringList = new ArrayList<>();
    stringList.add("a");
    stringList.add("b");
    row.putValue("aStringList", stringList);
    List<Integer> intList = new ArrayList<>();
    intList.add(100);
    intList.add(200);
    row.putValue("anIntList", intList);
    row.putValue("aStringArray", new String[]{"x", "y", null});
    row.putValue("aDoubleArray", new Double[]{0.4, 0.5});
    Map<String, Object> simpleMap = new HashMap<>();
    simpleMap.put("name", "Mr. Foo");
    simpleMap.put("age", 100);
    simpleMap.put("phoneNumber", 9090909090L);
    row.putValue("aSimpleMap", simpleMap);
    Map<String, Object> advancedMap = new HashMap<>();
    advancedMap.put("list", Lists.newArrayList("p", "q", "r"));
    advancedMap.put("map", simpleMap);
    row.putValue("anAdvancedMap", advancedMap);
    row.putValue("nullString", null);

    return row;
  }
}


