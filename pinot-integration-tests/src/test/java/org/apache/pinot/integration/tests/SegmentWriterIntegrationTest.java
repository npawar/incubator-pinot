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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Function;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordReader;
import org.apache.pinot.plugin.segmentwriter.common.SegmentWriterConstants;
import org.apache.pinot.plugin.segmentwriter.filebased.FileBasedSegmentWriter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.ingestion.segment.writer.SegmentWriter;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests creating segments via the {@link SegmentWriter} implementations
 */
public class SegmentWriterIntegrationTest extends BaseClusterIntegrationTest {

  private String _tableNameWithType;
  private PinotConfiguration _segmentWriterConf;
  private List<File> _avroFiles;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Create and upload the schema
    Schema schema = createSchema();
    addSchema(schema);
    _tableNameWithType = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(getTableName());

    // Segment writer props
    Map<String, Object> segmentWriterProps = new HashMap<>();
    segmentWriterProps.put(SegmentWriterConstants.CONTROLLER_URI_PROP, _controllerBaseApiUrl);
    segmentWriterProps.put(SegmentWriterConstants.TABLE_NAME_WITH_TYPE_PROP, _tableNameWithType);
    _segmentWriterConf = new PinotConfiguration(segmentWriterProps);

    // Get avro files
    _avroFiles = getAllAvroFiles();
  }

  @BeforeMethod
  public void setupTable()
      throws IOException {
    TableConfig offlineTableConfig = createOfflineTableConfig();
    addTableConfig(offlineTableConfig);
  }

  @AfterMethod
  public void teardownTable()
      throws IOException {
    dropOfflineTable(_tableNameWithType);
  }

  /**
   * Write the records from 3 avro files into the Pinot table using the {@link FileBasedSegmentWriter}
   * Calls {@link SegmentWriter#flush()} after writing records from each avro file
   * Checks the number of segments created, number of docs in each segment, and total docs fro the query
   */
  @Test
  public void testFileBasedSegmentWriter()
      throws Exception {

    SegmentWriter segmentWriter = new FileBasedSegmentWriter();
    segmentWriter.init(_segmentWriterConf);

    GenericRow reuse = new GenericRow();
    long totalDocs = 0;
    for (int i = 0; i < 3; i++) {
      AvroRecordReader avroRecordReader = new AvroRecordReader();
      avroRecordReader.init(_avroFiles.get(i), null, null);

      long numDocsInSegment = 0;
      while (avroRecordReader.hasNext()) {
        avroRecordReader.next(reuse);
        segmentWriter.collect(reuse);
        numDocsInSegment++;
        totalDocs++;
      }
      segmentWriter.flush();

      // check num segments
      Assert.assertEquals(getNumSegments(), i + 1);
      // check numDocs in latest segment
      Assert.assertEquals(getNumDocsInLatestSegment(), numDocsInSegment);
      // check totalDocs in query
      final long expectedDocs = totalDocs;
      TestUtils.waitForCondition(new Function<Void, Boolean>() {
        @Nullable
        @Override
        public Boolean apply(@Nullable Void aVoid) {
          try {
            return getTotalDocsFromQuery() == expectedDocs;
          } catch (Exception e) {
            return null;
          }
        }
      }, 100L, 120_000, "Failed to load " + expectedDocs + " documents", true);
    }

    segmentWriter.close();
  }

  private int getNumSegments()
      throws IOException {
    String jsonOutputStr = sendGetRequest(_controllerRequestURLBuilder.
        forSegmentListAPIWithTableType(_tableNameWithType, TableType.OFFLINE.toString()));
    JsonNode array = JsonUtils.stringToJsonNode(jsonOutputStr);
    return array.get(0).get("OFFLINE").size();
  }

  private int getNumDocsInLatestSegment()
      throws IOException {
    String jsonOutputStr = sendGetRequest(_controllerRequestURLBuilder.
        forSegmentListAPIWithTableType(_tableNameWithType, TableType.OFFLINE.toString()));
    JsonNode array = JsonUtils.stringToJsonNode(jsonOutputStr);
    JsonNode segments = array.get(0).get("OFFLINE");
    String segmentName = segments.get(segments.size() - 1).asText();

    jsonOutputStr = sendGetRequest(_controllerRequestURLBuilder.
        forSegmentMetadata(_tableNameWithType, segmentName));
    JsonNode metadata = JsonUtils.stringToJsonNode(jsonOutputStr);
    return metadata.get("segment.total.docs").asInt();
  }

  private int getTotalDocsFromQuery()
      throws Exception {
    JsonNode response = postSqlQuery(String.format("select count(*) from %s", _tableNameWithType), _brokerBaseApiUrl);
    return response.get("resultTable").get("rows").get(0).get(0).asInt();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }
}
