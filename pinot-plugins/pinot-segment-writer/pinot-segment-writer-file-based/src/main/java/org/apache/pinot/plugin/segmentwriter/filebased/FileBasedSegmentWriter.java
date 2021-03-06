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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.segment.generation.SegmentGenerationUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.util.FileIngestionUtils;
import org.apache.pinot.plugin.inputformat.genericrow.GenericRowRecordReader;
import org.apache.pinot.plugin.segmentwriter.common.SegmentWriterConstants;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.ingestion.segment.writer.SegmentWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link SegmentWriter} implementation that uses a local file as a buffer to collect {@link GenericRow}.
 * The {@link GenericRow} are written to the buffer using default object serde.
 * The segment generation process uses {@link GenericRowRecordReader} to read the objects from the buffer file.
 */
public class FileBasedSegmentWriter implements SegmentWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedSegmentWriter.class);

  private TableConfig _tableConfig;
  private String _tableNameWithType;
  private Schema _schema;
  private String _controllerHost;
  private int _controllerPort;

  private File _stagingDir;
  private File _bufferFile;
  private ObjectOutputStream _genericRowOutputStream;

  @Override
  public void init(PinotConfiguration conf)
      throws IOException, URISyntaxException {
    String controllerURIStr = conf.getProperty(SegmentWriterConstants.CONTROLLER_URI_PROP);
    Preconditions.checkState(StringUtils.isNotBlank(controllerURIStr));
    URI controllerURI = new URI(controllerURIStr);
    _controllerHost = controllerURI.getHost();
    _controllerPort = controllerURI.getPort();

    String tableNameWithType = conf.getProperty(SegmentWriterConstants.TABLE_NAME_WITH_TYPE_PROP);
    Preconditions.checkState(StringUtils.isNotBlank(tableNameWithType));
    String tableConfigURI = SegmentGenerationUtils.generateTableConfigURI(controllerURIStr, tableNameWithType);
    _tableConfig = SegmentGenerationUtils.getTableConfig(tableConfigURI);
    _tableNameWithType = _tableConfig.getTableName();

    String schemaURI = SegmentGenerationUtils.generateSchemaURI(controllerURIStr, tableNameWithType);
    _schema = SegmentGenerationUtils.getSchema(schemaURI);

    // create tmp dir
    _stagingDir = new File(FileUtils.getTempDirectory(),
        String.format("segment_writer_staging_%s_%d", tableNameWithType, System.currentTimeMillis()));
    Preconditions.checkState(_stagingDir.mkdirs(), "Failed to create staging dir: %s", _stagingDir.getAbsolutePath());

    // create buffer file
    File bufferDir = new File(_stagingDir, "buffer_dir");
    Preconditions.checkState(bufferDir.mkdirs(), "Failed to create buffer_dir: %s", bufferDir.getAbsolutePath());
    _bufferFile = new File(bufferDir, "buffer_file");
    resetBuffer();
  }

  private void resetBuffer()
      throws IOException {
    FileUtils.deleteQuietly(_bufferFile);
    _genericRowOutputStream = new ObjectOutputStream(new FileOutputStream(_bufferFile));
  }

  @Override
  public void collect(GenericRow row)
      throws IOException {
    _genericRowOutputStream.writeObject(row);
  }

  @Override
  public void collect(GenericRow[] rowBatch)
      throws IOException {
    for (GenericRow row : rowBatch) {
      collect(row);
    }
  }

  @Override
  public void flush()
      throws IOException {
    _genericRowOutputStream.close();

    File flushDir = new File(_stagingDir, "flush_dir_" + System.currentTimeMillis());
    Preconditions.checkState(flushDir.mkdirs(), "Failed to create flush dir: %s", flushDir);

    try {
      File segmentDir = new File(flushDir, "segment_dir");
      SegmentGeneratorConfig segmentGeneratorConfig = FileIngestionUtils
          .generateSegmentGeneratorConfig(_tableConfig, FileFormat.GENERIC_ROW, _schema, _bufferFile, segmentDir);
      String segmentName = FileIngestionUtils.buildSegment(segmentGeneratorConfig);

      File segmentTarDir = new File(flushDir, "segment_tar_dir");
      Preconditions.checkState(segmentTarDir.mkdirs(), "Failed to create dir for segment tar: %s",
          segmentTarDir.getAbsolutePath());
      File segmentTarFile =
          new File(segmentTarDir, segmentName + org.apache.pinot.spi.ingestion.batch.spec.Constants.TAR_GZ_FILE_EXT);
      TarGzCompressionUtils.createTarGzFile(new File(segmentDir, segmentName), segmentTarFile);
      FileIngestionUtils
          .uploadSegment(_tableNameWithType, Lists.newArrayList(segmentTarFile), _controllerHost, _controllerPort);
      LOGGER.info("Uploaded tar: {} to {}:{}", segmentTarFile.getAbsolutePath(), _controllerHost, _controllerPort);
    } catch (Exception e) {
      throw new RuntimeException(String
          .format("Caught exception while generating segment from buffer file: %s for table:%s",
              _bufferFile.getAbsolutePath(), _tableConfig.getTableName()), e);
    } finally {
      FileUtils.deleteQuietly(flushDir);
    }

    resetBuffer();
  }

  @Override
  public void close()
      throws IOException {
    _genericRowOutputStream.close();
    FileUtils.deleteQuietly(_stagingDir);
  }
}
