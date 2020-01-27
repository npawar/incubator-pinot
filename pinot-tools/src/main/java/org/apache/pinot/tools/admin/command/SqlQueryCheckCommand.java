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
package org.apache.pinot.tools.admin.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.NetUtil;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.common.utils.CommonConstants.Broker.Request.PQL;
import static org.apache.pinot.common.utils.CommonConstants.Broker.Request.SQL;


public class SqlQueryCheckCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(SqlQueryCheckCommand.class);

  @Option(name = "-pqlQueryLogFile", required = true, metaVar = "<string>", usage = "Path to pql-query.log file.")
  private String _pqlQueryLogFile = null;

  @Option(name = "-outputFile", required = true, metaVar = "<string>", usage = "Queries which did not parse.")
  private String _outputFile = null;

  @Option(name = "-help", help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String description() {
    return "Pass all queries from pql-query.log through calcite parser";
  }

  @Override
  public String getName() {
    return "CheckCalciteParser";
  }

  @Override
  public String toString() {
    return ("CheckCalciteParser -pqlQueryLogFile " + _pqlQueryLogFile + " -outputFile " + _outputFile);
  }

  @Override
  public void cleanup() {

  }

  public SqlQueryCheckCommand setPqlQueryLogFile(String pqlQueryLogFile) {
    _pqlQueryLogFile = pqlQueryLogFile;
    return this;
  }

  public SqlQueryCheckCommand setOutputFile(String outputFile) {
    _outputFile = outputFile;
    return this;
  }

  @Override
  public boolean execute()
      throws Exception {

    Pattern pattern = Pattern.compile(" PQL: (.*?) Time: ");

    try (BufferedReader br = new BufferedReader(new FileReader(new File(_pqlQueryLogFile)));
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File(_outputFile)))) {

      String line;
      while ((line = br.readLine()) != null) {
        Matcher matcher = pattern.matcher(line);
        while (matcher.find()) {
          String query = matcher.group(1);
          try {
            CalciteSqlParser.compileToPinotQuery(query);
          } catch (Exception e) {
            ObjectNode jsonNode = JsonUtils.newObjectNode();
            jsonNode.put(SQL, query);
            jsonNode.put("exception", e.getMessage());
            bw.write(jsonNode.toString() + ",\n");
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("Exception in execute", e);
    }

    return true;
  }

  public static void main(String[] args) {
    String query = "select sum(population) from table group by city";
    String logQuery =
        "SELECT sum(revenue) from myTable where dim1 != 10 limit 10, 200";
    CalciteSqlParser.compileToPinotQuery(logQuery);
  }
}