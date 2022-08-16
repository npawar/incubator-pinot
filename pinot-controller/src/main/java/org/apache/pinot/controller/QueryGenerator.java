package org.apache.pinot.controller;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.commons.lang.math.RandomUtils;


public class QueryGenerator {
  public static void main(String[] args)
      throws IOException {

    final long datasetStartSeconds = 1647586800;
    final int numQueries = 1000;

    try (BufferedWriter bw = new BufferedWriter(new FileWriter("/tmp/queries"))) {
      for (int i = 0; i < numQueries; i++) {
        long dayOffset = RandomUtils.nextInt(10);
        long startMillis = datasetStartSeconds + dayOffset * 86400;
        long endMillis = startMillis + 3 * 86400;
        String query =
            "SELECT COUNT(*) FROM prometheus_metrics_v3 WHERE \"timestamp\" >= " + startMillis + " AND \"timestamp\" < "
                + endMillis + " option(timeoutMs=180000)\n";
        bw.write(query);
      }
    }
  }
}
