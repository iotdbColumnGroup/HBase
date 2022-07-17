/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.regionserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FlushGroupingEngine {
  private static final Logger LOG = LoggerFactory.getLogger(HRegion.class);

  public static class ColumnGroup {
    public ArrayList<Integer> columns;
    public int maxGain = -1;
    public int maxGainPosition = -1;
    public int timeSeriesLength; // number of exist timestamps, i.e., zeros in bitmap
    public long interval;
    public ArrayList<Integer> bitmap;
    public ArrayList<Integer> gains;

    public ColumnGroup(
        ArrayList<Integer> columns, int maxGain, ArrayList<Integer> bitmap,  int GroupNum) {
      this.columns = columns;
      this.maxGain = maxGain;
      this.bitmap = bitmap;
      this.gains = new ArrayList<>(Collections.nCopies(GroupNum, -1));
      this.updateTimestampInfo();
    }

    public void updateTimestampInfo() {
      this.timeSeriesLength = 0;
      for (int bit: this.bitmap) {
        if (bit == 1){
          this.timeSeriesLength += 1;
        }
      }
    }

    public static ColumnGroup mergeColumnGroup(ColumnGroup g1, ColumnGroup g2) {
      // merge bitmap
      ArrayList<Integer> newBitmap = new ArrayList<>();
      for (int i = 0; i < g1.bitmap.size(); i++) {
        if (g1.bitmap.get(i) == 1 || g2.bitmap.get(i) == 1){
          newBitmap.add(1);
        } else {
          newBitmap.add(0);
        }
      }
      // merge columns
      ArrayList<Integer> newColumns = (ArrayList<Integer>) g1.columns.clone();
      newColumns.addAll(g2.columns);

      // update posIndex
      return new ColumnGroup(newColumns, -1, newBitmap, g1.gains.size() - 1);
    }
  }

  /** autoaligned grouping method * */
  /** autoaligned grouping method * */
  public static void grouping(ArrayList<ArrayList<Integer>> dataList, ArrayList<String> schemaList) {
    try {
      int timeSeriesNumber = dataList.size();
      ArrayList<ColumnGroup> columnGroups = new ArrayList<>();

      if (timeSeriesNumber == 0) {
        return;
      }
      if (timeSeriesNumber == 1) {
        outputGroupingResults(schemaList);
        return;
      }
      long t1 = System.currentTimeMillis();
      // init posMap, maxGain, groupingResult
      for (int i = 0; i < timeSeriesNumber; i++) {
        ArrayList<Integer> newGroup = new ArrayList<>();
        newGroup.add(i);
        ArrayList<Integer> bitmap = dataList.get(i);
        columnGroups.add(new ColumnGroup(newGroup, -1, bitmap, timeSeriesNumber));
      }

      // init gain matrix
      t1 = System.currentTimeMillis();
      for (int i = 0; i < timeSeriesNumber; i++) {
        for (int j = i + 1; j < timeSeriesNumber; j++) {
          int gain = computeGain(columnGroups.get(i), columnGroups.get(j));
          columnGroups.get(i).gains.set(j, gain);
          columnGroups.get(j).gains.set(i, gain);
          if (columnGroups.get(i).maxGain < gain) {
            columnGroups.get(i).maxGain = gain;
            columnGroups.get(i).maxGainPosition = j;
          }
          if (columnGroups.get(j).maxGain < gain) {
            columnGroups.get(j).maxGain = gain;
            columnGroups.get(j).maxGainPosition = i;
          }
        }
      }

      while (true) {
        /** merge * */

        // find the main gain
        int maxGainOfAll = -1;
        int maxGainOfAllPos = -1;
        for (int i = 0; i < columnGroups.size(); i++) {
          if (columnGroups.get(i).maxGain > maxGainOfAll) {
            maxGainOfAll = columnGroups.get(i).maxGain;
            maxGainOfAllPos = i;
          }
        }
        if (maxGainOfAll <= 0) {
          break;
        }

        // merge the group, create a new group
        int source = maxGainOfAllPos;
        int target = columnGroups.get(source).maxGainPosition;

        ColumnGroup newGroup =
            ColumnGroup.mergeColumnGroup(columnGroups.get(source), columnGroups.get(target));

        // remove the old groups
        if (target < source) {
          LOG.error("grouping error: target < source");
        }
        columnGroups.remove(target);
        columnGroups.remove(source);

        // load target into source
        columnGroups.add(source, newGroup);

        /** update * */

        // update gains
        // remove the target, and update the source
        for (int i = 0; i < columnGroups.size(); i++) {
          if (i != source) {
            ColumnGroup g = columnGroups.get(i);
            ColumnGroup gSource = columnGroups.get(source);
            g.gains.remove(target);
            int gain = computeGain(g, gSource);
            g.gains.set(source, gain);
            // update the maxgain in i
            if ((g.maxGainPosition != source) && (g.maxGainPosition != target)) {
              if (gain > g.maxGain) {
                g.maxGain = gain;
                g.maxGainPosition = source;
              } else {
                if (target < g.maxGainPosition) {
                  g.maxGainPosition -= 1;
                }
              }
            } else {
              g.maxGain = gain;
              g.maxGainPosition = source;
              for (int j = 0; j < g.gains.size(); j++) {
                if (g.gains.get(j) > g.maxGain) {
                  g.maxGain = g.gains.get(j);
                  g.maxGainPosition = j;
                }
              }
            }
            // update the maxgain and data in source
            gSource.gains.set(i, gain);
            if (gain > gSource.maxGain) {
              gSource.maxGain = gain;
              gSource.maxGainPosition = i;
            }
          }
        }
      }

      long t2 = System.currentTimeMillis() - t1;
      writeTimeToFile("flush time", t2);
      outputGroupingResults(columnGroups, schemaList);
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }

  public static void outputGroupingResults(ArrayList<String> schemaList) {
    String outputFile =
        "/Users/db/code/paper-experiments/alignment-storage-experiments/client-py/grouping_results/grouping_results.csv";
    BufferedWriter out = null;
    try {
      out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile, true)));
      int i = 0;
      for (; i < schemaList.size() - 1; i++) {
        out.write(schemaList.get(i) + ",");
      }
      out.write(schemaList.get(i) + "\r\n");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        out.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void outputGroupingResults(
      ArrayList<ColumnGroup> columnGroups, ArrayList<String> schemaList) {
    String outputFile =
        "/Users/db/code/paper-experiments/hbase-2.4.13/grouping_results/grouping_results.csv";
    BufferedWriter out = null;
    try {
      out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile, true)));
      for (int i = 0; i < columnGroups.size(); i++) {
        for (int j = 0; j < columnGroups.get(i).columns.size() - 1; j++) {
          int idx = columnGroups.get(i).columns.get(j);
          String measurement = schemaList.get(idx);
          out.write(measurement + ",");
        }
        int idx = columnGroups.get(i).columns.get(columnGroups.get(i).columns.size() - 1);
        out.write(schemaList.get(idx) + "\r\n");
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        out.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static int computeOverlap(ArrayList<Integer> col1, ArrayList<Integer> col2) {
    int overlaps = 0;
    for (int i = 0; i < col1.size(); i++) {
      if (col1.get(i) == 1 && col2.get(i) == 1) {
        overlaps += 1;
      }
    }
    return overlaps;
  }

  public static int computeGain(
      ColumnGroup g1, ColumnGroup g2) {
    int overlap = computeOverlap(g1.bitmap, g2.bitmap);
    if (g1.columns.size() == 1 && g2.columns.size() == 1) {
      // col - col
      int gain = overlap * Long.SIZE - 2 * (g1.timeSeriesLength + g2.timeSeriesLength - overlap);
      return gain;
    }
    if (g1.columns.size() == 1) {
      // col - group
      int m_s_a = g1.timeSeriesLength;
      int n_g_a = g2.columns.size();
      int m_g_a = g2.timeSeriesLength;
      int gain = overlap * Long.SIZE + n_g_a * m_g_a - (n_g_a + 1) * (m_s_a + m_g_a - overlap);
      return gain;
    }
    if (g2.columns.size() == 1) {
      // group - col
      int m_s_a = g2.timeSeriesLength;
      int n_g_a = g1.columns.size();
      int m_g_a = g1.timeSeriesLength;
      int gain = overlap * Long.SIZE + n_g_a * m_g_a - (n_g_a + 1) * (m_s_a + m_g_a - overlap);
      return gain;
    }

    // group - group
    int n_g_a = g1.columns.size();
    int m_g_a = g1.timeSeriesLength;
    int n_g_b = g2.columns.size();
    int m_g_b = g2.timeSeriesLength;

    int gain = overlap * Long.SIZE + (n_g_a + n_g_b) * overlap - n_g_a * m_g_b - n_g_b * m_g_a;
    return gain;
  }

  public static void writeTimeToFile(String prefix, double t) {
    String outputFile =
        "/Users/db/code/paper-experiments/hbase-2.4.13/grouping_results/time_costs.csv";
    BufferedWriter out = null;
    try {
      out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile, true)));
      out.write(String.format(prefix + " costs %.3fs", t / 1000.0) + "\r\n");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        out.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
