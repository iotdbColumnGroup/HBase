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

package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.regionserver.HRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FlushApprGroupingEngine {
  private static final Logger LOG = LoggerFactory.getLogger(HRegion.class);

  public static class ColumnGroupAppr {
    public ArrayList<Integer> columns;
    public int maxGain = -1;
    public int maxGainPosition = -1;
    public int timeSeriesLength; // number of exist timestamps, i.e., zeros in bitmap
    public long interval;
    public ArrayList<Integer> bitmap;
    public ArrayList<Integer> gains;
    public ArrayList<Feature> features;
    public ArrayList<Long> rangeMap;
    public ArrayList<ArrayList<Integer>> rangeMapCols;

    public ColumnGroupAppr(
        ArrayList<Integer> columns,
        int maxGain,
        ArrayList<Integer> bitmap,
        int GroupNum,
        ArrayList<Long> timestampList) {
      this.columns = columns;
      this.maxGain = maxGain;
      this.bitmap = bitmap;
      this.gains = new ArrayList<>(Collections.nCopies(GroupNum, -1));
      this.updateTimestampInfo(timestampList);
    }

    public ColumnGroupAppr(
        ArrayList<Integer> columns,
        int maxGain,
        int timeSeriesLength,
        int GroupNum,
        ArrayList<Feature> features,
        ArrayList<Long> rangeMap,
        ArrayList<ArrayList<Integer>> rangeMapCols) {
      this.columns = columns;
      this.maxGain = maxGain;
      this.timeSeriesLength = timeSeriesLength;
      this.gains = new ArrayList<>(Collections.nCopies(GroupNum, -1));
      this.features = features;
      this.rangeMap = rangeMap;
      this.rangeMapCols = rangeMapCols;
    }

    public void updateTimestampInfo(ArrayList<Long> timestampList) {
      ArrayList<Long> timestamps = new ArrayList<>();

      this.timeSeriesLength = 0;
      for (int idx = 0; idx < this.bitmap.size(); idx++) {
        if (this.bitmap.get(idx) == 1) {
          this.timeSeriesLength += 1;
          Long timestamp = timestampList.get(idx);
          timestamps.add(timestamp);
        }
      }

      int intervalGran = 1;
      long intervalSum = 0;
      int intervalCount = 0;
      int intervalMax = 1000;

      for (int i = 1; i < timestamps.size(); i++) {
        long interval_ = timestamps.get(i) - timestamps.get(i - 1);
        if (interval_ < intervalMax) {
          intervalSum += interval_;
          intervalCount++;
        }
      }
      long interval = intervalSum / intervalCount;
      interval = interval / intervalGran * intervalGran;

      long start = timestamps.get(0);
      double sigma;
      double sigmaSum = 0;
      long offset = 0;
      for (int i = 0; i < timestamps.size(); i++) {
        sigma = Math.abs(timestamps.get(i) - start - i * interval - offset);
        if (sigma > 10 * interval) {
          sigma = Math.abs(timestamps.get(i) - start - i * interval) % interval;
          offset = Math.abs(timestamps.get(i) - start - i * interval) / interval * interval;
        }
        sigmaSum += sigma;
      }
      sigma = sigmaSum / timestamps.size();

      this.features = new ArrayList<>();
      this.features.add(new Feature(interval, this.timeSeriesLength, start, sigma));

      this.rangeMap = new ArrayList<>();
      this.rangeMapCols = new ArrayList<>();
      rangeMap.add(start);
      rangeMap.add(start + interval * (this.timeSeriesLength - 1));
      ArrayList<Integer> overlapGroup = new ArrayList<>();
      overlapGroup.add(this.columns.get(0));
      rangeMapCols.add(overlapGroup);
    }

    public static ColumnGroupAppr mergeColumnGroup(ColumnGroupAppr g1, ColumnGroupAppr g2) {
      // merge columns, features;
      ArrayList<Integer> newColumns = (ArrayList<Integer>) g1.columns.clone();
      newColumns.addAll(g2.columns);
      ArrayList<Feature> features = (ArrayList<Feature>) g1.features.clone();
      features.addAll(g2.features);

      int overlap = estimateOverlap(g1, g2);

      int timeSeriesLength = g1.timeSeriesLength + g2.timeSeriesLength - overlap;
      int GroupNum = g1.gains.size() - 1;

      // merge rangeMap
      ArrayList<Long> map1;
      ArrayList<Long> map2;
      ArrayList<ArrayList<Integer>> mapCols1;
      ArrayList<ArrayList<Integer>> mapCols2;
      ArrayList<Long> newMap = new ArrayList<>();
      ArrayList<ArrayList<Integer>> newMapCols = new ArrayList<>();

      if (g1.rangeMap.get(0) < g2.rangeMap.get(0)) {
        map1 = g1.rangeMap;
        map2 = g2.rangeMap;
        mapCols1 = g1.rangeMapCols;
        mapCols2 = g2.rangeMapCols;
      } else {
        map1 = g2.rangeMap;
        map2 = g1.rangeMap;
        mapCols1 = g2.rangeMapCols;
        mapCols2 = g1.rangeMapCols;
      }

      int i = 0;
      int j = 0;

      int lastPoint = map1.get(0).equals(map2.get(0)) ? 3 : 1; // 1: map1, 2:map2, 3: equal
      newMap.add(map1.get(i));

      while (true) {
        if (lastPoint == 1) {
          i += 1;
          if (i >= map1.size()) {
            break;
          }
          if (map1.get(i) < map2.get(j)) {
            newMap.add(map1.get(i));
            newMapCols.add(mapCols1.get(i - 1));
          } else if (map1.get(i).equals(map2.get(j))) {
            lastPoint = 3;
            newMap.add(map1.get(i));
            if (j == 0) {
              newMapCols.add(mapCols1.get(i - 1));
            } else {
              ArrayList<Integer> tmpCols = (ArrayList<Integer>) mapCols1.get(i - 1).clone();
              tmpCols.addAll(mapCols2.get(j - 1));
              newMapCols.add(tmpCols);
            }
          } else {
            lastPoint = 2;
            newMap.add(map2.get(j));
            if (j == 0) {
              newMapCols.add(mapCols1.get(i - 1));
            } else {
              ArrayList<Integer> tmpCols = (ArrayList<Integer>) mapCols1.get(i - 1).clone();
              tmpCols.addAll(mapCols2.get(j - 1));
              newMapCols.add(tmpCols);
            }
          }

        } else if (lastPoint == 2) {
          j += 1;
          if (j >= map2.size()) {
            break;
          }
          if (map2.get(j) < map1.get(i)) {
            newMap.add(map2.get(j));
            newMapCols.add(mapCols2.get(j - 1));
          } else if (map1.get(i).equals(map2.get(j))) {
            lastPoint = 3;
            newMap.add(map2.get(j));
            if (i == 0) {
              newMapCols.add(mapCols2.get(j - 1));
            } else {
              ArrayList<Integer> tmpCols = (ArrayList<Integer>) mapCols2.get(j - 1).clone();
              tmpCols.addAll(mapCols1.get(i - 1));
              newMapCols.add(tmpCols);
            }
          } else {
            lastPoint = 1;
            newMap.add(map1.get(i));
            if (i == 0) {
              newMapCols.add(mapCols2.get(j - 1));
            } else {
              ArrayList<Integer> tmpCols = (ArrayList<Integer>) mapCols2.get(j - 1).clone();
              tmpCols.addAll(mapCols1.get(i - 1));
              newMapCols.add(tmpCols);
            }
          }
        } else {
          // lastPoint == 3
          i += 1;
          j += 1;
          if ((i >= map1.size()) || (j >= map2.size())) {
            break;
          }
          if (map1.get(i) < map2.get(j)) {
            lastPoint = 1;
            newMap.add(map1.get(i));
          } else if (map1.get(i) == map2.get(j)) {
            newMap.add(map1.get(i));
          } else {
            lastPoint = 3;
            newMap.add(map2.get(j));
          }
          ArrayList<Integer> tmpCols = (ArrayList<Integer>) mapCols1.get(i - 1).clone();
          tmpCols.addAll(mapCols2.get(j - 1));
          newMapCols.add(tmpCols);
        }
      }

      if (i >= map1.size()) {
        if (lastPoint == 1) {
          while (j < map2.size()) {
            newMap.add(map2.get(j));
            newMapCols.add(mapCols2.get(j - 1));
            j += 1;
          }
        } else {
          j += 1;
          while (j < map2.size()) {
            newMap.add(map2.get(j));
            newMapCols.add(mapCols2.get(j - 1));
            j += 1;
          }
        }
      } else if (j >= map2.size()) {
        if (lastPoint == 2) {
          while (i < map1.size()) {
            newMap.add(map1.get(i));
            newMapCols.add(mapCols1.get(i - 1));
            i += 1;
          }
        } else {
          i += 1;
          while (i < map1.size()) {
            newMap.add(map1.get(i));
            newMapCols.add(mapCols1.get(i - 1));
            i += 1;
          }
        }
      }

      return new ColumnGroupAppr(
          newColumns, -1, timeSeriesLength, GroupNum, features, newMap, newMapCols);
    }
  }

  public static void groupingAppr(ArrayList<ArrayList<Integer>> dataList, ArrayList<String> schemaList, ArrayList<Long> timestampList) {
    try {
      int timeSeriesNumber = dataList.size();
      ArrayList<ColumnGroupAppr> columnGroups = new ArrayList<>();

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
        columnGroups.add(
            new ColumnGroupAppr(newGroup, -1, bitmap, timeSeriesNumber, timestampList));
      }

      // init gain matrix
      for (int i = 0; i < timeSeriesNumber; i++) {
        columnGroups.get(i).gains.set(i, 0);
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
        System.out.println(columnGroups.size());
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

        ColumnGroupAppr newGroup =
            ColumnGroupAppr.mergeColumnGroup(columnGroups.get(source), columnGroups.get(target));

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
            ColumnGroupAppr g = columnGroups.get(i);
            ColumnGroupAppr gSource = columnGroups.get(source);
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
      outputGroupingResults(schemaList, columnGroups);
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }

  public static int computeGain(ColumnGroupAppr g1, ColumnGroupAppr g2) {
    int overlap = estimateOverlap(g1, g2);
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

  public static int estimateOverlap(ColumnGroupAppr g1, ColumnGroupAppr g2) {
    ArrayList<Double> overlapList = new ArrayList<>();

    for (int i = 0; i < g2.columns.size(); i++) {
      Feature f2 = g2.features.get(i);
      long start2 = f2.start;
      long end2 = f2.start + f2.epsilon * (f2.n - 1);
      double overlapTmp = 0;

      int t = 0;

      while ((t < g1.rangeMap.size() - 1) && (g1.rangeMap.get(t) < start2)) {
        t += 1;
      }
      if (t == g1.rangeMap.size() - 1) {
        overlapList.add(0.0);
        continue;
      } else {
        if ((t > 0) && (g1.rangeMap.get(t - 1) < start2)) {
          int n2Tmp = (int) ((start2 - g1.rangeMap.get(t - 1)) / f2.epsilon);
          double prob = 1;
          for (int col : g1.rangeMapCols.get(t - 1)) {
            for (int k = 0; k < g1.columns.size(); k++) {
              if (g1.columns.get(k) == col) {
                Feature f1 = g1.features.get(k);
                prob *= (1 - computeProbability(f1, f2));
                break;
              }
            }
          }
          prob -= 1;
          overlapTmp += prob * n2Tmp;
        }
      }

      if (g1.rangeMap.get(t) >= start2) {
        int j = t;
        while ((j < g1.rangeMap.size() - 1) && (g1.rangeMap.get(j) < end2)) {
          j += 1;
          int n2Tmp;
          if (g1.rangeMap.get(j) < end2) {
            n2Tmp = (int) ((g1.rangeMap.get(j) - g1.rangeMap.get(j - 1)) / f2.epsilon);
          } else {
            n2Tmp = (int) ((end2 - g1.rangeMap.get(j - 1)) / f2.epsilon);
          }
          double prob = 1;
          for (int col : g1.rangeMapCols.get(j - 1)) {
            for (int k = 0; k < g1.columns.size(); k++) {
              if (g1.columns.get(k) == col) {
                Feature f1 = g1.features.get(k);
                prob *= (1 - computeProbability(f1, f2));
                break;
              }
            }
          }
          prob = 1 - prob;
          overlapTmp += prob * n2Tmp;
        }
      }
      overlapList.add(overlapTmp);
    }

    double sumOverlap = 0;
    for (double overlap : overlapList) {
      sumOverlap += overlap;
    }
    int sumN = 0;
    for (Feature f : g2.features) {
      sumN += f.n;
    }

    int overlapEstimation = (int) ((sumOverlap / sumN) * g2.timeSeriesLength);
    return overlapEstimation;
  }

  public static double computeProbability(Feature f1, Feature f2) {
    if (f1.epsilon == f2.epsilon && f1.n == f2.n && f1.sigma == f2.sigma && f1.start == f2.start) {
      return 1;
    }
    if ((f1.start > f2.start + f2.n * f2.epsilon) || (f2.start > f1.start + f1.n * f1.epsilon)) {
      return 0;
    }

    int lambda = 3;
    int tau = 10;
    double prob = 0;
    long lcmInterval = lcm(f1.epsilon, f2.epsilon);
    long scenarios;
    if (f1.epsilon > f2.epsilon) {
      scenarios = lcmInterval / f1.epsilon;
      for (int i = 0; i < scenarios; i++) {
        int delta =
            ((int) (f1.epsilon - f2.epsilon) * i + (int) f1.start - (int) f2.start)
                % (int) f2.epsilon;
        if (f1.sigma == 0 && f2.sigma == 0) {
          if (delta == 0) {
            prob += 1;
          } else {
            prob += 0;
          }
        } else {
          double z1 =
              ((lambda * tau) - delta) / Math.sqrt(f1.sigma * f1.sigma + f2.sigma * f2.sigma);
          double z2 =
              (-(lambda * tau) - delta) / Math.sqrt(f1.sigma * f1.sigma + f2.sigma * f2.sigma);
          prob += NormSDist(z1) - NormSDist(z2);
        }
      }
    } else {
      scenarios = lcmInterval / f2.epsilon;
      for (int i = 0; i < scenarios; i++) {
        int delta =
            ((int) (f2.epsilon - f1.epsilon) * i + (int) f2.start - (int) f1.start)
                % (int) f1.epsilon;
        if (f1.sigma == 0 && f2.sigma == 0) {
          if (delta == 0) {
            prob += 1;
          } else {
            prob += 0;
          }
        } else {
          double z1 =
              ((lambda * tau) - delta) / Math.sqrt(f1.sigma * f1.sigma + f2.sigma * f2.sigma);
          double z2 =
              (-(lambda * tau) - delta) / Math.sqrt(f1.sigma * f1.sigma + f2.sigma * f2.sigma);
          prob += NormSDist(z1) - NormSDist(z2);
        }
      }
    }
    prob /= scenarios;
    return prob;
  }


  public static class Feature {
    public long epsilon;
    public int n;
    public long start;
    public double sigma;

    public Feature(long epsilon_, int n_, long start_, double sigma_) {
      this.epsilon = epsilon_;
      this.n = n_;
      this.start = start_;
      this.sigma = sigma_;
    }
  }

  public static double NormSDist(double z) {
    // compute approximate normal distribution cdf F(z)
    if (z > 6) return 1;
    if (z < -6) return 0;
    double gamma = 0.231641900,
        a1 = 0.319381530,
        a2 = -0.356563782,
        a3 = 1.781477973,
        a4 = -1.821255978,
        a5 = 1.330274429;
    double x = Math.abs(z);
    double t = 1 / (1 + gamma * x);
    double n =
        1
            - (1 / (Math.sqrt(2 * Math.PI)) * Math.exp(-z * z / 2))
            * (a1 * t
            + a2 * Math.pow(t, 2)
            + a3 * Math.pow(t, 3)
            + a4 * Math.pow(t, 4)
            + a5 * Math.pow(t, 5));
    if (z < 0) return 1.0 - n;
    return n;
  }

  public static long lcm(long number1, long number2) {
    if (number1 == 0 || number2 == 0) {
      return 0;
    }
    long absNumber1 = Math.abs(number1);
    long absNumber2 = Math.abs(number2);
    long absHigherNumber = Math.max(absNumber1, absNumber2);
    long absLowerNumber = Math.min(absNumber1, absNumber2);
    long lcm = absHigherNumber;
    while (lcm % absLowerNumber != 0) {
      lcm += absHigherNumber;
    }
    return lcm;
  }

  public static void writeTimeToFile(String prefix, double t) {
    String outputFile =
        "/Users/db/code/paper-experiments/hbase-2.4.13/grouping_results/time_costs_appr.csv";
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

  public static void outputGroupingResults(
      ArrayList<String> schemaList, ArrayList<ColumnGroupAppr> columnGroups) {
    String outputFile =
        "/Users/db/code/paper-experiments/hbase-2.4.13/grouping_results/grouping_results_appr.csv";
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

}
