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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.mob;

import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The mob file name. It consists of a md5 of a start key, a date and an uuid. It looks like
 * md5(start) + date + uuid.
 * <ol>
 * <li>characters 0-31: md5 hex string of a start key. Since the length of the start key is not
 * fixed, have to use the md5 instead which has a fix length.</li>
 * <li>characters 32-39: a string of a date with format yyyymmdd. The date is the latest timestamp
 * of cells in this file</li>
 * <li>the remaining characters: the uuid.</li>
 * </ol>
 * Using md5 hex string of start key as the prefix of file name makes files with the same start key
 * unique, they're different from the ones with other start keys The cells come from different
 * regions might be in the same mob file by region split, this is allowed. Has the latest timestamp
 * of cells in the file name in order to clean the expired mob files by TTL easily. If this
 * timestamp is older than the TTL, it's regarded as expired.
 */
@InterfaceAudience.Private
public final class MobFileName {
  private final String date;
  private final String startKey;
  private final String uuid;
  private final String fileName;

  private static final int STARTKEY_END_INDEX = 32;
  private static final int DATE_END_INDEX = 40;
  private static final int UUID_END_INDEX = 72;

  /**
   * n * The start key. n * The string of the latest timestamp of cells in this file, the format is
   * yyyymmdd. n * The uuid
   */
  private MobFileName(byte[] startKey, String date, String uuid) {
    this.startKey = MD5Hash.getMD5AsHex(startKey, 0, startKey.length);
    this.uuid = uuid;
    this.date = date;
    this.fileName = this.startKey + this.date + this.uuid;
  }

  /**
   * n * The md5 hex string of the start key. n * The string of the latest timestamp of cells in
   * this file, the format is yyyymmdd. n * The uuid
   */
  private MobFileName(String startKey, String date, String uuid) {
    this.startKey = startKey;
    this.uuid = uuid;
    this.date = date;
    this.fileName = this.startKey + this.date + this.uuid;
  }

  /**
   * Creates an instance of MobFileName n * The md5 hex string of the start key. n * The string of
   * the latest timestamp of cells in this file, the format is yyyymmdd.
   * @param uuid The uuid.
   * @return An instance of a MobFileName.
   */
  public static MobFileName create(byte[] startKey, String date, String uuid) {
    return new MobFileName(startKey, date, uuid);
  }

  /**
   * Creates an instance of MobFileName n * The md5 hex string of the start key. n * The string of
   * the latest timestamp of cells in this file, the format is yyyymmdd.
   * @param uuid The uuid.
   * @return An instance of a MobFileName.
   */
  public static MobFileName create(String startKey, String date, String uuid) {
    return new MobFileName(startKey, date, uuid);
  }

  /**
   * Creates an instance of MobFileName.
   * @param fileName The string format of a file name.
   * @return An instance of a MobFileName.
   */
  public static MobFileName create(String fileName) {
    // The format of a file name is md5HexString(0-31bytes) + date(32-39bytes) + UUID
    // The date format is yyyyMMdd
    String startKey = fileName.substring(0, STARTKEY_END_INDEX);
    String date = fileName.substring(STARTKEY_END_INDEX, DATE_END_INDEX);
    String uuid = fileName.substring(DATE_END_INDEX, UUID_END_INDEX);
    return new MobFileName(startKey, date, uuid);
  }

  /**
   * get startKey from MobFileName.
   * @param fileName file name. n
   */
  public static String getStartKeyFromName(final String fileName) {
    return fileName.substring(0, STARTKEY_END_INDEX);
  }

  /**
   * get date from MobFileName.
   * @param fileName file name. n
   */
  public static String getDateFromName(final String fileName) {
    return fileName.substring(STARTKEY_END_INDEX, DATE_END_INDEX);
  }

  /**
   * Gets the hex string of the md5 for a start key.
   * @return The hex string of the md5 for a start key.
   */
  public String getStartKey() {
    return startKey;
  }

  /**
   * Gets the date string. Its format is yyyymmdd.
   * @return The date string.
   */
  public String getDate() {
    return this.date;
  }

  @Override
  public int hashCode() {
    return fileName.hashCode();
  }

  @Override
  public boolean equals(Object anObject) {
    if (this == anObject) {
      return true;
    }
    if (anObject instanceof MobFileName) {
      MobFileName another = (MobFileName) anObject;
      return this.getFileName().equals(another.getFileName());
    }
    return false;
  }

  /**
   * Gets the file name.
   * @return The file name.
   */
  public String getFileName() {
    return this.fileName;
  }
}
