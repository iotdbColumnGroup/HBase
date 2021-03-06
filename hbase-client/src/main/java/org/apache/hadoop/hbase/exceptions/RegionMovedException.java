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
package org.apache.hadoop.hbase.exceptions;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Subclass if the server knows the region is now on another server. This allows the client to call
 * the new region server without calling the master.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RegionMovedException extends NotServingRegionException {
  private static final Logger LOG = LoggerFactory.getLogger(RegionMovedException.class);
  private static final long serialVersionUID = -7232903522310558396L;

  private final String hostname;
  private final int port;
  private final long startCode;
  private final long locationSeqNum;

  private static final String HOST_FIELD = "hostname=";
  private static final String PORT_FIELD = "port=";
  private static final String STARTCODE_FIELD = "startCode=";
  private static final String LOCATIONSEQNUM_FIELD = "locationSeqNum=";

  public RegionMovedException(ServerName serverName, long locationSeqNum) {
    this.hostname = serverName.getHostname();
    this.port = serverName.getPort();
    this.startCode = serverName.getStartcode();
    this.locationSeqNum = locationSeqNum;
  }

  public String getHostname() {
    return hostname;
  }

  public int getPort() {
    return port;
  }

  public ServerName getServerName() {
    return ServerName.valueOf(hostname, port, startCode);
  }

  public long getLocationSeqNum() {
    return locationSeqNum;
  }

  /**
   * For hadoop.ipc internal call. Do NOT use. We have to parse the hostname to recreate the
   * exception. The input is the one generated by {@link #getMessage()}
   */
  public RegionMovedException(String s) {
    int posHostname = s.indexOf(HOST_FIELD) + HOST_FIELD.length();
    int posPort = s.indexOf(PORT_FIELD) + PORT_FIELD.length();
    int posStartCode = s.indexOf(STARTCODE_FIELD) + STARTCODE_FIELD.length();
    int posSeqNum = s.indexOf(LOCATIONSEQNUM_FIELD) + LOCATIONSEQNUM_FIELD.length();

    String tmpHostname = null;
    int tmpPort = -1;
    long tmpStartCode = -1;
    long tmpSeqNum = HConstants.NO_SEQNUM;
    try {
      // TODO: this whole thing is extremely brittle.
      tmpHostname = s.substring(posHostname, s.indexOf(' ', posHostname));
      tmpPort = Integer.parseInt(s.substring(posPort, s.indexOf(' ', posPort)));
      tmpStartCode = Long.parseLong(s.substring(posStartCode, s.indexOf('.', posStartCode)));
      tmpSeqNum = Long.parseLong(s.substring(posSeqNum, s.indexOf('.', posSeqNum)));
    } catch (Exception ignored) {
      LOG.warn(
        "Can't parse the hostname, port and startCode from this string: " + s + ", continuing");
    }

    hostname = tmpHostname;
    port = tmpPort;
    startCode = tmpStartCode;
    locationSeqNum = tmpSeqNum;
  }

  @Override
  public String getMessage() {
    // TODO: deserialization above depends on this. That is bad, but also means this
    // should be modified carefully.
    return "Region moved to: " + HOST_FIELD + hostname + " " + PORT_FIELD + port + " "
      + STARTCODE_FIELD + startCode + ". As of " + LOCATIONSEQNUM_FIELD + locationSeqNum + ".";
  }
}
