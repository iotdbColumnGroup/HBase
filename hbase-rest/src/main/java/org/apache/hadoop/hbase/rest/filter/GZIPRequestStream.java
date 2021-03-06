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
package org.apache.hadoop.hbase.rest.filter;

import java.io.IOException;
import java.util.zip.GZIPInputStream;
import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class GZIPRequestStream extends ServletInputStream {
  private GZIPInputStream in;

  public GZIPRequestStream(HttpServletRequest request) throws IOException {
    this.in = new GZIPInputStream(request.getInputStream());
  }

  @Override
  public int read() throws IOException {
    return in.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return in.read(b);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return in.read(b, off, len);
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public boolean isFinished() {
    throw new UnsupportedOperationException("Asynchonous operation is not supported.");
  }

  @Override
  public boolean isReady() {
    throw new UnsupportedOperationException("Asynchonous operation is not supported.");
  }

  @Override
  public void setReadListener(ReadListener listener) {
    throw new UnsupportedOperationException("Asynchonous operation is not supported.");
  }
}
