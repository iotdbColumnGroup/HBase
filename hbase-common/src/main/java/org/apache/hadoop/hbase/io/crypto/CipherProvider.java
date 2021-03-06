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
package org.apache.hadoop.hbase.io.crypto;

import org.apache.hadoop.conf.Configurable;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * An CipherProvider contributes support for various cryptographic Ciphers.
 */
@InterfaceAudience.Public
public interface CipherProvider extends Configurable {

  /**
   * Return the provider's name
   */
  public String getName();

  /**
   * Return the set of Ciphers supported by this provider
   */
  public String[] getSupportedCiphers();

  /**
   * Get an Cipher
   * @param name Cipher name, e.g. "AES"
   * @return the appropriate Cipher
   */
  public Cipher getCipher(String name);

}
