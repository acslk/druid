/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation;

import com.google.common.primitives.Chars;

import java.nio.ByteBuffer;

public class StringHelper
{
  public static void putString(ByteBuffer buf, int position, String string, int maxLength) {
    int length = Math.min(string.length(), maxLength);
    for (int i = 0; i < length; i++) {
      buf.putChar(position + i * Chars.BYTES, string.charAt(i));
    }
    if (length < maxLength) {
      buf.putChar(position + length * Chars.BYTES, '\0');
    }
  }

  public static String getString(ByteBuffer buf, int position, int maxLength) {
    StringBuilder sb = new StringBuilder(maxLength);
    for (int i = 0; i < maxLength; i++) {
      char c = buf.getChar(position + i * Chars.BYTES);
      if (c == '\0') {
        return sb.toString();
      }
      sb.append(c);
    }
    return sb.toString();
  }
}
