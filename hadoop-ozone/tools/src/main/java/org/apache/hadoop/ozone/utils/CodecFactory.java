/**
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

package org.apache.hadoop.ozone.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Creates Codec for a given type class.
 */
public class CodecFactory {

  private static final Logger LOG = LoggerFactory.getLogger(
      CodecFactory.class);
  public static final class MyCodec<T> implements Codec<T> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final Class<T> typeClass;

    public MyCodec(Class<T> typeClass) {
      this.typeClass = typeClass;
    }

    @Override
    public Class<T> getTypeClass() {
      return typeClass;
    }

    @Override
    public byte[] toPersistedFormat(T object)
        throws IOException {
      return MAPPER.writeValueAsBytes(object);
    }

    @Override
    public T fromPersistedFormat(byte[] rawData)
        throws IOException {
      return MAPPER.readValue(rawData, typeClass);
    }

    @Override
    public T copyObject(T object) {
      return object;
    }
  }
  public static <T> MyCodec<T> createCodec(String className) throws ClassNotFoundException {
    LOG.info("tej creating codec for: " + className);
    Class<T> clazz = (Class<T>) Class.forName(className);
    return new MyCodec<>(clazz);
  }

}
