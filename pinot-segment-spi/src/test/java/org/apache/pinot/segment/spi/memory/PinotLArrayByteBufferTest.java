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
package org.apache.pinot.segment.spi.memory;

import net.openhft.chronicle.core.Jvm;
import org.apache.pinot.segment.spi.utils.JavaVersion;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;


public class PinotLArrayByteBufferTest extends PinotDataBufferTest {
  public PinotLArrayByteBufferTest() {
    super(new LArrayPinotBufferFactory());
  }

  @Override
  protected boolean prioritizeByteBuffer() {
    return false;
  }

  @BeforeClass
  public void abortOnModernJava() {
    //larray isn't supported on Mac/aarch64
    if (Jvm.isMacArm()) {
      throw new SkipException("Skipping LArray tests because they cannot run on Mac/aarch64");
    }
    if (JavaVersion.VERSION > 15) {
      throw new SkipException("Skipping LArray tests because they cannot run in Java " + JavaVersion.VERSION);
    }
  }
}
