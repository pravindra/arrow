/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.memory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import io.netty.buffer.ArrowBuf;

public class TestJNI {
  private static final int MAX_ALLOCATION = 8 * 1024;

  public static void main(String[] args) throws InterruptedException {
    long startTime = System.currentTimeMillis();
    ExecutorService executors = Executors.newFixedThreadPool(4);
    System.load("/Users/praveenkumar/Desktop/dremio/code/arrow-for/arrow/java/memory/temp.so");
    IntStream.range(0, 4)
            .forEach(
                    i -> {
                      executors.submit(
                              () -> {
                                for (int j = 0; j < 100000; j++) {
                                  try (final RootAllocator rootAllocator = new RootAllocator(MAX_ALLOCATION)) {
                                    final BufferAllocator childAllocator1 = rootAllocator.newChildAllocator("transferSliced1", 0, MAX_ALLOCATION);
                                    final BufferAllocator childAllocator2 = rootAllocator.newChildAllocator("transferSliced2", 0, MAX_ALLOCATION);

                                    final ArrowBuf arrowBuf1 = childAllocator1.buffer(MAX_ALLOCATION / 8);
                                    final ArrowBuf arrowBuf2 = childAllocator2.buffer(MAX_ALLOCATION / 8);


                                    arrowBuf1.release(); // releases arrowBuf1
                                    arrowBuf2.release(); // releases arrowBuf2

                                    childAllocator1.close();
                                    childAllocator2.close();
                                  }
                                }
                              });
                    });
    executors.shutdown();
    executors.awaitTermination(100, java.util.concurrent.TimeUnit.SECONDS);
    System.out.println((System.currentTimeMillis() - startTime));
  }
}
