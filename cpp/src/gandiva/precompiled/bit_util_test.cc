// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Adapted from Apache Impala

#include <gtest/gtest.h>

#include "gandiva/precompiled/bit_util.h"
#include "gandiva/precompiled/multi_precision.h"

namespace gandiva {

TEST(BitUtil, UnsignedWidth) {
  EXPECT_EQ(BitUtil::UnsignedWidth<signed char>(), 7);
  EXPECT_EQ(BitUtil::UnsignedWidth<unsigned char>(), 8);
  EXPECT_EQ(BitUtil::UnsignedWidth<volatile int64_t>(), 63);
  EXPECT_EQ(BitUtil::UnsignedWidth<uint64_t&>(), 64);
  EXPECT_EQ(BitUtil::UnsignedWidth<const int128_t&>(), 127);
  EXPECT_EQ(BitUtil::UnsignedWidth<const volatile unsigned __int128&>(), 128);
}

TEST(BitUtil, Sign) {
  EXPECT_EQ(BitUtil::Sign<int32_t>(0), 1);
  EXPECT_EQ(BitUtil::Sign<int32_t>(1), 1);
  EXPECT_EQ(BitUtil::Sign<int32_t>(-1), -1);
  EXPECT_EQ(BitUtil::Sign<int32_t>(200), 1);
  EXPECT_EQ(BitUtil::Sign<int32_t>(-200), -1);
  EXPECT_EQ(BitUtil::Sign<uint32_t>(0), 1);
  EXPECT_EQ(BitUtil::Sign<uint32_t>(1), 1);
  EXPECT_EQ(BitUtil::Sign<uint32_t>(-1U), 1);
  EXPECT_EQ(BitUtil::Sign<uint32_t>(200), 1);
  EXPECT_EQ(BitUtil::Sign<uint32_t>(-200), 1);
  EXPECT_EQ(BitUtil::Sign<int128_t>(0), 1);
  EXPECT_EQ(BitUtil::Sign<int128_t>(1), 1);
  EXPECT_EQ(BitUtil::Sign<int128_t>(-1), -1);
  EXPECT_EQ(BitUtil::Sign<int128_t>(200), 1);
  EXPECT_EQ(BitUtil::Sign<int128_t>(-200), -1);
}

}  // namespace gandiva
