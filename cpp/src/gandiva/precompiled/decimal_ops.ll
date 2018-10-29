; Licensed to the Apache Software Foundation (ASF) under one
; or more contributor license agreements.  See the NOTICE file
; distributed with this work for additional information
; regarding copyright ownership.  The ASF licenses this file
; to you under the Apache License, Version 2.0 (the
; "License"); you may not use this file except in compliance
; with the License.  You may obtain a copy of the License at
;
;   http://www.apache.org/licenses/LICENSE-2.0
;
; Unless required by applicable law or agreed to in writing,
; software distributed under the License is distributed on an
; "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
; KIND, either express or implied.  See the License for the
; specific language governing permissions and limitations
; under the License.

; Decimal functions

%struct.Decimal128Full = type { i128, i32, i32 }

; Function Attrs: alwaysinline nounwind ssp uwtable
define void @add_decimal128_decimal128(%struct.Decimal128Full*, %struct.Decimal128Full*, %struct.Decimal128Full*) #0 {
  %4 = alloca %struct.Decimal128Full*, align 8
  %5 = alloca %struct.Decimal128Full*, align 8
  %6 = alloca %struct.Decimal128Full*, align 8
  store %struct.Decimal128Full* %0, %struct.Decimal128Full** %4, align 8
  store %struct.Decimal128Full* %1, %struct.Decimal128Full** %5, align 8
  store %struct.Decimal128Full* %2, %struct.Decimal128Full** %6, align 8
  %7 = load %struct.Decimal128Full*, %struct.Decimal128Full** %4, align 8
  %8 = load %struct.Decimal128Full*, %struct.Decimal128Full** %5, align 8
  %9 = load %struct.Decimal128Full*, %struct.Decimal128Full** %6, align 8

  ; extract x
  %x = getelementptr inbounds %struct.Decimal128Full, %struct.Decimal128Full* %7, i32 0, i32 0
  %x_val = load i128, i128* %x, align 8

  ; extract y
  %y = getelementptr inbounds %struct.Decimal128Full, %struct.Decimal128Full* %8, i32 0, i32 0
  %y_val = load i128, i128* %y, align 8

  %out = getelementptr inbounds %struct.Decimal128Full, %struct.Decimal128Full* %9, i32 0, i32 0

  ; add the values
  %result = add nsw i128 %x_val, %y_val
  store i128 %result, i128* %out, align 8
  ret void
}

