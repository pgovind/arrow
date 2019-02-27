// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Apache.Arrow.Types;

namespace Apache.Arrow.Tests
{
    public class ArrayTests
    {
        [Fact]
        public void TestNullCount()
        {
            ArrowBuffer buffer = new ArrowBuffer.Builder<int>().Build();
            ArrowBuffer nullBitmap = new ArrowBuffer.Builder<int>().Build();

            ArrayData intData = new ArrayData(Int32Type.Default, 100, 10, 0, new List<ArrowBuffer>() { nullBitmap, buffer });
            IArrowArray intArray = ArrowArrayFactory.BuildArray(intData);
            Assert.Equal(10, intArray.NullCount);
            Assert.Equal(100, intArray.Length);

            intData = new ArrayData(Int32Type.Default, 0, 0, 0, new List<ArrowBuffer>() { nullBitmap, buffer });
            intArray = ArrowArrayFactory.BuildArray(intData);
            Assert.Equal(0, intArray.NullCount);
        }

        [Fact]
        public void TestLength()
        {
            ArrowBuffer buffer = new ArrowBuffer.Builder<int>().Build();
            ArrowBuffer nullBitmap = new ArrowBuffer.Builder<int>().Build();

            ArrayData intData = new ArrayData(Int32Type.Default, 100, 10, 0, new List<ArrowBuffer>() { nullBitmap, buffer });
            IArrowArray intArray = ArrowArrayFactory.BuildArray(intData);
            Assert.Equal(100, intArray.NullCount);
        }
    }
}
