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

        [Fact]
        public void TestStringArrayBasics()
        {
            List<char> chars = new List<char>() {'a', 'b', 'b', 'c', 'c', 'c'};
            List<int> offsets = new List<int>() {0, 1, 1, 1, 3, 6};
            List<int> validBytes = new List<int>() {1, 1, 0, 1, 1}; 

            List<string> expected = new List<string>() {"a", "", "", "bb", "ccc"};

            //var valueBufferBuilder = new ArrowBuffer.Builder<char>();
            //chars.Select(c => valueBufferBuilder.Append(c)); //AppendRange(chars).Build();
            //ArrowBuffer valueBuffer = valueBufferBuilder.Build();
            ArrowBuffer valueBuffer = new ArrowBuffer.Builder<char>().AppendRange(chars).Build();
            ArrowBuffer offsetBuffer = new ArrowBuffer.Builder<int>().AppendRange(offsets).Build();
            ArrowBuffer validityBuffer = new ArrowBuffer.Builder<int>().AppendRange(validBytes).Build();

            List<ArrowBuffer> buffers = new List<ArrowBuffer>() { validityBuffer, offsetBuffer, valueBuffer };
            ArrayData stringData = new ArrayData(StringType.Default, 5, 1, 0, buffers);

            StringArray stringArray = ArrowArrayFactory.BuildArray(stringData) as StringArray;

            int pos = 0;
            for (int ii = 0; ii < expected.Count; ii++)
            {
                Assert.Equal(pos, stringArray.GetValueOffset(ii));
                Assert.Equal(expected[ii].Length, stringArray.GetValueLength(ii));
                if (validBytes[ii] == 0)
                {
                    Assert.True(stringArray.IsNull(ii));
                }
                else
                {
                    Assert.Equal(expected[ii], stringArray.GetString(ii));
                }
                pos += expected[ii].Length;
            }
        }

        //[Fact]
        //public void BuildLargeInMemoryArray()
        //{
        //    long length = (long)Int32.MaxValue + 1;

        //    var builder = new ArrowBuffer.Builder<bool>();
        //    for (long ii = 0; ii <= length; ii++)
        //    {
        //        builder.Append(false);
        //    }
            
        //}

        //[Fact]
        //public void TestArraySliceAndNullCount()
        //{
        //    List<int> bytes = new List<int>() { 1, 0, 1, 1, 0, 1, 0, 0, 0 };
        //    ArrowBuffer buffer = new ArrowBuffer.Builder<int>().AppendRange(bytes).Build();
        //    ArrowBuffer nullBitmap = new ArrowBuffer.Builder<int>().AppendRange(bytes).Build();

        //    ArrayData intData = new ArrayData(Int32Type.Default, 8, 5, 0, new List<ArrowBuffer>() { nullBitmap, buffer });
        //    IArrowArray array = ArrowArrayFactory.BuildArray(intData);

        //    Assert.Equal(5, array.NullCount);

        //    var slice = array.Data.Buffers[1].Span.Slice(1, 4);
        //}

    }
}
