﻿// Licensed to the Apache Software Foundation (ASF) under one or more
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

using System.Runtime.InteropServices;
using System;
using System.Text;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public class StringArray: BinaryArray
    {
        public StringArray(ArrayData data) 
            : base(ArrowTypeId.String, data) { }

        public StringArray(int length,
            ArrowBuffer valueOffsetsBuffer,
            ArrowBuffer dataBuffer,
            ArrowBuffer nullBitmapBuffer,
            int nullCount = 0, int offset = 0)
            : this(new ArrayData(StringType.Default, length, nullCount, offset,
                new[] { nullBitmapBuffer, valueOffsetsBuffer, dataBuffer }))
        { }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        new public ReadOnlySpan<char> Values => ValueBuffer.Span.CastTo<char>();
        new public ReadOnlySpan<char> GetBytes(int index)
        {
            var offset = GetValueOffset(index);
            var length = GetValueLength(index);

            return Values.Slice(offset, length);
        }

        public string GetString(int index, Encoding encoding = default)
        {
            // I don't think I need encoding here? Whatever chars went in should come out that same?
            //encoding = encoding ?? Encoding.UTF8;

            ReadOnlySpan<char> bytes = GetBytes(index);

            return bytes.ToString();
            //unsafe
            //{
            //    var reference = MemoryMarshal.GetReference(bytes);
            //    fixed (byte* data = &MemoryMarshal.GetReference(bytes))
            //        return encoding.GetString(data, bytes.Length);
            //}
        }
    }
}
