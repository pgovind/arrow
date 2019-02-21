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
using Apache.Arrow.Types;
using Apache.Arrow.Ipc;
using System.IO;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ArrowStreamTests
    {
        public class SchemaTests
        {
            void CheckRoundTrip(Schema schema)
            {
                var stream = new MemoryStream();
                var writer = new ArrowStreamWriter(stream, schema);
                writer.WriteSchemaAsync(schema, System.Threading.CancellationToken.None).RunSynchronously();

                var reader = new ArrowStreamReader(stream);
                var readSchema = reader.Schema;
                Assert.True(schema.Equals(readSchema));
            }
            [Fact]
            public void TestRoundTripPrimitiveFields()
            {
                Field f0 = new Field.Builder().Name("f0").DataType(Int8Type.Default).Build();
                Field f1 = new Field.Builder().Name("f1").DataType(Int16Type.Default).Build();
                Field f2 = new Field.Builder().Name("f2").DataType(Int32Type.Default).Build();
                Field f3 = new Field.Builder().Name("f3").DataType(Int64Type.Default).Build();
                Field f4 = new Field.Builder().Name("f4").DataType(UInt8Type.Default).Build();
                Field f5 = new Field.Builder().Name("f5").DataType(UInt16Type.Default).Build();
                Field f6 = new Field.Builder().Name("f6").DataType(UInt32Type.Default).Build();
                Field f7 = new Field.Builder().Name("f7").DataType(UInt64Type.Default).Build();
                Field f8 = new Field.Builder().Name("f8").DataType(FloatType.Default).Build();
                Field f9 = new Field.Builder().Name("f9").DataType(DoubleType.Default).Build();
                Field f10 = new Field.Builder().Name("f10").DataType(BooleanType.Default).Build();

                Schema schema = new Schema.Builder()
                                    .Field(f0)
                                    .Field(f1)
                                    .Field(f2)
                                    .Field(f3)
                                    .Field(f4)
                                    .Field(f5)
                                    .Field(f6)
                                    .Field(f7)
                                    .Field(f8)
                                    .Field(f9)
                                    .Field(f10)
                                    .Build();


                                    
            }
        }

    }
}
