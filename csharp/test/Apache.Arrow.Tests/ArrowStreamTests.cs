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
using System.Threading.Tasks;
using System.Linq;

namespace Apache.Arrow.Tests
{
    public class ArrowStreamTests
    {
        static public void CheckRoundTrip(Schema schema)
        {
            var stream = new MemoryStream();
            var writer = new ArrowStreamWriter(stream, schema);
            writer.WriteSchemaAsync(schema, System.Threading.CancellationToken.None).Wait();

            stream.Position = 0;
            var reader = new ArrowStreamReader(stream);
            reader.ReadSchemaAsync().Wait();
            var readSchema = reader.Schema;
            Assert.True(SchemaComparer.Equals(schema, readSchema));
        }
        public class SchemaTests
        {
            [Fact]
            public void TestPrimitiveFieldsRoundTrip()
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


                ArrowStreamTests.CheckRoundTrip(schema);
            }

            // TODO: Turn these on once ArrowFlatBuffersBuilder is updated for ListType and StructType
            //[Fact]
            //public void TestRoundTripNestedFields()
            //{
                
            //    Field f = new Field.Builder().Name("f0").DataType(Int32Type.Default).Build();
            //    ListType lt = new ListType(f, Int32Type.Default);
            //    Field flt = new Field.Builder().DataType(lt).Name("list").Build();

            //    Field f0 = new Field.Builder().Name("f0").DataType(Int32Type.Default).Build();
            //    Field f1 = new Field.Builder().Name("f1").DataType(Int16Type.Default).Build();
            //    Field f2 = new Field.Builder().Name("f2").DataType(Int32Type.Default).Build();
            //    StructType st = new StructType(new List<Field>() {f0, f1, f2});

            //    Field fst = new Field.Builder().Name("f3").DataType(st).Build();

            //    Schema schema = new Schema.Builder()
            //                        .Field(flt)
            //                        .Field(fst)
            //                        .Build();


            //    CheckRoundTrip(schema);
            //}


        }

        public class RecordBatchTests
        {
            public class ArrowArrayTestsVisitor :
                IArrowArrayVisitor<Int8Array>,
                IArrowArrayVisitor<Int32Array>
            {
                public IArrowArray right;
                public bool Equal { get; private set; }
                public ArrowArrayTestsVisitor(IArrowArray inRight)
                {
                    this.right = inRight;
                    Equal = true;
                }
                public void VisitPrimitiveArray(Array left)
                {
                    if (left.Length == right.Length || left.NullCount == right.NullCount || left.Offset == right.Offset)
                    {
                        ArrayData rightData = right.Data;
                        ArrayData leftData = left.Data;
                        for (int ii = 0; ii < leftData.Buffers.Length; ii++)
                        {
                            if (!leftData.Buffers[ii].Equals(rightData.Buffers[ii]))
                            {
                                Equal = false;
                                break;
                            }
                        }
                    }
                    else
                    {
                        Equal = false;
                    }
                }

                public void Visit(Int8Array left) => VisitPrimitiveArray(left);
                public void Visit(Int32Array left) => VisitPrimitiveArray(left);

                public void Visit(IArrowArray left)
                {
                    left.Accept(this);
                }
            }

            void CheckRoundTrip(RecordBatch batch)
            {
                Schema schema = batch.Schema;
                ArrowStreamTests.CheckRoundTrip(schema);

                var stream = new MemoryStream();
                var writer = new ArrowStreamWriter(stream, schema);
                writer.WriteRecordBatchAsync(batch, System.Threading.CancellationToken.None).Wait();

                stream.Position = 0;
                var reader = new ArrowStreamReader(stream);
                RecordBatch readInBatch = reader.ReadNextRecordBatchAsync().Result;

                Assert.Equal(batch.ColumnCount, readInBatch.ColumnCount);
                Assert.True(SchemaComparer.Equals(batch.Schema, readInBatch.Schema));

                for (int i = 0; i < batch.ColumnCount; i++)
                {
                    var batchColumn = batch.Column(i);
                    var readInColumn = readInBatch.Column(i);

                    ArrowArrayTestsVisitor visitor = new ArrowArrayTestsVisitor(readInColumn);
                    visitor.Visit(batchColumn);
                    Assert.True(visitor.Equal);
                }
            }

            [Fact]
            public void CheckBatchRoundTrip()
            {
                Field f0 = new Field.Builder().Name("f0").DataType(Int32Type.Default).Build();
                //Field f1 = new Field.Builder().Name("f1").DataType(Int16Type.Default).Build();
                //Field f2 = new Field.Builder().Name("f2").DataType(Int32Type.Default).Build();
                //Field f3 = new Field.Builder().Name("f3").DataType(Int64Type.Default).Build();
                //Field f4 = new Field.Builder().Name("f4").DataType(UInt8Type.Default).Build();
                //Field f5 = new Field.Builder().Name("f5").DataType(UInt16Type.Default).Build();
                //Field f6 = new Field.Builder().Name("f6").DataType(UInt32Type.Default).Build();
                //Field f7 = new Field.Builder().Name("f7").DataType(UInt64Type.Default).Build();
                //Field f8 = new Field.Builder().Name("f8").DataType(FloatType.Default).Build();
                //Field f9 = new Field.Builder().Name("f9").DataType(DoubleType.Default).Build();
                //Field f10 = new Field.Builder().Name("f10").DataType(BooleanType.Default).Build();

                Schema schema = new Schema.Builder()
                                    .Field(f0)
                                    //.Field(f1)
                                    //.Field(f2)
                                    //.Field(f3)
                                    //.Field(f4)
                                    //.Field(f5)
                                    //.Field(f6)
                                    //.Field(f7)
                                    //.Field(f8)
                                    //.Field(f9)
                                    //.Field(f10)
                                    .Build();
                var builder = new ArrowBuffer.Builder<int>();
                var data = Enumerable.Range(0, 10).Select(x => x).ToArray();

                builder.AppendRange(data);
                builder.Clear();

                var buffer = builder.Build();

                ArrayData intData = new ArrayData(Int32Type.Default, 2, 0, 0, new List<ArrowBuffer>() { buffer, ArrowBuffer.Empty });
                IArrowArray intArray = ArrowArrayFactory.BuildArray(intData);

                RecordBatch recordBatch = new RecordBatch(schema, new List<IArrowArray>() { intArray }, 1);
                CheckRoundTrip(recordBatch);
            }
        }

    }
}
