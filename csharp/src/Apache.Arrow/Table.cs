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
using System.Text;
using Apache.Arrow.Types;

namespace Apache.Arrow
{

    public class Table
    {
        public Schema Schema;
        public int NumRows = 0;
        public int NumColumns = 0;
        public Column Column(int columnIndex) => _columns[columnIndex];

        private IList<Column> _columns;
        public static Table TableFromRecordBatches(Schema schema, IList<RecordBatch> recordBatches)
        {
            int nBatches = recordBatches.Count;
            int nColumns = schema.Fields.Count;

            List<Column> columns = new List<Column>();
            List<Array> columnArrays = new List<Array>();
            for (int icol = 0; icol < nColumns; icol++)
            {
                for (int jj = 0; jj < nBatches; jj++)
                {
                    columnArrays.Add(recordBatches[jj].Column(icol) as Array);
                }
                columns.Add(new Arrow.Column(schema.GetFieldByIndex(icol), columnArrays));
                columnArrays.Clear();
            }
            return new Table(schema, columns);
        }

        public Table(Schema schema, IList<Column> columns)
        {
            Schema = schema;
            _columns = columns;
            if (columns.Count > 0)
            {
                NumRows = columns[0].Length;
                NumColumns = columns.Count;
            }
        }

        public Table()
        {
            Schema = new Schema.Builder().Build();
            _columns = new List<Column>();
        }


        public void RemoveColumn(int columnIndex)
        {
            // Does not return a new table.
            Field field = Schema.GetFieldByIndex(columnIndex);
            Schema.RemoveField(columnIndex);
            _columns.RemoveAt(columnIndex);
            NumColumns--;
        }

        public void AddColumn(int columnIndex, Column column)
        {
            // Does not return a new table
            column = column ?? throw new ArgumentNullException(nameof(column));
            if (columnIndex < 0 || columnIndex > _columns.Count)
            {
                throw new ArgumentException($"Invalid columnIndex {columnIndex} passed into Table.AddColumn");
            }
            if (column.Length != NumRows)
            {
                throw new ArgumentException($"Column's length {column.Length} must match Table's length {NumRows}");
            }
            Schema.AddField(column.Field, columnIndex);

            IList<Column> newColumns = new List<Column>();
            for (int ii = 0; ii < columnIndex; ii++)
            {
                newColumns.Add(Column(ii));
            }
            newColumns.Add(column);
            for (int ii = columnIndex; ii < _columns.Count; ii++)
            {
                newColumns.Add(Column(ii));
            }
            _columns = newColumns;
            NumColumns++;
        }

        public void SetColumn(int columnIndex, Column column)
        {
            column = column ?? throw new ArgumentNullException(nameof(column));
            if (columnIndex < 0 || columnIndex >= NumColumns)
            {
                throw new ArgumentException($"Invalid columnIndex {columnIndex} passed in to Table.SetColumn");
            }
            if (column.Length != NumRows)
            {
                throw new ArgumentException($"Column's length {column.Length} must match table's length {NumRows}");
            }
            int fieldIndex = Schema.GetFieldIndex(column.Name);
            Schema.SetField(fieldIndex, column.Field);
            _columns[columnIndex] = column;
        }

        // TODO: Flatten for Tables with Lists/Structs?

        public bool Validate()
        {
            if (_columns.Count != Schema.Fields.Count)
            {
                return false;
            }
            for (int ii = 0; ii < _columns.Count; ii++)
            {
                Column column = Column(ii);
                if (column == null)
                {
                    return false;
                }
                // TODO: Implement Equals() for Column here to compare with Schema.Field?
                if (column.Length != NumRows)
                {
                    return false;
                }
            }
            return true;
        }
    }
}
