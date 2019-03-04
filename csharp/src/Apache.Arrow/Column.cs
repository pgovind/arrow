using System;
using System.Collections.Generic;
using System.Text;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public class Column
    {
        public readonly Field Field;
        private readonly ChunkedArray _columnArrays;

        public Column(Field field, IList<Array> arrays)
        {
            _columnArrays = new ChunkedArray(arrays);
            Field = field;
            if (!Validate())
            {
                throw new ArgumentException($"{Field.DataType} must match {_columnArrays.DataType}");
            }
        }

        private Column(Field field, ChunkedArray arrays)
        {
            Field = field;
            _columnArrays = arrays;
        }

        public int Length => _columnArrays.Length;
        public int NullCount => _columnArrays.NullCount;
        public string Name => Field.Name;
        public IArrowType Type => Field.DataType;

        public Column Slice(int offset, int length)
        {
            return new Column(Field, _columnArrays.Slice(offset, length));
        }

        public Column Slice(int offset)
        {
            return new Column(Field, _columnArrays.Slice(offset));
        }

        public bool Validate()
        {
            for (int ii = 0; ii < _columnArrays.Arrays.Count; ii++)
            {
                if (_columnArrays.Arrays[ii].Data.DataType != Field.DataType)
                {
                    return false;
                }
            }
            return true;
        }
    }
}
