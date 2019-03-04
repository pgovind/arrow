using System;
using System.Collections.Generic;
using System.Text;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    internal class ChunkedArray
    {
        public readonly IList<Array> Arrays;
        public readonly IArrowType DataType;
        public readonly int Length;
        public readonly int NullCount;

        public ChunkedArray(IList<Array> arrays)
        {
            Arrays = arrays ?? throw new ArgumentNullException(nameof(arrays));
            if (arrays.Count < 1)
            {
                throw new ArgumentException($"Count must be atleast 1. Got {arrays.Count} instead");
            }
            DataType = arrays[0].Data.DataType;
            foreach (Array array in arrays)
            {
                Length += array.Length;
                NullCount += array.NullCount;
            }
        }

        public ChunkedArray(Array array) : this(new[] { array }) { }

        public ChunkedArray Slice(int offset, int length)
        {
            if (offset > Length)
            {
                throw new ArgumentException($"Offset {offset} cannot be greater than Length {Length} for ChunkedArray.Slice");
            }
            int curArrayIndex = 0;
            int numArrays = Arrays.Count;
            while (curArrayIndex < numArrays && offset > Arrays[curArrayIndex].Length)
            {
                offset -= Arrays[curArrayIndex].Length;
                curArrayIndex++;
            }

            IList<Array> newArrays = new List<Array>();
            while (curArrayIndex < numArrays && length > 0)
            {
                newArrays.Add(Arrays[curArrayIndex].Slice(offset, length));
                length -= Arrays[curArrayIndex].Length - offset;
                offset = 0;
                curArrayIndex++;
            }
            return new ChunkedArray(newArrays);
        }
        public ChunkedArray Slice(int offset)
        {
            return Slice(offset, Length);
        }

        // TODO: Flatten for Structs
    }
}
