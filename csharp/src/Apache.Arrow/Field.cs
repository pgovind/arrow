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
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public partial class Field: IEquatable<Field>
    {
        public IArrowType DataType { get; }

        public string Name { get; }

        public bool IsNullable { get; }

        public bool HasMetadata => Metadata?.Count > 0;

        public IReadOnlyDictionary<string, string> Metadata { get; }

        public Field(string name, IArrowType dataType, bool nullable,
            IEnumerable<KeyValuePair<string, string>> metadata = default)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            Name = name;
            DataType = dataType ?? NullType.Default;
            IsNullable = nullable;
            Metadata = metadata?.ToDictionary(kv => kv.Key, kv => kv.Value);
        }

        public override bool Equals(object obj)
        {
            if (obj == null || !this.GetType().Equals(obj.GetType()))
            {
                return false;
            }
            return Equals((Field)obj);
        }

        public bool Equals(Field other)
        {
            if (this == other)
            {
                return true;
            }
            if (this.Name == other.Name && this.IsNullable == other.IsNullable &&
                this.DataType.TypeId == other.DataType.TypeId)
            {
                if (this.HasMetadata && other.HasMetadata)
                {
                    return this.Metadata.Keys.Count() == other.Metadata.Keys.Count() &&
                           this.Metadata.Keys.All(k => other.Metadata.ContainsKey(k) && this.Metadata[k] == other.Metadata[k]) &&
                           other.Metadata.Keys.All(k => this.Metadata.ContainsKey(k) && other.Metadata[k] == this.Metadata[k]);
                    //return this.Metadata.OrderBy(r => r.Key).SequenceEqual(other.Metadata.OrderBy(r => r.Key));
                }
                else if (!this.HasMetadata && !other.HasMetadata) {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            return false;
        }

        public override int GetHashCode()
        {
            string concatenatedString = DataType.Name;
            concatenatedString += Name + IsNullable;
            if (this.HasMetadata)
            {
                var orderedMetadata = this.Metadata.OrderBy(kv => kv.Key);
                foreach (var kv in orderedMetadata)
                {
                    concatenatedString += kv.Key + kv.Value;
                }
            }
            return concatenatedString.GetHashCode();
        }
    }
}
