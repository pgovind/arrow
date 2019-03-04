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

namespace Apache.Arrow
{
    public partial class Schema
    {
        public IReadOnlyDictionary<string, Field> Fields => (IReadOnlyDictionary<string, Field>)_fieldsDictionary;
        public IReadOnlyDictionary<string, string> Metadata { get; }

        public bool HasMetadata =>
            Metadata != null && Metadata.Count > 0;

        private IList<Field> _fields;
        private IDictionary<string, Field> _fieldsDictionary;
        public Schema(
            IEnumerable<Field> fields,
            IEnumerable<KeyValuePair<string, string>> metadata)
        {
            if (fields == null)
            {
                throw new ArgumentNullException(nameof(fields));
            }

            _fields = fields.ToList();

            _fieldsDictionary = fields.ToDictionary(
                field => field.Name, field => field,
                StringComparer.OrdinalIgnoreCase);

            Metadata = metadata?.ToDictionary(kv => kv.Key, kv => kv.Value);
        }

        public Field GetFieldByIndex(int i)
        {
            return _fields[i];
        }

        public Field GetFieldByName(string name) =>
            Fields.TryGetValue(name, out var field) ? field : null;

        public int GetFieldIndex(string name, StringComparer comparer = default)
        {
            if (comparer == null)
                comparer = StringComparer.CurrentCulture;

            return _fields.IndexOf(
                _fields.Single(x => comparer.Compare(x.Name, name) == 0));
        }

        public void RemoveField(int fieldIndex)
        {
            if (fieldIndex < 0 || fieldIndex > _fields.Count)
            {
                throw new ArgumentException(nameof(fieldIndex), "Invalid fieldIndex");
            }
            _fieldsDictionary.Remove(_fields[fieldIndex].Name);
            _fields.RemoveAt(fieldIndex);
        }

        public void AddField(Field newField, int fieldIndex)
        {
            newField = newField ?? throw new ArgumentNullException(nameof(newField));
            if (fieldIndex < 0 || fieldIndex > _fields.Count)
            {
                throw new ArgumentException(nameof(fieldIndex), $"Invalid fieldIndex {fieldIndex} passed in to Schema.AddField");
            }

            List<Field> newFields = new List<Field>();
            for (int ii = 0; ii < fieldIndex; ii++)
            {
                newFields.Add(_fields[ii]);
            }
            newFields.Add(newField);
            for (int ii = fieldIndex; ii < _fields.Count; ii++)
            {
                newFields.Add(newField);
            }
            _fields = newFields;
            _fieldsDictionary.Add(newField.Name, newField);
        }

        public void SetField(int fieldIndex, Field newField)
        {
            if (fieldIndex <0 || fieldIndex >= Fields.Count)
            {
                throw new ArgumentException($"Invalid fieldIndex {fieldIndex} passed in to Schema.SetColumn");
            }
            Field oldField = GetFieldByIndex(fieldIndex);
            _fields[fieldIndex] = newField ?? throw new ArgumentNullException(nameof(newField));
            _fieldsDictionary.Remove(oldField.Name);
            _fieldsDictionary.Add(newField.Name, newField);
        }
    }
}
