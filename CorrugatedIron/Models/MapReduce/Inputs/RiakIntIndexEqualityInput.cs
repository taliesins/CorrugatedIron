// Copyright (c) 2011 - OJ Reeves & Jeremiah Peschka
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using CorrugatedIron.Extensions;
using Newtonsoft.Json;

namespace CorrugatedIron.Models.MapReduce.Inputs
{
    public class RiakIntIndexEqualityInput : RiakIndexInput
    {
        public string Bucket { get; set; }
        public string Index { get; set; }
        public int Key { get; set; }

        public RiakIntIndexEqualityInput(string bucket, string index, int key)
        {
            Bucket = bucket;
            Index = index.ToIntegerKey();
            Key = key;
        }

        public override JsonWriter WriteJson(JsonWriter writer)
        {
            writer.WritePropertyName("inputs");

            writer.WriteStartObject();
            writer.WritePropertyName("bucket");
            writer.WriteValue(Bucket);

            writer.WritePropertyName("index");
            writer.WriteValue(Index);

            writer.WritePropertyName("key");
            writer.WriteValue(Key);
            writer.WriteEndObject();

            return writer;
        }
    }
}
