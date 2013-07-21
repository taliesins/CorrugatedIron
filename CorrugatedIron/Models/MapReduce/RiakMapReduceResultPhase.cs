﻿// Copyright (c) 2011 - OJ Reeves & Jeremiah Peschka
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

using System;
using CorrugatedIron.Extensions;
using CorrugatedIron.Messages;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Linq;

namespace CorrugatedIron.Models.MapReduce
{
    public class RiakMapReduceResultPhase
    {
        public bool Success { get; private set; }
        public uint Phase { get; private set; }
        public List<byte[]> Values { get; private set; }

        internal RiakMapReduceResultPhase(uint phase, IEnumerable<RpbMapRedResp> results)
        {
            Phase = phase;
            Values = results.Select(r => r.response).Where(b => b != null).ToList();
            Success = true;
        }

        internal RiakMapReduceResultPhase()
        {
            Success = false;
        }

        /// <summary>
        /// Deserialize a List of <typeparam name="T">T</typeparam> from the phase results
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns>IList<typeparam name="T">T</typeparam></returns>
        public IList<T> GetObjects<T>()
        {
            var valuesString = Values.Select(v => v.FromRiakString());
            var rVal = valuesString.Select(JsonConvert.DeserializeObject<T>).ToList();
            return rVal;
        }

        /// <summary>
        /// Deserialize a List of <see cref="CorrugatedIron.Models.RiakObjectId"/> from $key query
        /// </summary>
        /// <returns>IList of <see cref="CorrugatedIron.Models.RiakObjectId"/></returns>
        /// <remarks>This is designed specifically to deal with the data structure that is returned from
        /// Riak when querying the $key index. This should be used when querying $key directly or through
        /// one of the convenience methods.</remarks>
        public IList<RiakObjectId> GetObjectIds()
        {
            var rVal = Values.SelectMany(v => JsonConvert.DeserializeObject<string[][]>(v.FromRiakString()).Select(
                a => new RiakObjectId(a[0], a[1]))).ToList();
            return rVal;
        }

        public IEnumerable<dynamic> GetObjects()
        {
            return GetObjects<dynamic>();
        }
    }
}