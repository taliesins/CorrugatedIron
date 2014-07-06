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
using System.Linq;
using System.Reactive.Linq;
using CorrugatedIron.Messages;
using System.Collections.Generic;

namespace CorrugatedIron.Models.MapReduce
{
    public class RiakStreamedMapReduceResult : IRiakMapReduceResult
    {
        private readonly IEnumerable<RiakMapReduceResultPhase> _responseReader;

        internal RiakStreamedMapReduceResult(IObservable<RpbMapRedResp> responseReader)
        {
            _responseReader = responseReader
                .Select(item => new RiakMapReduceResultPhase(item.phase, new List<RpbMapRedResp> { item }))
                .ToEnumerable()
                .ToList();
        }

        public IEnumerable<RiakMapReduceResultPhase> PhaseResults
        {
            get { return _responseReader; }
        }
    }
}