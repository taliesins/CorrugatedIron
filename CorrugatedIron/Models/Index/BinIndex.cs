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

using CorrugatedIron.Util;

namespace CorrugatedIron.Models.Index
{
    public class BinIndex : SecondaryIndex<BinIndex, string>
    {
        internal BinIndex(RiakObject container, string name)
            : base(container, name)
        {
        }

        protected override BinIndex TypedThis
        {
            get { return this; }
        }

        protected override string IndexSuffix
        {
            get { return RiakConstants.IndexSuffix.Binary; }
        }

        public RiakObject Delete()
        {
            Container.BinIndexes.Remove(Name);
            return Container;
        }
    }
}