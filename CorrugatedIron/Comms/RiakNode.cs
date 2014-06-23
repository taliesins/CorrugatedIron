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

using System.Threading.Tasks;
using CorrugatedIron.Config;
using System;
using System.Collections.Generic;

namespace CorrugatedIron.Comms
{
    public class RiakNode : IRiakNode
    {
        private readonly IRiakConnectionManager _connections;
        private bool _disposing;

        public RiakNode(IRiakNodeConfiguration nodeConfiguration, IRiakConnectionFactory connectionFactory)
        {
            // assume that if the node has a pool size of 0 then the intent is to have the connections
            // made on the fly
            if (nodeConfiguration.PoolSize == 0)
            {
                _connections = new RiakOnTheFlyConnection(nodeConfiguration, connectionFactory);
            }
            else
            {
                _connections = new RiakConnectionPool(nodeConfiguration, connectionFactory);
            }
        }

        public Task<RiakResult> UseConnection(Func<IRiakConnection, Task<RiakResult>> useFun)
        {
            if (_disposing) return Task.FromResult(RiakResult.Error(ResultCode.ShuttingDown, "Connection is shutting down", true));

            var response = _connections.Consume(useFun);
            if (response.Item1)
            {
                return response.Item2;
            }
            return Task.FromResult(RiakResult.Error(ResultCode.NoConnections, "Unable to acquire connection", true));
        }

        public Task<RiakResult<TResult>> UseConnection<TResult>(Func<IRiakConnection, Task<RiakResult<TResult>>> useFun)
        {
            if (_disposing) return Task.FromResult(RiakResult<TResult>.Error(ResultCode.ShuttingDown, "Connection is shutting down", true));

            var response = _connections.Consume(useFun);
            if (response.Item1)
            {
                return response.Item2;
            }
            return Task.FromResult(RiakResult<TResult>.Error(ResultCode.NoConnections, "Unable to acquire connection", true));
        }

        public Task<RiakResult<IEnumerable<TResult>>> UseDelayedConnection<TResult>(Func<IRiakConnection, Action, Task<RiakResult<IEnumerable<TResult>>>> useFun)
        {
            if(_disposing) return Task.FromResult(RiakResult<IEnumerable<TResult>>.Error(ResultCode.ShuttingDown, "Connection is shutting down", true));

            var response = _connections.DelayedConsume(useFun);
            if(response.Item1)
            {
                return response.Item2;
            }
            return Task.FromResult(RiakResult<IEnumerable<TResult>>.Error(ResultCode.NoConnections, "Unable to acquire connection", true));
        }

        public void Dispose()
        {
            _disposing = true;
            _connections.Dispose();
        }
    }
}