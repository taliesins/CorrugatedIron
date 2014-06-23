// Copyright (c) 2013 - OJ Reeves & Jeremiah Peschka
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
using CorrugatedIron.Comms.Sockets;
using CorrugatedIron.Config;
using System;

namespace CorrugatedIron.Comms
{
    internal class RiakOnTheFlyConnection : IRiakConnectionManager
    {
        private readonly IRiakNodeConfiguration _nodeConfig;
        private readonly IRiakConnectionFactory _connFactory;
        private readonly SocketAwaitablePool _pool;
        private readonly BlockingBufferManager _bufferManager;
        private bool _disposing;

        public RiakOnTheFlyConnection(IRiakNodeConfiguration nodeConfig, IRiakConnectionFactory connFactory)
        {
            _nodeConfig = nodeConfig;
            _connFactory = connFactory;
            _pool = new SocketAwaitablePool(nodeConfig.PoolSize);
            _bufferManager = new BlockingBufferManager(nodeConfig.BufferSize, nodeConfig.PoolSize);
        }

        public Tuple<bool, Task<TResult>> Consume<TResult>(Func<IRiakConnection, Task<TResult>> consumer)
        {
            if (_disposing) return Tuple.Create<bool, Task<TResult>>(false, null);

            using (var conn = _connFactory.CreateConnection(_nodeConfig, _pool, _bufferManager))
            {
                try
                {
                    var result = consumer(conn);
                    return Tuple.Create(true, result);
                }
                catch(Exception)
                {
                    return Tuple.Create<bool, Task<TResult>>(false, null);
                }
            }
        }

        public Tuple<bool, Task<TResult>> DelayedConsume<TResult>(Func<IRiakConnection, Action, Task<TResult>> consumer)
        {
            if (_disposing) return Tuple.Create<bool, Task<TResult>>(false, null);

            IRiakConnection conn = null;

            try
            {
                conn = _connFactory.CreateConnection(_nodeConfig, _pool, _bufferManager);
                var result = consumer(conn, conn.Dispose);
                return Tuple.Create(true, result);
            }
            catch(Exception)
            {
                if (conn != null)
                {
                    conn.Dispose();
                }
                return Tuple.Create<bool, Task<TResult>>(false, null);
            }
        }

        public void Dispose()
        {
            if(_disposing) return;

            _disposing = true;
        }
    }
}
