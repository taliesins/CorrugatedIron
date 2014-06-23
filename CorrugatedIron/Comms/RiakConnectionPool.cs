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
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace CorrugatedIron.Comms
{
    internal class RiakConnectionPool : IRiakConnectionManager
    {
        private readonly List<IRiakConnection> _allResources;
        private readonly ConcurrentStack<IRiakConnection> _resources;
        private readonly SocketAwaitablePool _pool;
        private readonly BlockingBufferManager _bufferManager;
        private bool _disposing;

        public RiakConnectionPool(IRiakNodeConfiguration nodeConfig, IRiakConnectionFactory connFactory)
        {
            var poolSize = nodeConfig.PoolSize;
            _pool = new SocketAwaitablePool(nodeConfig.PoolSize);
            _bufferManager = new BlockingBufferManager(nodeConfig.BufferSize, nodeConfig.PoolSize);

            _allResources = new List<IRiakConnection>();
            _resources = new ConcurrentStack<IRiakConnection>();

            for(var i = 0; i < poolSize; ++i)
            {
                var conn = connFactory.CreateConnection(nodeConfig, _pool, _bufferManager);
                _allResources.Add(conn);
                _resources.Push(conn);
            }
        }

        public Tuple<bool, Task<TResult>> Consume<TResult>(Func<IRiakConnection, Task<TResult>> consumer)
        {
            if (_disposing) return Tuple.Create<bool, Task<TResult>>(false, null);

            IRiakConnection instance = null;
            try
            {
                if(_resources.TryPop(out instance))
                {
                    var result = consumer(instance);
                    return Tuple.Create(true, result);
                }
            }
            catch(Exception)
            {
                return Tuple.Create<bool, Task<TResult>>(false, null);
            }
            finally
            {
                if(instance != null)
                {
                    _resources.Push(instance);
                }
            }

            return Tuple.Create<bool, Task<TResult>>(false, null);
        }

        public Tuple<bool, Task<TResult>> DelayedConsume<TResult>(Func<IRiakConnection, Action, Task<TResult>> consumer)
        {
            if (_disposing) return Tuple.Create<bool, Task<TResult>>(false, null);

            IRiakConnection instance = null;
            try
            {
                if(_resources.TryPop(out instance))
                {
                    Action cleanup = () =>
                    {
                        var i = instance;
                        instance = null;
                        _resources.Push(i);
                    };

                    var result = consumer(instance, cleanup);
                    return Tuple.Create(true, result);
                }
            }
            catch(Exception)
            {
                if(instance != null)
                {
                    _resources.Push(instance);
                }
                return Tuple.Create<bool, Task<TResult>>(false, null);
            }

            return Tuple.Create<bool, Task<TResult>>(false, null);
        }

        public void Dispose()
        {
            if(_disposing) return;

            _disposing = true;

            foreach(var conn in _allResources)
            {
                conn.Dispose();
            }
        }
    }
}
