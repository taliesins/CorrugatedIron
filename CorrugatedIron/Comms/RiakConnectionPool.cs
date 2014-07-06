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

using CorrugatedIron.Comms.Sockets;
using CorrugatedIron.Config;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using CorrugatedIron.Extensions;

namespace CorrugatedIron.Comms
{
    internal class RiakConnectionPool : IRiakConnectionManager
    {
        private readonly List<RiakPbcSocket> _allResources;
        private readonly BlockingCollection<RiakPbcSocket> _resources;
        private readonly string _serverUrl;
        private bool _disposing;

        public RiakConnectionPool(IRiakNodeConfiguration nodeConfig)
        {
            var poolSize = nodeConfig.PoolSize;
            var pool = new SocketAwaitablePool(nodeConfig.PoolSize);
            var bufferManager = new BlockingBufferManager(nodeConfig.BufferSize, nodeConfig.PoolSize);
            _serverUrl = @"{0}://{1}:{2}".Fmt(nodeConfig.RestScheme, nodeConfig.HostAddress, nodeConfig.RestPort);
            _allResources = new List<RiakPbcSocket>();

            for(var i = 0; i < poolSize; ++i)
            {
                var socket = new RiakPbcSocket(
                    nodeConfig.HostAddress,
                    nodeConfig.PbcPort,
                    nodeConfig.NetworkReadTimeout,
                    nodeConfig.NetworkWriteTimeout,
                    pool,
                    bufferManager);

                _allResources.Add(socket);
            }

            _resources = new BlockingCollection<RiakPbcSocket>(new ConcurrentQueue<RiakPbcSocket>(_allResources));
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

        public string CreateServerUrl()
        {
            return _serverUrl;
        }

        public void Release(string serverUrl)
        {
        }

        public RiakPbcSocket CreateSocket()
        {
            if (_disposing) throw new ObjectDisposedException(this.GetType().Name);

            RiakPbcSocket socket = null;

            if (_resources.TryTake(out socket, -1))
            {
                return socket;
            }

            throw new TimeoutException("Unable to create socket");
 
        }

        public void Release(RiakPbcSocket socket)
        {
            if (_disposing) return;

            _resources.Add(socket);
        }
    }
}
