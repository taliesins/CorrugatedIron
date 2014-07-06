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

namespace CorrugatedIron.Comms
{
    public class RiakNode : IRiakNode
    {
        private readonly IRiakConnectionManager _connectionManager;

        public RiakNode(IRiakNodeConfiguration nodeConfiguration)
        {
            // assume that if the node has a pool size of 0 then the intent is to have the connections
            // made on the fly
            if (nodeConfiguration.PoolSize == 0)
            {
                _connectionManager = new RiakOnTheFlyConnection(nodeConfiguration);
            }
            else
            {
                _connectionManager = new RiakConnectionPool(nodeConfiguration);
            }
        }

        public RiakPbcSocket CreateSocket()
        {
            return _connectionManager.CreateSocket();
        }

        public void Release(RiakPbcSocket socket)
        {
            _connectionManager.Release(socket);
        }

        public async Task GetSingleResultViaPbc(Func<RiakPbcSocket, Task> useFun)
        {
            var socket = _connectionManager.CreateSocket();
            try
            {
                await useFun(socket);
            }
            finally
            {
                _connectionManager.Release(socket);
            }
        }

        public async Task GetSingleResultViaPbc(RiakPbcSocket socket, Func<RiakPbcSocket, Task> useFun)
        {
            await useFun(socket).ConfigureAwait(false);
        }

        public async Task<TResult> GetSingleResultViaPbc<TResult>(Func<RiakPbcSocket, Task<TResult>> useFun)
        {
            var socket = _connectionManager.CreateSocket();
            try
            {
                var result = await useFun(socket).ConfigureAwait(false);
                return result;
            }
            finally
            {
                _connectionManager.Release(socket);
            }
        }

        public async Task<TResult> GetSingleResultViaPbc<TResult>(RiakPbcSocket socket, Func<RiakPbcSocket, Task<TResult>> useFun)
        {
            var result = await useFun(socket).ConfigureAwait(false);
            return result;
        }

        public async Task GetMultipleResultViaPbc(Action<RiakPbcSocket> useFun)
        {
            var socket = _connectionManager.CreateSocket();
            try
            {
                useFun(socket);
            }
            finally
            {
                _connectionManager.Release(socket);
            }
        }

        public async Task GetMultipleResultViaPbc(RiakPbcSocket socket, Action<RiakPbcSocket> useFun)
        {
            useFun(socket);
        }

        public async Task GetSingleResultViaRest(Func<string, Task> useFun)
        {
            var serverUrl = _connectionManager.CreateServerUrl();
            try
            {
                await useFun(serverUrl).ConfigureAwait(false);
            }
            finally
            {
                _connectionManager.Release(serverUrl);
            }
        }

        public async Task<TResult> GetSingleResultViaRest<TResult>(Func<string, Task<TResult>> useFun)
        {
            var serverUrl = _connectionManager.CreateServerUrl();
            try
            {
                var result = await useFun(serverUrl).ConfigureAwait(false);
                return result;
            }
            finally
            {
                _connectionManager.Release(serverUrl);
            }
        }

        public async Task GetMultipleResultViaRest(Action<string> useFun)
        {
            var serverUrl = _connectionManager.CreateServerUrl();
            try
            {
                useFun(serverUrl);
            }
            finally
            {
                _connectionManager.Release(serverUrl);
            }
        }

        public void Dispose()
        {
            _connectionManager.Dispose();
        }
    }
}