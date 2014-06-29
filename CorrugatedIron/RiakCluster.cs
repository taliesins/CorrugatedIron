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

using CorrugatedIron.Comms;
using CorrugatedIron.Comms.LoadBalancing;
using CorrugatedIron.Config;
using CorrugatedIron.Messages;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CorrugatedIron
{
    public class RiakCluster : RiakEndPoint
    {
        private readonly RoundRobinStrategy _loadBalancer;
        private readonly List<IRiakNode> _nodes;
        private readonly ConcurrentQueue<IRiakNode> _offlineNodes;
        private readonly int _nodePollTime;
        private readonly int _defaultRetryCount;
        private bool _disposing;

        protected override int DefaultRetryCount
        {
            get { return _defaultRetryCount; }
        }

        public RiakCluster(IRiakClusterConfiguration clusterConfiguration, IRiakConnectionFactory connectionFactory)
        {
            _nodePollTime = clusterConfiguration.NodePollTime;

            _nodes =
                clusterConfiguration.RiakNodes.Select(rn => new RiakNode(rn, connectionFactory))
                    .Cast<IRiakNode>()
                    .ToList();

            _loadBalancer = new RoundRobinStrategy();
            _loadBalancer.Initialise(_nodes);
            _offlineNodes = new ConcurrentQueue<IRiakNode>();
            _defaultRetryCount = clusterConfiguration.DefaultRetryCount;
            RetryWaitTime = clusterConfiguration.DefaultRetryWaitTime;

            Task.Factory.StartNew(NodeMonitor);
        }

        /// <summary>
        /// Creates an instance of <see cref="CorrugatedIron.IRiakClient"/> populated from from the configuration section
        /// specified by <paramref name="configSectionName"/>.
        /// </summary>
        /// <param name="configSectionName">The name of the configuration section to load the settings from.</param>
        /// <returns>A fully configured <see cref="IRiakEndPoint"/></returns>
        public static IRiakEndPoint FromConfig(string configSectionName)
        {
            return new RiakCluster(RiakClusterConfiguration.LoadFromConfig(configSectionName), new RiakConnectionFactory());
        }

        /// <summary>
        /// Creates an instance of <see cref="CorrugatedIron.IRiakClient"/> populated from from the configuration section
        /// specified by <paramref name="configSectionName"/>.
        /// </summary>
        /// <param name="configSectionName">The name of the configuration section to load the settings from.</param>
        /// <param name="configFileName">The full path and name of the config file to load the configuration from.</param>
        /// <returns>A fully configured <see cref="IRiakEndPoint"/></returns>
        public static IRiakEndPoint FromConfig(string configSectionName, string configFileName)
        {
            return new RiakCluster(RiakClusterConfiguration.LoadFromConfig(configSectionName, configFileName), new RiakConnectionFactory());
        }

        private void DeactivateNode(IRiakNode node)
        {
            lock (node)
            {
                if (!_offlineNodes.Contains(node))
                {
                    _loadBalancer.RemoveNode(node);
                    _offlineNodes.Enqueue(node);
                }
            }
        }

        private async void NodeMonitor()
        {
            while (!_disposing)
            {
                var deadNodes = new List<IRiakNode>();
                IRiakNode node = null;
                while (_offlineNodes.TryDequeue(out node) && !_disposing)
                {
                    var result = await node.UseConnection(c => c.PbcWriteRead(MessageCode.PingReq, MessageCode.PingResp));

                    if (result.IsSuccess)
                    {
                        _loadBalancer.AddNode(node);
                    }
                    else
                    {
                        deadNodes.Add(node);
                    }
                }

                if (!_disposing)
                {
                    foreach (var deadNode in deadNodes)
                    {
                        _offlineNodes.Enqueue(deadNode);
                    }

                    Thread.Sleep(_nodePollTime);
                }
            }
        }

        protected async override Task<RiakResult> UseConnection(Func<IRiakConnection, Task<RiakResult>> useFun, Func<ResultCode, string, bool, RiakResult> onError, int retryAttempts)
        {
            if (retryAttempts < 0) return onError(ResultCode.NoRetries, "Unable to access a connection on the cluster.", false);
            if (_disposing) return onError(ResultCode.ShuttingDown, "System currently shutting down", true);

            var node = _loadBalancer.SelectNode();

            if (node == null) return onError(ResultCode.ClusterOffline, "Unable to access functioning Riak node", true);

            var result = await node.UseConnection(useFun);
            if (result.IsSuccess) return result;
            RiakResult nextResult = null;
            if (result.ResultCode == ResultCode.NoConnections)
            {
                Thread.Sleep(RetryWaitTime);
                nextResult = await UseConnection(useFun, onError, retryAttempts - 1);
            }
            else if (result.ResultCode == ResultCode.CommunicationError)
            {
                if (result.NodeOffline)
                {
                    DeactivateNode(node);
                }

                Thread.Sleep(RetryWaitTime);
                nextResult = await UseConnection(useFun, onError, retryAttempts - 1);
            }

            // if the next result is successful then return that
            if (nextResult != null && nextResult.IsSuccess)
            {
                return nextResult;
            }

            // otherwise we'll return the result that we had at this call to make sure that
            // the correct/initial error is shown
            return onError(result.ResultCode, result.ErrorMessage, result.NodeOffline);
        }

        protected async override Task<RiakResult<T>> UseConnection<T>(Func<IRiakConnection, Task<RiakResult<T>>> useFun, Func<ResultCode, string, bool, RiakResult<T>> onError, int retryAttempts)
        {
            if (retryAttempts < 0) return onError(ResultCode.NoRetries, "Unable to access a connection on the cluster.", false);
            if (_disposing) return onError(ResultCode.ShuttingDown, "System currently shutting down", true);

            var node = _loadBalancer.SelectNode();

            if (node == null) return onError(ResultCode.ClusterOffline, "Unable to access functioning Riak node", true);

            var result = await node.UseConnection(useFun);
            if (result.IsSuccess) return result;
            RiakResult<T> nextResult = null;
            if (result.ResultCode == ResultCode.NoConnections)
            {
                Thread.Sleep(RetryWaitTime);
                nextResult = await UseConnection(useFun, onError, retryAttempts - 1);
            }
            else if (result.ResultCode == ResultCode.CommunicationError)
            {
                if (result.NodeOffline)
                {
                    DeactivateNode(node);
                }

                Thread.Sleep(RetryWaitTime);
                nextResult = await UseConnection(useFun, onError, retryAttempts - 1);
            }

            // if the next result is successful then return that
            if (nextResult != null && nextResult.IsSuccess)
            {
                return nextResult;
            }

            // otherwise we'll return the result that we had at this call to make sure that
            // the correct/initial error is shown
            return onError(result.ResultCode, result.ErrorMessage, result.NodeOffline);
        }

        protected async override Task<RiakResult<IObservable<T>>> UseConnection<T>(Func<IRiakConnection, Task<RiakResult<IObservable<T>>>> useFun, Func<ResultCode, string, bool, RiakResult<IObservable<T>>> onError, int retryAttempts)
        {
            if (retryAttempts < 0)
            {
                return onError(ResultCode.NoRetries, "Unable to access a connection on the cluster.", false);
            }

            if (_disposing)
            {
                return onError(ResultCode.ShuttingDown, "System currently shutting down", true);
            }

            var node = _loadBalancer.SelectNode();

            if (node == null)
            {
                return onError(ResultCode.ClusterOffline, "Unable to access functioning Riak node", true);
            }

            var result = await node.UseConnection(useFun);
            if (result.IsSuccess) return result;
            RiakResult<IObservable<T>> nextResult = null;
            if (result.ResultCode == ResultCode.NoConnections)
            {
                Thread.Sleep(RetryWaitTime);
                nextResult = await UseConnection(useFun, onError, retryAttempts - 1);
            }
            else if (result.ResultCode == ResultCode.CommunicationError)
            {
                if (result.NodeOffline)
                {
                    DeactivateNode(node);
                }

                Thread.Sleep(RetryWaitTime);
                nextResult = await UseConnection(useFun, onError, retryAttempts - 1);
            }

            // if the next result is successful then return that
            if (nextResult != null && nextResult.IsSuccess)
            {
                return nextResult;
            }

            // otherwise we'll return the result that we had at this call to make sure that
            // the correct/initial error is shown
            return onError(result.ResultCode, result.ErrorMessage, result.NodeOffline);
        }

        protected async override Task<RiakResult> UseConnection(Func<IRiakConnection, Action, Task<RiakResult>> useFun, Func<ResultCode, string, bool, RiakResult> onError, int retryAttempts)
        {
            if (retryAttempts < 0) return onError(ResultCode.NoRetries, "Unable to access a connection on the cluster.", false);
            if (_disposing) return onError(ResultCode.ShuttingDown, "System currently shutting down", true);

            var node = _loadBalancer.SelectNode();

            if (node == null)
            {
                return onError(ResultCode.ClusterOffline, "Unable to access functioning Riak node", true);
            }
            var result = await node.UseConnection(useFun);
            if (result.IsSuccess) return result;
            if (result.ResultCode == ResultCode.NoConnections)
            {
                Thread.Sleep(RetryWaitTime);
                return await UseConnection(useFun, retryAttempts - 1);
            }

            if (result.ResultCode != ResultCode.CommunicationError) return result;

            if (result.NodeOffline)
            {
                DeactivateNode(node);
            }

            Thread.Sleep(RetryWaitTime);
            return await UseConnection(useFun, retryAttempts - 1);
        }

        protected async override Task<RiakResult<T>> UseConnection<T>(Func<IRiakConnection, Action, Task<RiakResult<T>>> useFun, Func<ResultCode, string, bool, RiakResult<T>> onError, int retryAttempts)
        {
            if (retryAttempts < 0) return onError(ResultCode.NoRetries, "Unable to access a connection on the cluster.", false);
            if (_disposing) return onError(ResultCode.ShuttingDown, "System currently shutting down", true);

            var node = _loadBalancer.SelectNode();

            if (node == null)
            {
                return onError(ResultCode.ClusterOffline, "Unable to access functioning Riak node", true);
            }
            var result = await node.UseConnection(useFun);
            if (result.IsSuccess) return result;
            if (result.ResultCode == ResultCode.NoConnections)
            {
                Thread.Sleep(RetryWaitTime);
                return await UseConnection(useFun, retryAttempts - 1);
            }

            if (result.ResultCode != ResultCode.CommunicationError) return result;

            if (result.NodeOffline)
            {
                DeactivateNode(node);
            }

            Thread.Sleep(RetryWaitTime);
            return await UseConnection(useFun, retryAttempts - 1);
        }

        protected async override Task<RiakResult<IObservable<T>>> UseConnection<T>(Func<IRiakConnection, Action, Task<RiakResult<IObservable<T>>>> useFun, Func<ResultCode, string, bool, RiakResult<IObservable<T>>> onError, int retryAttempts)
        {
            if (retryAttempts < 0) return onError(ResultCode.NoRetries, "Unable to access a connection on the cluster.", false);
            if (_disposing) return onError(ResultCode.ShuttingDown, "System currently shutting down", true);

            var node = _loadBalancer.SelectNode();

            if (node == null)
            {
                return onError(ResultCode.ClusterOffline, "Unable to access functioning Riak node", true);
            }
            var result = await node.UseConnection(useFun);
            if (result.IsSuccess) return result;
            if (result.ResultCode == ResultCode.NoConnections)
            {
                Thread.Sleep(RetryWaitTime);
                return await UseConnection(useFun, retryAttempts - 1);
            }

            if (result.ResultCode != ResultCode.CommunicationError) return result;

            if (result.NodeOffline)
            {
                DeactivateNode(node);
            }

            Thread.Sleep(RetryWaitTime);
            return await UseConnection(useFun, retryAttempts - 1);
        }

        public override void Dispose()
        {
            _disposing = true;
            _nodes.ForEach(n => n.Dispose());
        }
    }
}
