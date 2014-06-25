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
using CorrugatedIron.Comms;
using CorrugatedIron.Config;
using System;
using System.Collections.Generic;
using System.Threading;

namespace CorrugatedIron
{
    public class RiakExternalLoadBalancer : RiakEndPoint
    {
        private readonly IRiakExternalLoadBalancerConfiguration _lbConfiguration;
        private readonly RiakNode _node;
        private bool _disposing;

        public RiakExternalLoadBalancer(IRiakExternalLoadBalancerConfiguration lbConfiguration, IRiakConnectionFactory connectionFactory)
        {
            _lbConfiguration = lbConfiguration;
            _node = new RiakNode(_lbConfiguration.Target, connectionFactory);
        }

        public static IRiakEndPoint FromConfig(string configSectionName)
        {
            return new RiakExternalLoadBalancer(RiakExternalLoadBalancerConfiguration.LoadFromConfig(configSectionName), new RiakConnectionFactory());
        }

        public static IRiakEndPoint FromConfig(string configSectionName, string configFileName)
        {
            return new RiakExternalLoadBalancer(RiakExternalLoadBalancerConfiguration.LoadFromConfig(configSectionName, configFileName), new RiakConnectionFactory());
        }

        protected override int DefaultRetryCount
        {
            get { return _lbConfiguration.DefaultRetryCount; }
        }

        protected async override Task<RiakResult> UseConnection(Func<IRiakConnection, Task<RiakResult>> useFun, Func<ResultCode, string, bool, RiakResult> onError, int retryAttempts)
        {
            if (retryAttempts < 0)
            {
                return onError(ResultCode.NoRetries, "Unable to access a connection on the cluster.", true);
            }
            if (_disposing)
            {
                return onError(ResultCode.ShuttingDown, "System currently shutting down", true);
            }

            var node = _node;

            if (node != null)
            {
                var result = await node.UseConnection(useFun);
                if (!result.IsSuccess)
                {
                    Thread.Sleep(RetryWaitTime);
                    return await UseConnection(useFun, onError, retryAttempts - 1);
                }

                return result;
            }
            return onError(ResultCode.ClusterOffline, "Unable to access functioning Riak node", true);
        }

        protected async override Task<RiakResult<T>> UseConnection<T>(Func<IRiakConnection, Task<RiakResult<T>>> useFun, Func<ResultCode, string, bool, RiakResult<T>> onError, int retryAttempts)
        {
            if (retryAttempts < 0)
            {
                return onError(ResultCode.NoRetries, "Unable to access a connection on the cluster.", true);
            }
            if (_disposing)
            {
                return onError(ResultCode.ShuttingDown, "System currently shutting down", true);
            }

            var node = _node;

            if (node != null)
            {
                var result = await node.UseConnection(useFun);
                if (!result.IsSuccess)
                {
                    Thread.Sleep(RetryWaitTime);
                    return await UseConnection(useFun, onError, retryAttempts - 1);
                }

                return result;
            }
            return onError(ResultCode.ClusterOffline, "Unable to access functioning Riak node", true);
        }

        protected async override Task<RiakResult<IEnumerable<T>>> UseConnection<T>(Func<IRiakConnection, Task<RiakResult<IEnumerable<T>>>> useFun, Func<ResultCode, string, bool, RiakResult<IEnumerable<T>>> onError, int retryAttempts)
        {
            if (retryAttempts < 0)
            {
                return onError(ResultCode.NoRetries, "Unable to access a connection on the cluster.", true);
            }
            if (_disposing)
            {
                return onError(ResultCode.ShuttingDown, "System currently shutting down", true);
            }

            var node = _node;

            if (node != null)
            {
                var result = await node.UseConnection(useFun);
                if (!result.IsSuccess)
                {
                    Thread.Sleep(RetryWaitTime);
                    return await UseConnection(useFun, onError, retryAttempts - 1);
                }

                return result;
            }
            return onError(ResultCode.ClusterOffline, "Unable to access functioning Riak node", true);
        }

        protected async override Task<RiakResult> UseConnection(Func<IRiakConnection, Action, Task<RiakResult>> useFun, Func<ResultCode, string, bool, RiakResult> onError, int retryAttempts)
        {
            if (retryAttempts < 0)
            {
                return onError(ResultCode.NoRetries, "Unable to access a connection on the cluster.", true);
            }
            if (_disposing)
            {
                return onError(ResultCode.ShuttingDown, "System currently shutting down", true);
            }

            var node = _node;

            if (node != null)
            {
                var result = await node.UseConnection(useFun);
                if (!result.IsSuccess)
                {
                    Thread.Sleep(RetryWaitTime);
                    return await UseConnection(useFun, retryAttempts - 1);
                }
                return result;
            }
            return onError(ResultCode.ClusterOffline, "Unable to access functioning Riak node", true);
        }

        protected async override Task<RiakResult<T>> UseConnection<T>(Func<IRiakConnection, Action, Task<RiakResult<T>>> useFun, Func<ResultCode, string, bool, RiakResult<T>> onError, int retryAttempts)
        {
            if (retryAttempts < 0)
            {
                return onError(ResultCode.NoRetries, "Unable to access a connection on the cluster.", true);
            }
            if (_disposing)
            {
                return onError(ResultCode.ShuttingDown, "System currently shutting down", true);
            }

            var node = _node;

            if (node != null)
            {
                var result = await node.UseConnection(useFun);
                if (!result.IsSuccess)
                {
                    Thread.Sleep(RetryWaitTime);
                    return await UseConnection(useFun, retryAttempts - 1);
                }
                return result;
            }
            return onError(ResultCode.ClusterOffline, "Unable to access functioning Riak node", true);
        }

        protected async override Task<RiakResult<IEnumerable<T>>> UseConnection<T>(Func<IRiakConnection, Action, Task<RiakResult<IEnumerable<T>>>> useFun, Func<ResultCode, string, bool, RiakResult<IEnumerable<T>>> onError, int retryAttempts)
        {
            if (retryAttempts < 0)
            {
                return onError(ResultCode.NoRetries, "Unable to access a connection on the cluster.", true);
            }
            if (_disposing)
            {
                return onError(ResultCode.ShuttingDown, "System currently shutting down", true);
            }

            var node = _node;

            if (node != null)
            {
                var result = await node.UseConnection(useFun);
                if (!result.IsSuccess)
                {
                    Thread.Sleep(RetryWaitTime);
                    return await UseConnection(useFun, retryAttempts - 1);
                }
                return result;
            }
            return onError(ResultCode.ClusterOffline, "Unable to access functioning Riak node", true);
        }

        public override void Dispose()
        {
            _disposing = true;

            _node.Dispose();
        }
    }
}