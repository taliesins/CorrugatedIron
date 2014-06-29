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
using System;

namespace CorrugatedIron
{
    public abstract class RiakEndPoint : IRiakEndPoint
    {
        public int RetryWaitTime { get; set; }
        protected abstract int DefaultRetryCount { get; }

        /// <summary>
        /// Creates a new instance of <see cref="CorrugatedIron.RiakClient"/>.
        /// </summary>
        /// <returns>
        /// A minty fresh client.
        /// </returns>
        public IRiakClient CreateClient()
        {
            return new RiakClient(this) { RetryCount = DefaultRetryCount };
        }

        public Task<RiakResult> UseConnection(Func<IRiakConnection, Task<RiakResult>> useFun, int retryAttempts)
        {
            return UseConnection(useFun, RiakResult.Error, retryAttempts);
        }

        public Task<RiakResult<TResult>> UseConnection<TResult>(Func<IRiakConnection, Task<RiakResult<TResult>>> useFun, int retryAttempts)
        {
            return UseConnection(useFun, RiakResult<TResult>.Error, retryAttempts);
        }

        public Task<RiakResult<IObservable<TResult>>> UseConnection<TResult>(Func<IRiakConnection, Task<RiakResult<IObservable<TResult>>>> useFun, int retryAttempts)
        {
            return UseConnection(useFun, RiakResult<IObservable<TResult>>.Error, retryAttempts);
        }

        protected abstract Task<RiakResult> UseConnection(Func<IRiakConnection, Task<RiakResult>> useFun,
            Func<ResultCode, string, bool, RiakResult> onError, int retryAttempts);

        protected abstract Task<RiakResult<T>> UseConnection<T>(Func<IRiakConnection, Task<RiakResult<T>>> useFun,
            Func<ResultCode, string, bool, RiakResult<T>> onError, int retryAttempts);

        protected abstract Task<RiakResult<IObservable<T>>> UseConnection<T>(Func<IRiakConnection, Task<RiakResult<IObservable<T>>>> useFun, 
            Func<ResultCode, string, bool, RiakResult<IObservable<T>>> onError, int retryAttempts);
        
        public Task<RiakResult> UseConnection(Func<IRiakConnection, Action, Task<RiakResult>> useFun, int retryAttempts)
        {
            return UseConnection(useFun, RiakResult.Error, retryAttempts);
        }

        public Task<RiakResult<TResult>> UseConnection<TResult>(Func<IRiakConnection, Action, Task<RiakResult<TResult>>> useFun, int retryAttempts)
        {
            return UseConnection(useFun, RiakResult<TResult>.Error, retryAttempts);
        }

        public Task<RiakResult<IObservable<TResult>>> UseConnection<TResult>(Func<IRiakConnection, Action, Task<RiakResult<IObservable<TResult>>>> useFun, int retryAttempts)
        {
            return UseConnection(useFun, RiakResult<IObservable<TResult>>.Error, retryAttempts);
        }

        protected abstract Task<RiakResult> UseConnection(Func<IRiakConnection, Action, Task<RiakResult>> useFun,
            Func<ResultCode, string, bool, RiakResult> onError, int retryAttempts);

        protected abstract Task<RiakResult<T>> UseConnection<T>(Func<IRiakConnection, Action, Task<RiakResult<T>>> useFun,
            Func<ResultCode, string, bool, RiakResult<T>> onError, int retryAttempts);

        protected abstract Task<RiakResult<IObservable<T>>> UseConnection<T>(Func<IRiakConnection, Action, Task<RiakResult<IObservable<T>>>> useFun,
            Func<ResultCode, string, bool, RiakResult<IObservable<T>>> onError, int retryAttempts);

        public abstract void Dispose();
    }
}