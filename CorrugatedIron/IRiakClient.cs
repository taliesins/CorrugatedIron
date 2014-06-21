using System;

namespace CorrugatedIron
{
    public interface IRiakClient : IRiakBatchClient
    {
        void Batch(Action<IRiakBatchClient> batchAction);

        T Batch<T>(Func<IRiakBatchClient, T> batchFunction);

        IRiakAsyncClient Async { get; }
    }
}