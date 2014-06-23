using System;

namespace CorrugatedIron
{
    public interface IRiakClient : IRiakBatchClient
    {
        int RetryCount { get; set; }
        void Batch(Action<IRiakBatchClient> batchAction);
        T Batch<T>(Func<IRiakBatchClient, T> batchFunction);
        IRiakAsyncClient Async { get; }
    }
}