using System;
using System.Collections.Generic;
using System.Numerics;
using System.Threading.Tasks;
using CorrugatedIron.Models;
using CorrugatedIron.Models.Index;
using CorrugatedIron.Models.MapReduce;
using CorrugatedIron.Models.Search;
using CorrugatedIron.Util;

namespace CorrugatedIron
{
    public interface IRiakAsyncClient
    {
        Task<RiakResult> Ping();

        Task<RiakResult<RiakObject>> Get(string bucket, string key, RiakGetOptions options = null);
        Task<RiakResult<RiakObject>> Get(RiakObjectId objectId, RiakGetOptions options = null);
        Task<IEnumerable<RiakResult<RiakObject>>> Get(IEnumerable<RiakObjectId> bucketKeyPairs, RiakGetOptions options = null);

        Task<RiakCounterResult> IncrementCounter(string bucket, string counter, long amount, RiakCounterUpdateOptions options = null);
        Task<RiakCounterResult> GetCounter(string bucket, string counter, RiakCounterGetOptions options = null);

        Task<RiakResult<RiakObject>> Put(RiakObject value, RiakPutOptions options = null);
        Task<IEnumerable<RiakResult<RiakObject>>> Put(IEnumerable<RiakObject> values, RiakPutOptions options = null);

        Task<RiakResult> Delete(RiakObject riakObject, RiakDeleteOptions options = null);
        Task<RiakResult> Delete(string bucket, string key, RiakDeleteOptions options = null);
        Task<RiakResult> Delete(RiakObjectId objectId, RiakDeleteOptions options = null);
        Task<IEnumerable<RiakResult>> Delete(IEnumerable<RiakObjectId> objectIds, RiakDeleteOptions options = null);
        Task<IEnumerable<RiakResult>> DeleteBucket(string bucket, uint rwVal = RiakConstants.Defaults.RVal);

        Task<RiakResult<RiakSearchResult>> Search(RiakSearchRequest search);

        Task<RiakResult<RiakMapReduceResult>> MapReduce(RiakMapReduceQuery query);
        Task<RiakResult<RiakStreamedMapReduceResult>> StreamMapReduce(RiakMapReduceQuery query);

        Task<RiakResult<IEnumerable<string>>> ListBuckets();
        Task<RiakResult<IEnumerable<string>>> StreamListBuckets();
        Task<RiakResult<IEnumerable<string>>> ListKeys(string bucket);
        Task<RiakResult<IEnumerable<string>>> StreamListKeys(string bucket);

        Task<RiakResult<RiakBucketProperties>> GetBucketProperties(string bucket);
        Task<RiakResult> SetBucketProperties(string bucket, RiakBucketProperties properties, bool useHttp = false);
        Task<RiakResult> ResetBucketProperties(string bucket, bool useHttp = false);

        Task<RiakResult<IList<RiakObject>>> WalkLinks(RiakObject riakObject, IList<RiakLink> riakLinks);

        Task<RiakResult<RiakServerInfo>> GetServerInfo();

        Task<RiakResult<RiakIndexResult>> IndexGet(string bucket, string indexName, int value, RiakIndexGetOptions options = null);
        Task<RiakResult<RiakIndexResult>> IndexGet(string bucket, string indexName, string value, RiakIndexGetOptions options = null);
        Task<RiakResult<RiakIndexResult>> IndexGet(string bucket, string indexName, int minValue, int maxValue, RiakIndexGetOptions options = null);
        Task<RiakResult<RiakIndexResult>> IndexGet(string bucket, string indexName, string minValue, string maxValue, RiakIndexGetOptions options = null);

        Task<RiakResult<RiakStreamedIndexResult>> StreamIndexGet(string bucket, string indexName, BigInteger value, RiakIndexGetOptions options = null);
        Task<RiakResult<RiakStreamedIndexResult>> StreamIndexGet(string bucket, string indexName, string value, RiakIndexGetOptions options = null);
        Task<RiakResult<RiakStreamedIndexResult>> StreamIndexGet(string bucket, string indexName, BigInteger minValue, BigInteger maxValue, RiakIndexGetOptions options = null);
        Task<RiakResult<RiakStreamedIndexResult>> StreamIndexGet(string bucket, string indexName, string minValue, string maxValue, RiakIndexGetOptions options = null);

        Task<RiakResult<IList<string>>> ListKeysFromIndex(string bucket);

        Task Batch(Action<IRiakBatchClient> batchAction);

        [Obsolete("All async operations should use the functions that return Task<T>. This will be removed in CorrugatedIron 1.5.")]
        void Ping(Action<RiakResult> callback);

        [Obsolete("All async operations should use the functions that return Task<T>. This will be removed in CorrugatedIron 1.5.")]
        void Put(RiakObject value, Action<RiakResult<RiakObject>> callback, RiakPutOptions options = null);

        [Obsolete("All async operations should use the functions that return Task<T>. This will be removed in CorrugatedIron 1.5.")]
        void Put(IEnumerable<RiakObject> values, Action<IEnumerable<RiakResult<RiakObject>>> callback, RiakPutOptions options = null);

        [Obsolete("All async operations should use the functions that return Task<T>. This will be removed in CorrugatedIron 1.5.")]
        void Delete(string bucket, string key, Action<RiakResult> callback, RiakDeleteOptions options = null);

        [Obsolete("All async operations should use the functions that return Task<T>. This will be removed in CorrugatedIron 1.5.")]
        void Delete(RiakObjectId objectId, Action<RiakResult> callback, RiakDeleteOptions options = null);

        [Obsolete("All async operations should use the functions that return Task<T>. This will be removed in CorrugatedIron 1.5.")]
        void Delete(IEnumerable<RiakObjectId> objectIds, Action<IEnumerable<RiakResult>> callback, RiakDeleteOptions options = null);

        [Obsolete("All async operations should use the functions that return Task<T>. This will be removed in CorrugatedIron 1.5.")]
        void DeleteBucket(string bucket, Action<IEnumerable<RiakResult>> callback, uint rwVal = RiakConstants.Defaults.RVal);

        [Obsolete("All async operations should use the functions that return Task<T>. This will be removed in CorrugatedIron 1.5.")]
        void MapReduce(RiakMapReduceQuery query, Action<RiakResult<RiakMapReduceResult>> callback);

        [Obsolete("All async operations should use the functions that return Task<T>. This will be removed in CorrugatedIron 1.5.")]
        void StreamMapReduce(RiakMapReduceQuery query, Action<RiakResult<RiakStreamedMapReduceResult>> callback);

        [Obsolete("All async operations should use the functions that return Task<T>. This will be removed in CorrugatedIron 1.5.")]
        void ListBuckets(Action<RiakResult<IEnumerable<string>>> callback);

        [Obsolete("All async operations should use the functions that return Task<T>. This will be removed in CorrugatedIron 1.5.")]
        void ListKeys(string bucket, Action<RiakResult<IEnumerable<string>>> callback);

        [Obsolete("All async operations should use the functions that return Task<T>. This will be removed in CorrugatedIron 1.5.")]
        void StreamListKeys(string bucket, Action<RiakResult<IEnumerable<string>>> callback);

        [Obsolete("All async operations should use the functions that return Task<T>. This will be removed in CorrugatedIron 1.5.")]
        void GetBucketProperties(string bucket, Action<RiakResult<RiakBucketProperties>> callback);

        [Obsolete("All async operations should use the functions that return Task<T>. This will be removed in CorrugatedIron 1.5.")]
        void SetBucketProperties(string bucket, RiakBucketProperties properties, Action<RiakResult> callback);

        [Obsolete("All async operations should use the functions that return Task<T>. This will be removed in CorrugatedIron 1.5.")]
        void WalkLinks(RiakObject riakObject, IList<RiakLink> riakLinks, Action<RiakResult<IList<RiakObject>>> callback);

        [Obsolete("All async operations should use the functions that return Task<T>. This will be removed in CorrugatedIron 1.5.")]
        void GetServerInfo(Action<RiakResult<RiakServerInfo>> callback);
    }
}