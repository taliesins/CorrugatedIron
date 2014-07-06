using System;
using System.Collections.Generic;
using System.Numerics;
using System.Threading.Tasks;
using CorrugatedIron.Models;
using CorrugatedIron.Models.Index;
using CorrugatedIron.Models.MapReduce;
using CorrugatedIron.Models.Search;

namespace CorrugatedIron
{
    public interface IRiakAsyncBatchClient
    {
        Task<Pong> Ping();

        Task<RiakObject> Get(string bucket, string key, RiakGetOptions options = null);
        Task<RiakObject> Get(RiakObjectId objectId, RiakGetOptions options = null);

        IObservable<RiakObject> Get(IEnumerable<RiakObjectId> bucketKeyPairs, RiakGetOptions options = null);

        Task<RiakCounterResult> IncrementCounter(string bucket, string counter, long amount, RiakCounterUpdateOptions options = null);
        Task<RiakCounterResult> GetCounter(string bucket, string counter, RiakCounterGetOptions options = null);

        Task<RiakObject> Put(RiakObject value, RiakPutOptions options = null);
        IObservable<RiakObject> Put(IEnumerable<RiakObject> values, RiakPutOptions options = null);

        Task<RiakObjectId> Delete(RiakObject riakObject, RiakDeleteOptions options = null);
        Task<RiakObjectId> Delete(string bucket, string key, RiakDeleteOptions options = null);
        Task<RiakObjectId> Delete(RiakObjectId objectId, RiakDeleteOptions options = null);
        IObservable<RiakObjectId> Delete(IEnumerable<RiakObjectId> objectIds, RiakDeleteOptions options = null);
        IObservable<RiakObjectId> DeleteBucket(string bucket, RiakDeleteOptions deleteOptions = null);

        Task<RiakSearchResult> Search(RiakSearchRequest search);

        Task<RiakMapReduceResult> MapReduce(RiakMapReduceQuery query);
        Task<RiakStreamedMapReduceResult> StreamMapReduce(RiakMapReduceQuery query);

        IObservable<string> ListBuckets();
        IObservable<string> StreamListBuckets();
        IObservable<string> ListKeys(string bucket);
        IObservable<string> StreamListKeys(string bucket);

        Task<RiakBucketProperties> GetBucketProperties(string bucket);
        Task<bool> SetBucketProperties(string bucket, RiakBucketProperties properties, bool useHttp = false);
        Task<bool> ResetBucketProperties(string bucket, bool useHttp = false);

        IObservable<RiakObject> WalkLinks(RiakObject riakObject, IList<RiakLink> riakLinks);

        Task<RiakServerInfo> GetServerInfo();

        Task<RiakIndexResult> IndexGet(string bucket, string indexName, BigInteger value, RiakIndexGetOptions options = null);
        Task<RiakIndexResult> IndexGet(string bucket, string indexName, string value, RiakIndexGetOptions options = null);
        Task<RiakIndexResult> IndexGet(string bucket, string indexName, BigInteger minValue, BigInteger maxValue, RiakIndexGetOptions options = null);
        Task<RiakIndexResult> IndexGet(string bucket, string indexName, string minValue, string maxValue, RiakIndexGetOptions options = null);

        Task<RiakStreamedIndexResult> StreamIndexGet(string bucket, string indexName, BigInteger value, RiakIndexGetOptions options = null);
        Task<RiakStreamedIndexResult> StreamIndexGet(string bucket, string indexName, string value, RiakIndexGetOptions options = null);
        Task<RiakStreamedIndexResult> StreamIndexGet(string bucket, string indexName, BigInteger minValue, BigInteger maxValue, RiakIndexGetOptions options = null);
        Task<RiakStreamedIndexResult> StreamIndexGet(string bucket, string indexName, string minValue, string maxValue, RiakIndexGetOptions options = null);

        IObservable<string> ListKeysFromIndex(string bucket);
    }
}