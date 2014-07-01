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

using System.Linq;
using System.Net;
using System.Numerics;
using System.Reactive.Linq;
using System.Web;
using CorrugatedIron.Comms;
using CorrugatedIron.Extensions;
using CorrugatedIron.Messages;
using CorrugatedIron.Models;
using CorrugatedIron.Models.Index;
using CorrugatedIron.Models.MapReduce;
using CorrugatedIron.Models.MapReduce.Inputs;
using CorrugatedIron.Models.Rest;
using CorrugatedIron.Models.Search;
using CorrugatedIron.Util;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CorrugatedIron
{
    internal class RiakAsyncClient : IRiakAsyncClient
    {
        private const string ListKeysWarning = "*** [CI] -> ListKeys is an expensive operation and should not be used in Production scenarios. ***";
        private const string InvalidBucketErrorMessage = "Bucket cannot be blank or contain forward-slashes";
        private const string InvalidKeyErrorMessage = "Key cannot be blank or contain forward-slashes";

        private readonly IRiakEndPoint _endPoint;
        private readonly IRiakConnection _batchConnection;

        internal RiakAsyncClient(IRiakEndPoint endPoint)
        {
            _endPoint = endPoint;
        }

        internal RiakAsyncClient(IRiakConnection batchConnection)
        {
            _batchConnection = batchConnection;
        }

        #region Helper functions
        private Task<RiakResult> UseConnection(Func<IRiakConnection, Task<RiakResult>> op)
        {
            return _batchConnection != null ? op(_batchConnection) : _endPoint.UseConnection(op, RetryCount);
        }

        private Task<RiakResult<TResult>> UseConnection<TResult>(Func<IRiakConnection, Task<RiakResult<TResult>>> op)
        {
            return _batchConnection != null ? op(_batchConnection) : _endPoint.UseConnection(op, RetryCount);
        }

        private Task<RiakResult<IObservable<RiakResult>>> UseConnection<TResult>(
            Func<IRiakConnection, Task<RiakResult<IObservable<RiakResult>>>> op)
        {
            return _batchConnection != null
                ? op(_batchConnection)
                : _endPoint.UseConnection(op, RetryCount);
        }

        private Task<RiakResult<IObservable<RiakResult<TResult>>>> UseConnection<TResult>(
            Func<IRiakConnection, Task<RiakResult<IObservable<RiakResult<TResult>>>>> op)
        {
            return _batchConnection != null
                ? op(_batchConnection)
                : _endPoint.UseConnection(op, RetryCount);
        }

        private static bool IsValidBucketOrKey(string value)
        {
            return !string.IsNullOrWhiteSpace(value) && !value.Contains("/");
        }

        private static RiakGetOptions DefaultGetOptions()
        {
            return (new RiakGetOptions()).SetR(RiakConstants.Defaults.RVal);
        }

        #endregion

        public Task<RiakResult> Ping()
        {
            return UseConnection(conn => conn.PbcWriteRead(MessageCode.PingReq, MessageCode.PingResp));
        }

        public async Task<RiakResult<RiakObject>> Get(string bucket, string key, RiakGetOptions options = null)
        {
            options = options ?? RiakClient.DefaultGetOptions();

            if (!IsValidBucketOrKey(bucket))
            {
                return RiakResult<RiakObject>.Error(ResultCode.InvalidRequest, InvalidBucketErrorMessage, false);
            }

            if (!IsValidBucketOrKey(key))
            {
                return RiakResult<RiakObject>.Error(ResultCode.InvalidRequest, InvalidKeyErrorMessage, false);
            }

            var request = new RpbGetReq { bucket = bucket.ToRiakString(), key = key.ToRiakString() };

            options = options ?? new RiakGetOptions();
            options.Populate(request);

            var result = await UseConnection(conn => conn.PbcWriteRead<RpbGetReq, RpbGetResp>(request));

            if (!result.IsSuccess)
            {
                return RiakResult<RiakObject>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
            }

            if (result.Value.vclock == null)
            {
                return RiakResult<RiakObject>.Error(ResultCode.NotFound, "Unable to find value in Riak", false);
            }

            var o = new RiakObject(bucket, key, result.Value.content, result.Value.vclock);

            return RiakResult<RiakObject>.Success(o);
        }

        public Task<RiakResult<RiakObject>> Get(RiakObjectId objectId, RiakGetOptions options = null)
        {
            options = options ?? DefaultGetOptions();
            return Get(objectId.Bucket, objectId.Key, options);
        }

        public async Task<IObservable<RiakResult<RiakObject>>> Get(IEnumerable<RiakObjectId> bucketKeyPairs, RiakGetOptions options = null)
        {
            bucketKeyPairs = bucketKeyPairs.ToList();

            options = options ?? new RiakGetOptions();

            var results = await UseConnection(conn =>
            {
                var observables = Observable.Create<RiakResult<RiakObject>>(async obs =>
                {
                    foreach (var bucketKeyPair in bucketKeyPairs)
                    {
                        try
                        {
                            // modified closure FTW
                            var bk = bucketKeyPair;
                            if (!IsValidBucketOrKey(bk.Bucket))
                            {
                                var error = RiakResult<RiakObject>.Error(ResultCode.InvalidRequest, InvalidBucketErrorMessage, false);
                                obs.OnNext(error);
                                continue;
                            }

                            if (!IsValidBucketOrKey(bk.Key))
                            {
                                var error = RiakResult<RiakObject>.Error(ResultCode.InvalidRequest, InvalidKeyErrorMessage, false);
                                obs.OnNext(error);
                                continue;
                            }

                            var req = new RpbGetReq
                            {
                                bucket = bk.Bucket.ToRiakString(), 
                                key = bk.Key.ToRiakString()
                            };
                            options.Populate(req);

                            var result = await conn.PbcWriteRead<RpbGetReq, RpbGetResp>(req);

                            if (result.Value.vclock == null)
                            {
                                var error = RiakResult<RiakObject>.Error(ResultCode.NotFound, "Unable to find value in Riak", false);
                                obs.OnNext(error);
                                continue;
                            }

                            var riakObject = new RiakObject(bk.Bucket, bk.Key, result.Value.content.First(), result.Value.vclock);

                            if (result.Value.content.Count > 1)
                            {
                                riakObject.Siblings = result.Value.content
                                    .Select(c =>new RiakObject(bk.Bucket, bk.Key, c, result.Value.vclock))
                                    .ToList();
                            }

                            obs.OnNext(RiakResult<RiakObject>.Success(riakObject));
                        }
                        catch (Exception exception)
                        {
                            obs.OnError(exception);
                        }
                    }

                    obs.OnCompleted();
                });

                return Task.FromResult(RiakResult<IObservable<RiakResult<RiakObject>>>.Success(observables));
            });

            return results.Value;
        }

        public async Task<RiakCounterResult> IncrementCounter(string bucket, string counter, long amount, RiakCounterUpdateOptions options = null)
        {
            if (!IsValidBucketOrKey(bucket))
            {
                return new RiakCounterResult(RiakResult<RiakObject>.Error(ResultCode.InvalidRequest, InvalidBucketErrorMessage, false), null);
            }

            if (!IsValidBucketOrKey(counter))
            {
                return new RiakCounterResult(RiakResult<RiakObject>.Error(ResultCode.InvalidRequest, InvalidKeyErrorMessage, false), null);
            }

            var request = new RpbCounterUpdateReq { bucket = bucket.ToRiakString(), key = counter.ToRiakString(), amount = amount };
            options = options ?? new RiakCounterUpdateOptions();
            options.Populate(request);

            var result = await UseConnection(conn => conn.PbcWriteRead<RpbCounterUpdateReq, RpbCounterUpdateResp>(request));

            if (!result.IsSuccess)
            {
                return new RiakCounterResult(RiakResult<RiakObject>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline), null);
            }

            var o = new RiakObject(bucket, counter, result.Value.returnvalue);
            var cVal = 0L;
            var parseResult = false;

            if (options.ReturnValue != null && options.ReturnValue.Value)
                parseResult = long.TryParse(o.Value.FromRiakString(), out cVal);

            return new RiakCounterResult(RiakResult<RiakObject>.Success(o), parseResult ? (long?)cVal : null);
        }

        public async Task<RiakCounterResult> GetCounter(string bucket, string counter, RiakCounterGetOptions options = null) 
        {
            if (!IsValidBucketOrKey(bucket))
            {
                return new RiakCounterResult(RiakResult<RiakObject>.Error(ResultCode.InvalidRequest, InvalidBucketErrorMessage, false), null);
            }

            if (!IsValidBucketOrKey(counter))
            {
                return new RiakCounterResult(RiakResult<RiakObject>.Error(ResultCode.InvalidRequest, InvalidKeyErrorMessage, false), null);
            }

            var request = new RpbCounterGetReq { bucket = bucket.ToRiakString(), key = counter.ToRiakString() };
            options = options ?? new RiakCounterGetOptions();
            options.Populate(request);

            var result = await UseConnection(conn => conn.PbcWriteRead<RpbCounterGetReq, RpbCounterGetResp>(request));

            if (!result.IsSuccess)
            {
                return new RiakCounterResult(RiakResult<RiakObject>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline), null);
            }

            var riakObject = new RiakObject(bucket, counter, result.Value.returnvalue);
            long cVal;
            var parseResult = long.TryParse(riakObject.Value.FromRiakString(), out cVal);

            return new RiakCounterResult(RiakResult<RiakObject>.Success(riakObject), parseResult ? (long?)cVal : null);
        }

        public async Task<IObservable<RiakResult<RiakObject>>> Put(IEnumerable<RiakObject> values, RiakPutOptions options = null)
        {
            options = options ?? new RiakPutOptions();

            var results = await UseConnection(conn =>
            {
                var observables = Observable.Create<RiakResult<RiakObject>>(async obs =>
                {
                    foreach (var v in values)
                    {
                        try
                        {
                            if (!IsValidBucketOrKey(v.Bucket))
                            {
                                var error = RiakResult<RiakObject>.Error(ResultCode.InvalidRequest, InvalidBucketErrorMessage, false);
                                obs.OnNext(error);
                                continue;
                            }

                            if (!IsValidBucketOrKey(v.Key))
                            {
                                var error = RiakResult<RiakObject>.Error(ResultCode.InvalidRequest, InvalidKeyErrorMessage, false);
                                obs.OnNext(error);
                                continue;
                            }

                            var msg = v.ToMessage();
                            options.Populate(msg);

                            var result = await conn.PbcWriteRead<RpbPutReq, RpbPutResp>(msg);

                            if (!result.IsSuccess)
                            {
                                var error = RiakResult<RiakObject>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
                                obs.OnNext(error);
                                continue;
                            }

                            var finalResult = options.ReturnBody
                                ? new RiakObject(v.Bucket, v.Key, result.Value.content.First(), result.Value.vclock)
                                : v;

                            if (options.ReturnBody && result.Value.content.Count > 1)
                            {
                                finalResult.Siblings = result.Value.content.Select(c => new RiakObject(v.Bucket, v.Key, c, result.Value.vclock)).ToList();
                            }

                            obs.OnNext(RiakResult<RiakObject>.Success(finalResult));
                        }
                        catch (Exception exception)
                        {
                            obs.OnError(exception);
                        }
                    }
                    obs.OnCompleted();
                });

                return Task.FromResult(RiakResult<IObservable<RiakResult<RiakObject>>>.Success(observables));
            });

            return results.Value;
        }

        public async Task<RiakResult<RiakObject>> Put(RiakObject value, RiakPutOptions options = null)
        {
            if (!IsValidBucketOrKey(value.Bucket))
            {
                return RiakResult<RiakObject>.Error(ResultCode.InvalidRequest, InvalidBucketErrorMessage, false);
            }

            if (!IsValidBucketOrKey(value.Key))
            {
                return RiakResult<RiakObject>.Error(ResultCode.InvalidRequest, InvalidKeyErrorMessage, false);
            }

            options = options ?? new RiakPutOptions();

            var request = value.ToMessage();
            options.Populate(request);

            var result = await UseConnection(conn => conn.PbcWriteRead<RpbPutReq, RpbPutResp>(request));

            if (!result.IsSuccess)
            {
                return RiakResult<RiakObject>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
            }

            var finalResult = options.ReturnBody
                ? new RiakObject(value.Bucket, value.Key, result.Value.content.First(), result.Value.vclock)
                : value;

            if (options.ReturnBody && result.Value.content.Count > 1)
            {
                finalResult.Siblings = result.Value.content.Select(c =>
                    new RiakObject(value.Bucket, value.Key, c, result.Value.vclock)).ToList();
            }

            return RiakResult<RiakObject>.Success(finalResult);
        }

        public Task<RiakResult> Delete(RiakObject riakObject, RiakDeleteOptions options = null)
        {
            return Delete(riakObject.Bucket, riakObject.Key, options);
        }

        public async Task<RiakResult> Delete(string bucket, string key, RiakDeleteOptions options = null)
        {
            if (!IsValidBucketOrKey(bucket))
            {
                return RiakResult<RiakObject>.Error(ResultCode.InvalidRequest, InvalidBucketErrorMessage, false);
            }

            if (!IsValidBucketOrKey(key))
            {
                return RiakResult<RiakObject>.Error(ResultCode.InvalidRequest, InvalidKeyErrorMessage, false);
            }

            options = options ?? new RiakDeleteOptions();

            var request = new RpbDelReq { bucket = bucket.ToRiakString(), key = key.ToRiakString() };
            options.Populate(request);

            var result = await UseConnection(conn => conn.PbcWriteRead(request, MessageCode.DelResp));

            return result;
        }

        public Task<RiakResult> Delete(RiakObjectId objectId, RiakDeleteOptions options = null)
        {
            return Delete(objectId.Bucket, objectId.Key, options);
        }

        public async Task<IObservable<RiakResult>> Delete(IEnumerable<RiakObjectId> objectIds, RiakDeleteOptions options = null)
        {
            var results = await UseConnection(conn => Delete(conn, objectIds, options));
            return results.Value;
        }

        private static Task<RiakResult<IObservable<RiakResult>>> Delete(IRiakConnection conn, IEnumerable<RiakObjectId> objectIds, RiakDeleteOptions options = null)
        {
            options = options ?? new RiakDeleteOptions();

            var observables = Observable.Create<RiakResult>(async obs =>
            {
                foreach (var id in objectIds)
                {
                    try
                    {
                        if (!IsValidBucketOrKey(id.Bucket))
                        {
                            var error = RiakResult.Error(ResultCode.InvalidRequest, InvalidBucketErrorMessage, false);
                            obs.OnNext(error);
                            continue;
                        }

                        if (!IsValidBucketOrKey(id.Key))
                        {
                            var error = RiakResult.Error(ResultCode.InvalidRequest, InvalidKeyErrorMessage, false);
                            obs.OnNext(error);
                            continue;
                        }

                        var req = new RpbDelReq { bucket = id.Bucket.ToRiakString(), key = id.Key.ToRiakString() };
                        options.Populate(req);
                        var result = await conn.PbcWriteRead(req, MessageCode.DelResp);

                        obs.OnNext(result);
                    }
                    catch (Exception exception)
                    {
                        obs.OnError(exception);
                    }
                }
                obs.OnCompleted();
            });

            return Task.FromResult(RiakResult<IObservable<RiakResult>>.Success(observables));
        }

        public async Task<IObservable<RiakResult>> DeleteBucket(string bucket, RiakDeleteOptions deleteOptions = null)
        {
            var result = await UseConnection(async conn =>
            {
                var keyResults = await ListKeys(conn, bucket);
                if (!keyResults.IsSuccess)
                {
                    return RiakResult<IObservable<RiakResult>>
                        .Error(keyResults.ResultCode, keyResults.ErrorMessage, keyResults.NodeOffline);
                }

                var objectIds = keyResults.Value
                    .Select(key => new RiakObjectId(bucket, key))
                    .ToEnumerable()
                    .ToList();

                var deleteResult = await Delete(conn, objectIds, deleteOptions);

                return deleteResult;
            });

            return result.Value;
        }

        public async Task<RiakResult<RiakSearchResult>> Search(RiakSearchRequest search)
        {
            var request = search.ToMessage();
            var response = await UseConnection(conn => conn.PbcWriteRead<RpbSearchQueryReq, RpbSearchQueryResp>(request));

            if (response.IsSuccess)
            {
                return RiakResult<RiakSearchResult>.Success(new RiakSearchResult(response.Value));
            }

            return RiakResult<RiakSearchResult>.Error(response.ResultCode, response.ErrorMessage, response.NodeOffline);
        }

        public async Task<RiakResult<RiakMapReduceResult>> MapReduce(RiakMapReduceQuery query)
        {
            var request = query.ToMessage();
            var response = await UseConnection((conn) => conn.PbcWriteRead<RpbMapRedReq, RpbMapRedResp>(request, r => r.IsSuccess && !r.Value.done));

            if (response.IsSuccess)
            {
                return RiakResult<RiakMapReduceResult>.Success(new RiakMapReduceResult(response.Value));
            }

            return RiakResult<RiakMapReduceResult>.Error(response.ResultCode, response.ErrorMessage, response.NodeOffline);
        }

        public async Task<RiakResult<RiakStreamedMapReduceResult>> StreamMapReduce(RiakMapReduceQuery query)
        {
            var request = query.ToMessage();
            var response = await UseConnection((conn) => conn.PbcWriteStreamRead<RpbMapRedReq, RpbMapRedResp>(request, r => r.IsSuccess && !r.Value.done));

            if (response.IsSuccess)
            {
                return RiakResult<RiakStreamedMapReduceResult>.Success(new RiakStreamedMapReduceResult(response.Value));
            }
            return RiakResult<RiakStreamedMapReduceResult>.Error(response.ResultCode, response.ErrorMessage, response.NodeOffline);
        }

        public async Task<RiakResult<IObservable<string>>> StreamListBuckets()
        {
            var lbReq = new RpbListBucketsReq { stream = true };
            var result = await UseConnection((conn) => conn.PbcWriteStreamRead<RpbListBucketsReq, RpbListBucketsResp>(lbReq, lbr => lbr.IsSuccess && !lbr.Value.done));

            if (result.IsSuccess)
            {
                var buckets = result.Value
                    .Where(r => r.IsSuccess)
                    .SelectMany(r => r.Value.buckets)
                    .Select(k => k.FromRiakString());
                return RiakResult<IObservable<string>>.Success(buckets);
            }

            return RiakResult<IObservable<string>>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
        }

        public async Task<RiakResult<IObservable<string>>> ListBuckets()
        {
            var result = await UseConnection(conn => conn.PbcWriteRead<RpbListBucketsResp>(MessageCode.ListBucketsReq));

            if (result.IsSuccess)
            {
                var buckets = result.Value.buckets.Select(b => b.FromRiakString());
                return RiakResult<IObservable<string>>.Success(buckets.ToObservable());
            }
            return RiakResult<IObservable<string>>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
        }

        public Task<RiakResult<IObservable<string>>> ListKeys(string bucket)
        {
            return  UseConnection(conn => ListKeys(conn, bucket));
        }

        private async static Task<RiakResult<IObservable<string>>> ListKeys(IRiakConnection conn, string bucket)
        {
            System.Diagnostics.Debug.Write(ListKeysWarning);
            System.Diagnostics.Trace.TraceWarning(ListKeysWarning);
            Console.WriteLine(ListKeysWarning);

            var lkReq = new RpbListKeysReq { bucket = bucket.ToRiakString() };
            var result = await conn.PbcWriteRead<RpbListKeysReq, RpbListKeysResp>(lkReq, lkr => lkr.IsSuccess && !lkr.Value.done);
            if (result.IsSuccess)
            {
                var keys = result.Value
                    .Where(r => r.IsSuccess)
                    .SelectMany(r => r.Value.keys)
                    .Select(k => k.FromRiakString())
                    .Distinct()
                    .ToEnumerable()
                    .ToObservable();

                return RiakResult<IObservable<string>>.Success(keys);
            }
            return RiakResult<IObservable<string>>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
        }

        public async Task<RiakResult<IObservable<string>>> StreamListKeys(string bucket)
        {
            System.Diagnostics.Debug.Write(ListKeysWarning);
            System.Diagnostics.Trace.TraceWarning(ListKeysWarning);
            Console.WriteLine(ListKeysWarning);

            var lkReq = new RpbListKeysReq { bucket = bucket.ToRiakString() };
            var result = await UseConnection((conn) => conn.PbcWriteStreamRead<RpbListKeysReq, RpbListKeysResp>(lkReq, lkr => lkr.IsSuccess && !lkr.Value.done));

            if (result.IsSuccess)
            {
                var keys = result.Value.Where(r => r.IsSuccess).SelectMany(r => r.Value.keys).Select(k => k.FromRiakString());
                return RiakResult<IObservable<string>>.Success(keys);
            }
            return RiakResult<IObservable<string>>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
        }

        public async Task<RiakResult<RiakBucketProperties>> GetBucketProperties(string bucket)
        {
            // bucket names cannot have slashes in the names, the REST interface doesn't like it at all
            if (!IsValidBucketOrKey(bucket))
            {
                return RiakResult<RiakBucketProperties>.Error(ResultCode.InvalidRequest, InvalidBucketErrorMessage, false);
            }

            var bpReq = new RpbGetBucketReq { bucket = bucket.ToRiakString() };
            var result = await UseConnection(conn => conn.PbcWriteRead<RpbGetBucketReq, RpbGetBucketResp>(bpReq));

            if (result.IsSuccess)
            {
                var props = new RiakBucketProperties(result.Value.props);
                return RiakResult<RiakBucketProperties>.Success(props);
            }
            return RiakResult<RiakBucketProperties>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
        }

        public Task<RiakResult> SetBucketProperties(string bucket, RiakBucketProperties properties, bool useHttp = false)
        {
            return useHttp ? SetHttpBucketProperties(bucket, properties) : SetPbcBucketProperties(bucket, properties);
        }

        private static string ToBucketUri(string bucket)
        {
            return "{0}/{1}".Fmt(RiakConstants.Rest.Uri.RiakRoot, HttpUtility.UrlEncode(bucket));
        }

        private async Task<RiakResult> SetHttpBucketProperties(string bucket, RiakBucketProperties properties)
        {
            var request = new RiakRestRequest(ToBucketUri(bucket), RiakConstants.Rest.HttpMethod.Put)
            {
                Body = properties.ToJsonString().ToRiakString(),
                ContentType = RiakConstants.ContentTypes.ApplicationJson
            };

            var result = await UseConnection(conn => conn.RestRequest(request));
            if (result.IsSuccess && result.Value.StatusCode != HttpStatusCode.NoContent)
            {
                return RiakResult.Error(ResultCode.InvalidResponse, "Unexpected Status Code: {0} ({1})".Fmt(result.Value.StatusCode, (int)result.Value.StatusCode), result.NodeOffline);
            }

            return result;
        }

        private async Task<RiakResult> SetPbcBucketProperties(string bucket, RiakBucketProperties properties)
        {
            if (!IsValidBucketOrKey(bucket))
            {
                return RiakResult<RiakBucketProperties>.Error(ResultCode.InvalidRequest, InvalidBucketErrorMessage, false);
            }

            var request = new RpbSetBucketReq { bucket = bucket.ToRiakString(), props = properties.ToMessage() };
            var result = await UseConnection(conn => conn.PbcWriteRead(request, MessageCode.SetBucketResp));

            return result;
        }

        public Task<RiakResult> ResetBucketProperties(string bucket, bool useHttp = false)
        {
            if (!IsValidBucketOrKey(bucket))
            {
                return Task.FromResult(RiakResult.Error(ResultCode.InvalidRequest, InvalidBucketErrorMessage, false));
            }

            return useHttp ? ResetHttpBucketProperties(bucket) : ResetPbcBucketProperties(bucket);
        }

        private static string ToBucketPropsUri(string bucket)
        {
            return RiakConstants.Rest.Uri.BucketPropsFmt.Fmt(HttpUtility.UrlEncode(bucket));
        }

        private Task<RiakResult> ResetPbcBucketProperties(string bucket)
        {
            var request = new RpbResetBucketReq { bucket = bucket.ToRiakString() };
            var result = UseConnection(conn => conn.PbcWriteRead(request, MessageCode.ResetBucketResp));
            return result;
        }

        private async Task<RiakResult> ResetHttpBucketProperties(string bucket)
        {
            var request = new RiakRestRequest(ToBucketPropsUri(bucket), RiakConstants.Rest.HttpMethod.Delete);

            var result = await UseConnection(conn => conn.RestRequest(request));
            if (result.IsSuccess)
            {
                switch (result.Value.StatusCode)
                {
                    case HttpStatusCode.NoContent:
                        return result;
                    case HttpStatusCode.NotFound:
                        return RiakResult.Error(ResultCode.NotFound, "Bucket {0} not found.".Fmt(bucket), false);
                    default:
                        return RiakResult.Error(ResultCode.InvalidResponse, "Unexpected Status Code: {0} ({1})".Fmt(result.Value.StatusCode, (int)result.Value.StatusCode), result.NodeOffline);
                }
            }
            return result;
        }

        public Task<RiakResult<RiakIndexResult>> IndexGet(string bucket, string indexName, BigInteger value, RiakIndexGetOptions options = null)
        {
            return IndexGetEquals(bucket, indexName.ToIntegerKey(), value.ToString(), options);
        }

        public Task<RiakResult<RiakIndexResult>> IndexGet(string bucket, string indexName, string value, RiakIndexGetOptions options = null)
        {
            return IndexGetEquals(bucket, indexName.ToBinaryKey(), value, options);
        }

        public Task<RiakResult<RiakIndexResult>> IndexGet(string bucket, string indexName, BigInteger minValue, BigInteger maxValue, RiakIndexGetOptions options = null)
        {
            return IndexGetRange(bucket, indexName.ToIntegerKey(), minValue.ToString(), maxValue.ToString(), options);
        }

        public Task<RiakResult<RiakIndexResult>> IndexGet(string bucket, string indexName, string minValue, string maxValue, RiakIndexGetOptions options = null)
        {
            return IndexGetRange(bucket, indexName.ToBinaryKey(), minValue, maxValue, options);
        }

        private static bool ReturnTerms(RiakIndexGetOptions options)
        {
            return options.ReturnTerms != null && options.ReturnTerms.Value;
        }

        private async Task<RiakResult<RiakIndexResult>> IndexGetRange(string bucket, string indexName, string minValue, string maxValue, RiakIndexGetOptions options = null)
        {
            var message = new RpbIndexReq
            {
                bucket = bucket.ToRiakString(),
                index = indexName.ToRiakString(),
                qtype = RpbIndexReq.IndexQueryType.range,
                range_min = minValue.ToRiakString(),
                range_max = maxValue.ToRiakString()
            };

            options = options ?? new RiakIndexGetOptions();
            options.Populate(message);

            var result = await UseConnection(conn => conn.PbcWriteRead<RpbIndexReq, RpbIndexResp>(message));

            if (result.IsSuccess)
            {
                var r = RiakResult<RiakIndexResult>.Success(new RiakIndexResult(ReturnTerms(options), result));

                if (result.Done.HasValue)
                    r.SetDone(result.Done.Value);

                if (result.Value.continuation != null)
                {
                    var continuation = result.Value.continuation.FromRiakString();

                    if (!string.IsNullOrEmpty(continuation))
                        r.SetContinuation(continuation);
                }

                return r;
            }

            return RiakResult<RiakIndexResult>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
        }

        private async Task<RiakResult<RiakIndexResult>> IndexGetEquals(string bucket, string indexName, string value, RiakIndexGetOptions options = null)
        {
            var message = new RpbIndexReq
            {
                bucket = bucket.ToRiakString(),
                index = indexName.ToRiakString(),
                key = value.ToRiakString(),
                qtype = RpbIndexReq.IndexQueryType.eq
            };

            options = options ?? new RiakIndexGetOptions();
            options.Populate(message);

            var result = await  UseConnection(conn => conn.PbcWriteRead<RpbIndexReq, RpbIndexResp>(message));

            if (result.IsSuccess)
            {
                return RiakResult<RiakIndexResult>.Success(new RiakIndexResult(ReturnTerms(options), result));
            }

            return RiakResult<RiakIndexResult>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
        }

        public Task<RiakResult<RiakStreamedIndexResult>> StreamIndexGet(string bucket, string indexName, BigInteger value, RiakIndexGetOptions options = null)
        {
            return StreamIndexGetEquals(bucket, indexName.ToIntegerKey(), value.ToString(), options);
        }

        public Task<RiakResult<RiakStreamedIndexResult>> StreamIndexGet(string bucket, string indexName, string value, RiakIndexGetOptions options = null)
        {
            return StreamIndexGetEquals(bucket, indexName.ToBinaryKey(), value, options);
        }

        public Task<RiakResult<RiakStreamedIndexResult>> StreamIndexGet(string bucket, string indexName, BigInteger minValue, BigInteger maxValue, RiakIndexGetOptions options = null)
        {
            return StreamIndexGetRange(bucket, indexName.ToIntegerKey(), minValue.ToString(), maxValue.ToString(), options);
        }

        public Task<RiakResult<RiakStreamedIndexResult>> StreamIndexGet(string bucket, string indexName, string minValue, string maxValue, RiakIndexGetOptions options = null)
        {
            return StreamIndexGetRange(bucket, indexName.ToBinaryKey(), minValue, maxValue, options);
        }

        private async Task<RiakResult<RiakStreamedIndexResult>> StreamIndexGetEquals(string bucket, string indexName, string value,
                                                                        RiakIndexGetOptions options = null)
        {
            var message = new RpbIndexReq
            {
                bucket = bucket.ToRiakString(),
                index = indexName.ToRiakString(),
                key = value.ToRiakString(),
                qtype = RpbIndexReq.IndexQueryType.eq,
                stream = true
            };

            options = options ?? new RiakIndexGetOptions();
            options.Populate(message);

            var result = await UseConnection((conn) => conn.PbcWriteStreamRead<RpbIndexReq, RpbIndexResp>(message, lbr => lbr.IsSuccess && !lbr.Value.done));

            if (result.IsSuccess)
            {
                return
                    RiakResult<RiakStreamedIndexResult>.Success(new RiakStreamedIndexResult(ReturnTerms(options), result.Value));
            }

            return RiakResult<RiakStreamedIndexResult>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
        }

        private async Task<RiakResult<RiakStreamedIndexResult>> StreamIndexGetRange(string bucket, string indexName, string minValue, string maxValue,
                                         RiakIndexGetOptions options = null)
        {
            var message = new RpbIndexReq
            {
                bucket = bucket.ToRiakString(),
                index = indexName.ToRiakString(),
                qtype = RpbIndexReq.IndexQueryType.range,
                range_min = minValue.ToRiakString(),
                range_max = maxValue.ToRiakString(),
                stream = true
            };

            options = options ?? new RiakIndexGetOptions();
            options.Populate(message);

            var result = await UseConnection((conn) => conn.PbcWriteStreamRead<RpbIndexReq, RpbIndexResp>(message, lbr => lbr.IsSuccess && !lbr.Value.done));

            if (result.IsSuccess)
            {
                return
                    RiakResult<RiakStreamedIndexResult>.Success(new RiakStreamedIndexResult(ReturnTerms(options),
                                                                                            result.Value));
            }

            return RiakResult<RiakStreamedIndexResult>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
        }

        public async Task<RiakResult<IList<string>>> ListKeysFromIndex(string bucket)
        {
            var result = await IndexGet(bucket, RiakConstants.SystemIndexKeys.RiakBucketIndex, bucket);
            return RiakResult<IList<string>>.Success(result.Value.IndexKeyTerms.Select(ikt => ikt.Key).ToList());
        }

        public async Task<RiakResult<IList<RiakObject>>> WalkLinks(RiakObject riakObject, IList<RiakLink> riakLinks)
        {
            System.Diagnostics.Debug.Assert(riakLinks.Count > 0, "Link walking requires at least one link");

            var input = new RiakBucketKeyInput()
                .Add(riakObject.Bucket, riakObject.Key);

            var query = new RiakMapReduceQuery()
                .Inputs(input);

            var lastLink = riakLinks.Last();

            foreach (var riakLink in riakLinks)
            {
                var link = riakLink;
                var keep = ReferenceEquals(link, lastLink);

                query.Link(l => l.FromRiakLink(link).Keep(keep));
            }

            var result = await MapReduce(query);

            if (!result.IsSuccess)
            {
                return RiakResult<IList<RiakObject>>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
            }

            var linkResults = result.Value.PhaseResults
                .GroupBy(r => r.Phase)
                .Where(g => g.Key == riakLinks.Count - 1);
            var linkResultStrings = linkResults
                .SelectMany(lr => lr.ToList(), (lr, r) => new { lr, r })
                .SelectMany(@t => @t.r.Values, (@t, s) => s.FromRiakString());

            //var linkResultStrings = linkResults.SelectMany(g => g.Select(r => r.Values.Value.FromRiakString()));
            var rawLinks = linkResultStrings
                .SelectMany(RiakLink.ParseArrayFromJsonString)
                .Distinct();

            var oids = rawLinks
                .Select(l => new RiakObjectId(l.Bucket, l.Key))
                .ToList();

            var objects = await Get(oids, new RiakGetOptions());

            //TODO FIXME - This really should be a multi-phase map/reduce
            // we could be discarding results here. Not good?
            var successfulObjects = objects
                .Where(r => r.IsSuccess)
                .Select(r => r.Value)
                .ToEnumerable()
                .ToList();

            return RiakResult<IList<RiakObject>>.Success(successfulObjects);
        }

        public async Task<RiakResult<RiakServerInfo>> GetServerInfo()
        {
            var result = await UseConnection(conn => conn.PbcWriteRead<RpbGetServerInfoResp>(MessageCode.GetServerInfoReq));

            if (result.IsSuccess)
            {
                return RiakResult<RiakServerInfo>.Success(new RiakServerInfo(result.Value));
            }
            return RiakResult<RiakServerInfo>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
        }

        public int RetryCount { get; set; }

        public Task Batch(Action<IRiakAsyncClient> batchAction)
        {
            return Batch<object>(c => { batchAction(c); return null; });
        }

        private async Task<T> Batch<T>(Func<IRiakAsyncClient, T> batchFun)
        {
            var funResult = default(T);

            Func<IRiakConnection, Task<RiakResult<IObservable<RiakResult<object>>>>> helperBatchFun = (conn) =>
            {
                try
                {
                    funResult = batchFun(new RiakAsyncClient(conn));
                    return Task.FromResult(RiakResult<IObservable<RiakResult<object>>>.Success(null));
                }
                catch (Exception ex)
                {
                    return Task.FromResult(RiakResult<IObservable<RiakResult<object>>>.Error(ResultCode.BatchException, "{0}\n{1}".Fmt(ex.Message, ex.StackTrace), true));
                }
            };

            var result = await _endPoint.UseConnection(helperBatchFun, RetryCount);

            if (!result.IsSuccess && result.ResultCode == ResultCode.BatchException)
            {
                throw new Exception(result.ErrorMessage);
            }

            return funResult;
        }
    }
}