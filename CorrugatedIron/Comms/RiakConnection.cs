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

using System.Reactive.Linq;
using System.Threading.Tasks;
using CorrugatedIron.Exceptions;
using CorrugatedIron.Extensions;
using CorrugatedIron.Messages;
using CorrugatedIron.Models.Rest;
using CorrugatedIron.Util;
using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace CorrugatedIron.Comms
{
    internal class RiakConnection : IRiakConnection
    {
        private readonly string _restRootUrl;
        private readonly RiakPbcSocket _socket;

        public bool IsIdle
        {
            get { return _socket.IsConnected; }
        }

        static RiakConnection()
        {
            ServicePointManager.ServerCertificateValidationCallback += ServerValidationCallback;
        }

        public RiakConnection(string restRootUrl, RiakPbcSocket riakPbcSocket)
        {
            _restRootUrl = restRootUrl;
            _socket = riakPbcSocket;
        }

        public async Task<RiakResult<TResult>> PbcRead<TResult>()
            where TResult : class, new()
        {
            try
            {
                var result = await _socket.Read<TResult>();
                return RiakResult<TResult>.Success(result);
            }
            catch (RiakException ex)
            {
                if (ex.NodeOffline)
                {
                    Disconnect();
                }
                return RiakResult<TResult>.Error(ResultCode.CommunicationError, ex.Message, ex.NodeOffline);
            }
            catch (Exception ex)
            {
                Disconnect();
                return RiakResult<TResult>.Error(ResultCode.CommunicationError, ex.Message, true);
            }
        }

        public async Task<RiakResult> PbcRead(MessageCode expectedMessageCode)
        {
            try
            {
                await _socket.Read(expectedMessageCode);
                return RiakResult.Success();
            }
            catch (RiakException ex)
            {
                if (ex.NodeOffline)
                {
                    Disconnect();
                }
                return RiakResult.Error(ResultCode.CommunicationError, ex.Message, ex.NodeOffline);
            }
            catch(Exception ex)
            {
                Disconnect();
                return RiakResult.Error(ResultCode.CommunicationError, ex.Message, true);
            }
        }

        public async Task<RiakResult<IObservable<RiakResult<TResult>>>> PbcRepeatRead<TResult>(Func<RiakResult<TResult>, bool> repeatRead)
            where TResult : class, new()
        {
            var pbcStreamReadIterator = PbcStreamReadIterator(repeatRead);
            return RiakResult<IObservable<RiakResult<TResult>>>.Success(pbcStreamReadIterator);
        }

        public async Task<RiakResult> PbcWrite<TRequest>(TRequest request)
            where TRequest : class
        {
            try
            {
                await _socket.Write(request);
                return RiakResult.Success();
            }
            catch (RiakException ex)
            {
                if (ex.NodeOffline)
                {
                    Disconnect();
                }
                return RiakResult.Error(ResultCode.CommunicationError, ex.Message, ex.NodeOffline);
            }
            catch(Exception ex)
            {
                Disconnect();
                return RiakResult.Error(ResultCode.CommunicationError, ex.Message, true);
            }
        }

        public async Task<RiakResult> PbcWrite(MessageCode messageCode)
        {
            try
            {
                await _socket.Write(messageCode);
                return RiakResult.Success();
            }
            catch (RiakException ex)
            {
                if (ex.NodeOffline)
                {
                    Disconnect();
                }
                return RiakResult.Error(ResultCode.CommunicationError, ex.Message, ex.NodeOffline);
            }
            catch(Exception ex)
            {
                Disconnect();
                return RiakResult.Error(ResultCode.CommunicationError, ex.Message, true);
            }
        }

        public async Task<RiakResult<TResult>> PbcWriteRead<TRequest, TResult>(TRequest request)
            where TRequest : class
            where TResult : class, new()
        {
            var writeResult = await PbcWrite(request);
            if(writeResult.IsSuccess)
            {
                return await PbcRead<TResult>();
            }
            return RiakResult<TResult>.Error(writeResult.ResultCode, writeResult.ErrorMessage, writeResult.NodeOffline);
        }

        public async Task<RiakResult> PbcWriteRead<TRequest>(TRequest request, MessageCode expectedMessageCode)
            where TRequest : class
        {
            var writeResult = await PbcWrite(request);
            if(writeResult.IsSuccess)
            {
                return await PbcRead(expectedMessageCode);
            }
            return RiakResult.Error(writeResult.ResultCode, writeResult.ErrorMessage, writeResult.NodeOffline);
        }

        public async Task<RiakResult<TResult>> PbcWriteRead<TResult>(MessageCode messageCode)
            where TResult : class, new()
        {
            var writeResult = await PbcWrite(messageCode);
            if(writeResult.IsSuccess)
            {
                return await PbcRead<TResult>();
            }
            return RiakResult<TResult>.Error(writeResult.ResultCode, writeResult.ErrorMessage, writeResult.NodeOffline);
        }

        public async Task<RiakResult> PbcWriteRead(MessageCode messageCode, MessageCode expectedMessageCode)
        {
            var writeResult = await PbcWrite(messageCode);
            if(writeResult.IsSuccess)
            {
                return await PbcRead(expectedMessageCode);
            }
            return RiakResult.Error(writeResult.ResultCode, writeResult.ErrorMessage, writeResult.NodeOffline);
        }

        public async Task<RiakResult<IObservable<RiakResult<TResult>>>> PbcWriteRead<TRequest, TResult>(TRequest request,
            Func<RiakResult<TResult>, bool> repeatRead)
            where TRequest : class
            where TResult : class, new()
        {
            var writeResult = await PbcWrite(request);
            if(writeResult.IsSuccess)
            {
                return await PbcRepeatRead(repeatRead);
            }

            return RiakResult<IObservable<RiakResult<TResult>>>.Error(writeResult.ResultCode, writeResult.ErrorMessage, writeResult.NodeOffline);
        }

        public async Task<RiakResult<IObservable<RiakResult<TResult>>>> PbcWriteRead<TResult>(MessageCode messageCode,
            Func<RiakResult<TResult>, bool> repeatRead)
            where TResult : class, new()
        {
            var writeResult = await PbcWrite(messageCode);
            if(writeResult.IsSuccess)
            {
                return await PbcRepeatRead(repeatRead);
            }

            return RiakResult<IObservable<RiakResult<TResult>>>.Error(writeResult.ResultCode, writeResult.ErrorMessage, writeResult.NodeOffline);
        }

        public async Task<RiakResult<IObservable<RiakResult<TResult>>>> PbcStreamRead<TResult>(Func<RiakResult<TResult>, bool> repeatRead)
            where TResult : class, new()
        {
            var streamer = PbcStreamReadIterator(repeatRead);
            return RiakResult<IObservable<RiakResult<TResult>>>.Success(streamer);
        }

        private IObservable<RiakResult<TResult>> PbcStreamReadIterator<TResult>(Func<RiakResult<TResult>, bool> repeatRead)
            where TResult : class, new()
        {
            var pbcStreamReadIterator = Observable.Create<RiakResult<TResult>>(async observer =>
            {
                RiakResult<TResult> result = null;
                do
                {
                    try
                    {
                        result = await PbcRead<TResult>();
                        if (!result.IsSuccess)
                        {
                            var error = RiakResult<TResult>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
                            observer.OnNext(error);
                            break;
                        }

                        observer.OnNext(result);
                    }
                    catch (Exception exception)
                    {
                        observer.OnError(exception);
                    }
  
                } while (repeatRead(result));

                observer.OnCompleted();
            });

            // then return the failure to the client to indicate failure
            return pbcStreamReadIterator;
        }

        public async Task<RiakResult<IObservable<RiakResult<TResult>>>> PbcWriteStreamRead<TRequest, TResult>(TRequest request,
            Func<RiakResult<TResult>, bool> repeatRead)
            where TRequest : class
            where TResult : class, new()
        {
            var streamer = PbcWriteStreamReadIterator(request, repeatRead);
            return RiakResult<IObservable<RiakResult<TResult>>>.Success(streamer);
        }

        public async Task<RiakResult<IObservable<RiakResult<TResult>>>> PbcWriteStreamRead<TResult>(MessageCode messageCode,
            Func<RiakResult<TResult>, bool> repeatRead)
            where TResult : class, new()
        {
            var streamer = PbcWriteStreamReadIterator(messageCode, repeatRead);
            return RiakResult<IObservable<RiakResult<TResult>>>.Success(streamer);
        }

        private IObservable<RiakResult<TResult>> PbcWriteStreamReadIterator<TRequest, TResult>(TRequest request,
            Func<RiakResult<TResult>, bool> repeatRead)
            where TRequest : class
            where TResult : class, new()
        {
            var writeResult = PbcWrite(request).Result;
            if(writeResult.IsSuccess)
            {
                return PbcStreamReadIterator(repeatRead);
            }
            return new[] { RiakResult<TResult>.Error(writeResult.ResultCode, writeResult.ErrorMessage, writeResult.NodeOffline) }.ToObservable();
        }

        private IObservable<RiakResult<TResult>> PbcWriteStreamReadIterator<TResult>(MessageCode messageCode,
            Func<RiakResult<TResult>, bool> repeatRead)
            where TResult : class, new()
        {
            var writeResult = PbcWrite(messageCode).Result;
            if(writeResult.IsSuccess)
            {
                return PbcStreamReadIterator(repeatRead);
            }
            return new[] { RiakResult<TResult>.Error(writeResult.ResultCode, writeResult.ErrorMessage, writeResult.NodeOffline) }.ToObservable();
        }

        public async Task<RiakResult<RiakRestResponse>> RestRequest(RiakRestRequest request)
        {
            var baseUri = new StringBuilder(_restRootUrl).Append(request.Uri);
            if(request.QueryParams.Count > 0)
            {
                baseUri.Append("?");
                var first = request.QueryParams.First();
                baseUri.Append(first.Key.UrlEncoded()).Append("=").Append(first.Value.UrlEncoded());
                request.QueryParams.Skip(1).ForEach(kv => baseUri.Append("&").Append(kv.Key.UrlEncoded()).Append("=").Append(kv.Value.UrlEncoded()));
            }
            var targetUri = new Uri(baseUri.ToString());

            var req = (HttpWebRequest)WebRequest.Create(targetUri);
            req.KeepAlive = true;
            req.Method = request.Method;
            req.Credentials = CredentialCache.DefaultCredentials;

            if(!string.IsNullOrWhiteSpace(request.ContentType))
            {
                req.ContentType = request.ContentType;
            }

            if(!request.Cache)
            {
                req.Headers.Set(RiakConstants.Rest.HttpHeaders.DisableCacheKey, RiakConstants.Rest.HttpHeaders.DisableCacheValue);
            }

            request.Headers.ForEach(h => req.Headers.Set(h.Key, h.Value));

            if(request.Body != null && request.Body.Length > 0)
            {
                req.ContentLength = request.Body.Length;

                var writer = await req.GetRequestStreamAsync();
                writer.Write(request.Body, 0, request.Body.Length);
            }
            else
            {
                req.ContentLength = 0;
            }

            try
            {
                var response = (HttpWebResponse) await req.GetResponseAsync();

                var result = new RiakRestResponse
                {
                    ContentLength = response.ContentLength,
                    ContentType = response.ContentType,
                    StatusCode = response.StatusCode,
                    Headers = response.Headers.AllKeys.ToDictionary(k => k, k => response.Headers[k]),
                    ContentEncoding = !string.IsNullOrWhiteSpace(response.ContentEncoding)
                        ? Encoding.GetEncoding(response.ContentEncoding)
                        : Encoding.Default
                };

                if (response.ContentLength > 0)
                {
                    using (var responseStream = response.GetResponseStream())
                    {
                        if (responseStream != null)
                        {
                            using (var reader = new StreamReader(responseStream, result.ContentEncoding))
                            {
                                result.Body = reader.ReadToEnd();
                            }
                        }
                    }
                }

                return RiakResult<RiakRestResponse>.Success(result);
            }
            catch (RiakException ex)
            {
                return RiakResult<RiakRestResponse>.Error(ResultCode.CommunicationError, ex.Message, ex.NodeOffline);
            }
            catch (WebException ex)
            {
                if (ex.Status == WebExceptionStatus.ProtocolError)
                {
                    return RiakResult<RiakRestResponse>.Error(ResultCode.HttpError, ex.Message, false);
                }

                return RiakResult<RiakRestResponse>.Error(ResultCode.HttpError, ex.Message, true);
            }
            catch (Exception ex)
            {
                return RiakResult<RiakRestResponse>.Error(ResultCode.CommunicationError, ex.Message, true);
            }
        }

        private static bool ServerValidationCallback(object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors)
        {
            return true;
        }

        public void Dispose()
        {
            _socket.Dispose();
            Disconnect();
        }

        public void Disconnect()
        {
            _socket.Disconnect();
        }
    }
}
