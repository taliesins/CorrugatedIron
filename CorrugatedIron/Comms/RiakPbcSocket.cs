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

using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using CorrugatedIron.Comms.Sockets;
using CorrugatedIron.Exceptions;
using CorrugatedIron.Extensions;
using CorrugatedIron.Messages;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;

namespace CorrugatedIron.Comms
{
    internal class RiakPbcSocket : IDisposable
    {
        private const int SocketConnectAttempts = 3;
        private readonly DnsEndPoint _endPoint;
        private readonly int _receiveTimeout;
        private readonly int _sendTimeout;
        private readonly SocketAwaitablePool _socketAwaitablePool;
        private readonly BlockingBufferManager _blockingBufferManager;

        private static readonly Dictionary<MessageCode, Type> MessageCodeToTypeMap;
        private static readonly Dictionary<Type, MessageCode> TypeToMessageCodeMap;

        private Socket _socket;

        public RiakPbcSocket(string server, int port, int receiveTimeout, int sendTimeout, SocketAwaitablePool socketAwaitablePool, BlockingBufferManager blockingBufferManager)
        {
            _endPoint = new DnsEndPoint(server, port);
            _receiveTimeout = receiveTimeout;
            _sendTimeout = sendTimeout;
            _socketAwaitablePool = socketAwaitablePool;
            _blockingBufferManager = blockingBufferManager;
        }

        private async Task ConnectAsync(Socket socket, EndPoint endPoint)
        {
            var awaitable = _socketAwaitablePool.Take();
            try
            {
                awaitable.RemoteEndPoint = endPoint;

                var result = SocketError.Fault;
                for (var i = 0; i < SocketConnectAttempts; i++)
                {
                    result = await socket.ConnectAsync(awaitable);
                    if (result == SocketError.Success)
                    {
                        break;
                    }
                }

                if (result != SocketError.Success)
                {
                    throw new RiakException("Unable to connect to remote server: {0}:{1} error code {2}".Fmt(_endPoint.Host, _endPoint.Port, result));
                }
            }
            finally
            {
                awaitable.Clear();
                _socketAwaitablePool.Add(awaitable);
            }
        }

        private async Task ReceiveAsync(Socket socket, ArraySegment<byte> buffer)
        {
            var awaitable = _socketAwaitablePool.Take();
            try
            {
                awaitable.Buffer = buffer;

                var result = await socket.ReceiveAsync(awaitable);

                if (result != SocketError.Success)
                {
                    throw new RiakException("Unable to read data from the source stream: {0}:{1} error code {2}"
                        .Fmt(_endPoint.Host, _endPoint.Port, result));
                }

                if (awaitable.Arguments.BytesTransferred == 0)
                {
                    throw new RiakException("Unable to read data from the source stream: {0}:{1} remote server closed connection"
                        .Fmt(_endPoint.Host, _endPoint.Port));
                }

                if (awaitable.Buffer.Count != awaitable.Arguments.BytesTransferred)
                {
                    throw new RiakException("Unable to read data from the source stream: {0}:{1} error code expecting {2} bytes but only received {3}"
                        .Fmt(_endPoint.Host, _endPoint.Port, result, awaitable.Buffer.Count));
                }
            }
            finally
            {
                awaitable.Clear();
                _socketAwaitablePool.Add(awaitable);
            }
        }

        private async Task SendAsync(Socket socket, ArraySegment<byte> buffer)
        {
            var awaitable = _socketAwaitablePool.Take();
            try
            {
                awaitable.Buffer = buffer;

                while (true)
                {
                    var result = await socket.SendAsync(awaitable);

                    if (result != SocketError.Success)
                    {
                        throw new RiakException("Failed to send data to server - Timed Out: {0}:{1} error code {2}".Fmt(_endPoint.Host, _endPoint.Port, result));
                    }

                    if (awaitable.Buffer.Count == awaitable.Transferred.Count) 
                    {
                        return; // Break if all the data is sent.
                    } 

                    // Set the buffer to send the remaining data.
                    awaitable.Buffer = new ArraySegment<byte>(
                        awaitable.Buffer.Array,
                        awaitable.Buffer.Offset + awaitable.Transferred.Count,
                        awaitable.Buffer.Count - awaitable.Transferred.Count);
                }
            }
            finally
            {
                awaitable.Clear();
                _socketAwaitablePool.Add(awaitable);
            }
        }


        public bool IsConnected
        {
            get
            {
                return _socket != null && _socket.Connected;
            }
        }

        static RiakPbcSocket()
        {
            MessageCodeToTypeMap = new Dictionary<MessageCode, Type>
            {
                { MessageCode.ErrorResp, typeof(RpbErrorResp) },
                { MessageCode.GetClientIdResp, typeof(RpbGetClientIdResp) },
                { MessageCode.SetClientIdReq, typeof(RpbSetClientIdReq) },
                { MessageCode.GetServerInfoResp, typeof(RpbGetServerInfoResp) },
                { MessageCode.GetReq, typeof(RpbGetReq) },
                { MessageCode.GetResp, typeof(RpbGetResp) },
                { MessageCode.PutReq, typeof(RpbPutReq) },
                { MessageCode.PutResp, typeof(RpbPutResp) },
                { MessageCode.DelReq, typeof(RpbDelReq) },
                { MessageCode.ListBucketsReq, typeof(RpbListBucketsReq) },
                { MessageCode.ListBucketsResp, typeof(RpbListBucketsResp) },
                { MessageCode.ListKeysReq, typeof(RpbListKeysReq) },
                { MessageCode.ListKeysResp, typeof(RpbListKeysResp) },
                { MessageCode.GetBucketReq, typeof(RpbGetBucketReq) },
                { MessageCode.GetBucketResp, typeof(RpbGetBucketResp) },
                { MessageCode.SetBucketReq, typeof(RpbSetBucketReq) },
                { MessageCode.MapRedReq, typeof(RpbMapRedReq) },
                { MessageCode.MapRedResp, typeof(RpbMapRedResp) },
                { MessageCode.IndexReq, typeof(RpbIndexReq) },
                { MessageCode.IndexResp, typeof(RpbIndexResp) },
                { MessageCode.SearchQueryReq, typeof(RpbSearchQueryReq) },
                { MessageCode.SearchQueryResp, typeof(RpbSearchQueryResp) },
                { MessageCode.ResetBucketReq, typeof(RpbResetBucketReq) },
                { MessageCode.CsBucketReq, typeof(RpbCSBucketReq) },
                { MessageCode.CsBucketResp, typeof(RpbCSBucketResp) },
                { MessageCode.CounterUpdateReq, typeof(RpbCounterUpdateReq) },
                { MessageCode.CounterUpdateResp, typeof(RpbCounterUpdateResp) },
                { MessageCode.CounterGetReq, typeof(RpbCounterGetReq) },
                { MessageCode.CounterGetResp, typeof(RpbCounterGetResp) }
            };

            TypeToMessageCodeMap = new Dictionary<Type, MessageCode>();

            foreach(var item in MessageCodeToTypeMap)
            {
                TypeToMessageCodeMap.Add(item.Value, item.Key);
            }
        }

        private async Task<Socket> GetConnectedSocket()
        {
            if (_socket != null)
            {
                return _socket;
            }

            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
            {
                NoDelay = true,
                ReceiveTimeout = _receiveTimeout,
                SendTimeout = _sendTimeout
            };

            await ConnectAsync(_socket, _endPoint);

            return _socket;
        }

        public async Task Write(MessageCode messageCode)
        {
            const int sizeSize = sizeof(int);
            const int codeSize = sizeof(byte);
            const int headerSize = sizeSize + codeSize;

            var buffer = _blockingBufferManager.GetBuffer();
            try
            {
                var size = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(codeSize));

                var messageBody = new ArraySegment<byte>(
                    buffer.Array,
                    0,
                    headerSize);

                Array.Copy(size, messageBody.Array, sizeSize);
                messageBody.Array[sizeSize] = (byte) messageCode;

                var socket = await GetConnectedSocket();
                await SendAsync(socket, messageBody);
            }
            finally
            {
                _blockingBufferManager.ReleaseBuffer(buffer);
            }
        }

        public async Task Write<T>(T message) where T : class
        {
            const int sizeSize = sizeof(int);
            const int codeSize = sizeof(byte);
            const int headerSize = sizeSize + codeSize;

            var messageCode = TypeToMessageCodeMap[typeof(T)];

            if (message == null)
            {
                await Write(messageCode);
                return;
            }

            var buffer = _blockingBufferManager.GetBuffer();
            try
            {
                using (var stream = new ArraySegmentStream(buffer.Array))
                {
                    stream.Position += headerSize;
                    Serializer.Serialize(stream, message);
                    var messageLength = stream.Position;

                    var size = BitConverter.GetBytes(IPAddress.HostToNetworkOrder((int)messageLength - sizeSize));
                    Array.Copy(size, buffer.Array, sizeSize);
                    buffer.Array[sizeSize] = (byte)messageCode;

                    var messageBody = new ArraySegment<byte>(
                        buffer.Array,
                        0,
                        (int)stream.Length);

                    var socket = await GetConnectedSocket();
                    await SendAsync(socket, messageBody);
                }
            }
            finally
            {
                _blockingBufferManager.ReleaseBuffer(buffer);
            }
        }

        public async Task<MessageCode> Read(MessageCode expectedCode)
        {
            const int sizeSize = sizeof(int);
            const int codeSize = sizeof(byte);
            const int headerSize = sizeSize + codeSize;

            var buffer = _blockingBufferManager.GetBuffer();
            try
            {
                var socket = await GetConnectedSocket();

                var headerBuffer = new ArraySegment<byte>(buffer.Array, 0, headerSize);

                await ReceiveAsync(socket, headerBuffer);
                
                var size = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(headerBuffer.Array, 0));
                var messageCode = (MessageCode)headerBuffer.Array[sizeof(int)];
                
                if (messageCode == MessageCode.ErrorResp)
                {
                    var errorBuffer = new ArraySegment<byte>(buffer.Array, headerSize, size-codeSize);

                    await ReceiveAsync(socket, errorBuffer);
                    
                    using (var stream = new MemoryStream(errorBuffer.Array, errorBuffer.Offset, errorBuffer.Count))
                    {
                        var error = Serializer.Deserialize<RpbErrorResp>(stream);
                        throw new RiakException(error.errcode, error.errmsg.FromRiakString(), false);
                    }
                }

                if (expectedCode != messageCode)
                {
                    throw new RiakException("Expected return code {0} received {1}".Fmt(expectedCode, messageCode));
                }

                return messageCode;
            }
            finally
            {
                _blockingBufferManager.ReleaseBuffer(buffer);
            }
        }

        public async Task<T> Read<T>() where T : new()
        {
            const int sizeSize = sizeof (int);
            const int codeSize = sizeof (byte);
            const int headerSize = sizeSize + codeSize;

            var buffer = _blockingBufferManager.GetBuffer();
            try
            {
                var socket = await GetConnectedSocket();

                var headerBuffer = new ArraySegment<byte>(buffer.Array, 0, headerSize);

                await ReceiveAsync(socket, headerBuffer);
                
                var size = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(headerBuffer.Array, 0));
                var messageCode = (MessageCode) headerBuffer.Array[sizeof (int)];

                if (messageCode == MessageCode.ErrorResp)
                {
                    var errorBuffer = new ArraySegment<byte>(buffer.Array, headerSize, size - codeSize);

                    await ReceiveAsync(socket, errorBuffer);
 
                    using (var stream = new MemoryStream(errorBuffer.Array, errorBuffer.Offset, errorBuffer.Count))
                    {
                        var error = Serializer.Deserialize<RpbErrorResp>(stream);
                        throw new RiakException(error.errcode, error.errmsg.FromRiakString(), false);
                    }
                }

                if (!MessageCodeToTypeMap.ContainsKey(messageCode))
                {
                    throw new RiakInvalidDataException((byte) messageCode);
                }
#if DEBUG
                // This message code validation is here to make sure that the caller
                // is getting exactly what they expect. This "could" be removed from
                // production code, but it's a good thing to have in here for dev.
                if (MessageCodeToTypeMap[messageCode] != typeof (T))
                {
                    throw new InvalidOperationException(
                        string.Format("Attempt to decode message to type '{0}' when received type '{1}'.",
                            typeof (T).Name, MessageCodeToTypeMap[messageCode].Name));
                }
#endif

                if (size - codeSize <= 1)
                {
                    return new T();
                }

                var bodyBuffer = new ArraySegment<byte>(buffer.Array, headerSize, size - codeSize);

                await ReceiveAsync(socket, bodyBuffer);
 
                using (var stream = new MemoryStream(bodyBuffer.Array, bodyBuffer.Offset, bodyBuffer.Count))
                {
                    var message = Serializer.Deserialize<T>(stream);
                    return message;
                }
            }
            finally
            {
                _blockingBufferManager.ReleaseBuffer(buffer);
            }
        }

        public async Task Disconnect()
        {
            if (_socket == null) return;

            var awaitable = _socketAwaitablePool.Take();
            try
            {
                await _socket.DisonnectAsync(awaitable);
            }
            finally
            {
                awaitable.Clear();
                _socketAwaitablePool.Add(awaitable);
            }

            _socket.Dispose();
            _socket = null;
        }

        public void Dispose()
        {
            Disconnect().Wait();
        }
    }
}