using System;
using System.Threading.Tasks;
using CorrugatedIron.Messages;
using CorrugatedIron.Models.Rest;

namespace CorrugatedIron.Comms
{
    public interface IRiakConnection : IDisposable
    {
        bool IsIdle { get; }

        void Disconnect();

        // PBC interface
        Task<RiakResult<TResult>> PbcRead<TResult>()
            where TResult : class, new();

        Task<RiakResult> PbcRead(MessageCode expectedMessageCode);

        Task<RiakResult> PbcWrite<TRequest>(TRequest request)
            where TRequest : class;

        Task<RiakResult> PbcWrite(MessageCode messageCode);

        Task<RiakResult<TResult>> PbcWriteRead<TRequest, TResult>(TRequest request)
            where TRequest : class
            where TResult : class, new();

        Task<RiakResult<TResult>> PbcWriteRead<TResult>(MessageCode messageCode)
            where TResult : class, new();

        Task<RiakResult> PbcWriteRead<TRequest>(TRequest request, MessageCode expectedMessageCode)
            where TRequest : class;

        Task<RiakResult> PbcWriteRead(MessageCode messageCode, MessageCode expectedMessageCode);

        Task<RiakResult<IObservable<RiakResult<TResult>>>> PbcRepeatRead<TResult>(Func<RiakResult<TResult>, bool> repeatRead)
            where TResult : class, new();

        Task<RiakResult<IObservable<RiakResult<TResult>>>> PbcWriteRead<TResult>(MessageCode messageCode, Func<RiakResult<TResult>, bool> repeatRead)
            where TResult : class, new();

        Task<RiakResult<IObservable<RiakResult<TResult>>>> PbcWriteRead<TRequest, TResult>(TRequest request, Func<RiakResult<TResult>, bool> repeatRead)
            where TRequest : class
            where TResult : class, new();

        Task<RiakResult<IObservable<RiakResult<TResult>>>> PbcStreamRead<TResult>(Func<RiakResult<TResult>, bool> repeatRead, Action onFinish)
            where TResult : class, new();

        Task<RiakResult<IObservable<RiakResult<TResult>>>> PbcWriteStreamRead<TRequest, TResult>(TRequest request,
            Func<RiakResult<TResult>, bool> repeatRead, Action onFinish)
            where TRequest : class
            where TResult : class, new();

        Task<RiakResult<IObservable<RiakResult<TResult>>>> PbcWriteStreamRead<TResult>(MessageCode messageCode,
            Func<RiakResult<TResult>, bool> repeatRead, Action onFinish)
            where TResult : class, new();

        // REST interface
        Task<RiakResult<RiakRestResponse>> RestRequest(RiakRestRequest request);
    }
}