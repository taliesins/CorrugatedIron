using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CorrugatedIron.Comms
{
    public interface IRiakNode : IDisposable
    {
        Task<RiakResult> UseConnection(Func<IRiakConnection, Task<RiakResult>> useFun);
        Task<RiakResult<TResult>> UseConnection<TResult>(Func<IRiakConnection, Task<RiakResult<TResult>>> useFun);
        Task<RiakResult<IEnumerable<TResult>>> UseConnection<TResult>(Func<IRiakConnection, Task<RiakResult<IEnumerable<TResult>>>> useFun);
    }
}