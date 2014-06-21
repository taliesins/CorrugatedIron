using System;
using System.Collections.Generic;

namespace CorrugatedIron.Comms
{
    public interface IRiakNode : IDisposable
    {
        RiakResult UseConnection(Func<IRiakConnection, RiakResult> useFun);
        RiakResult<TResult> UseConnection<TResult>(Func<IRiakConnection, RiakResult<TResult>> useFun);

        RiakResult<IEnumerable<TResult>> UseDelayedConnection<TResult>(Func<IRiakConnection, Action, RiakResult<IEnumerable<TResult>>> useFun)
            where TResult : RiakResult;
    }
}