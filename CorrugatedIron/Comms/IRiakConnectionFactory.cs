using CorrugatedIron.Comms.Sockets;
using CorrugatedIron.Config;

namespace CorrugatedIron.Comms
{
    public interface IRiakConnectionFactory
    {
        IRiakConnection CreateConnection(IRiakNodeConfiguration nodeConfiguration, SocketAwaitablePool socketAwaitablePool, BlockingBufferManager blockingBufferManager);
    }
}