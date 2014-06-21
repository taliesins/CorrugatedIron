using CorrugatedIron.Config;

namespace CorrugatedIron.Comms
{
    public interface IRiakConnectionFactory
    {
        IRiakConnection CreateConnection(IRiakNodeConfiguration nodeConfiguration);
    }
}