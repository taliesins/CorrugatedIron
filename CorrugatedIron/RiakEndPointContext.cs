using CorrugatedIron.Comms;

namespace CorrugatedIron
{
    public class RiakEndPointContext : IRiakEndPointContext
    {
        public IRiakNode Node { get; set; }
        public RiakPbcSocket Socket { get; set; }
        public void Dispose()
        {
            if (Node != null && Socket != null)
            {
                Node.Release(Socket);
            }
        }
    }
}