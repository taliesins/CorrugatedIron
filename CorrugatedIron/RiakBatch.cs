using System;
using System.Threading.Tasks;
using CorrugatedIron.Comms;

namespace CorrugatedIron
{
    public class RiakBatch : IRiakEndPoint
    {
        private readonly IRiakEndPoint _endPoint;
        private readonly IRiakEndPointContext _endPointContext;

        public RiakBatch(IRiakEndPoint endPoint)
        {
            _endPoint = endPoint;
            _endPointContext = new RiakEndPointContext();
        }

        public void Dispose()
        {
        }

        public IRiakClient CreateClient()
        {
            return _endPoint.CreateClient();
        }

        public Task GetSingleResultViaPbc(Func<RiakPbcSocket, Task> useFun)
        {
            return _endPoint.GetSingleResultViaPbc(_endPointContext, useFun);
        }

        public Task<TResult> GetSingleResultViaPbc<TResult>(Func<RiakPbcSocket, Task<TResult>> useFun)
        {
            return _endPoint.GetSingleResultViaPbc(_endPointContext, useFun);
        }

        public Task GetMultipleResultViaPbc(Action<RiakPbcSocket> useFun)
        {
            return _endPoint.GetMultipleResultViaPbc(_endPointContext, useFun);
        }

        public Task GetSingleResultViaPbc(IRiakEndPointContext riakEndPointContext, Func<RiakPbcSocket, Task> useFun)
        {
            return _endPoint.GetSingleResultViaPbc(riakEndPointContext, useFun);
        }

        public Task<TResult> GetSingleResultViaPbc<TResult>(IRiakEndPointContext riakEndPointContext, Func<RiakPbcSocket, Task<TResult>> useFun)
        {
            return _endPoint.GetSingleResultViaPbc(riakEndPointContext, useFun);
        }

        public Task GetMultipleResultViaPbc(IRiakEndPointContext riakEndPointContext, Action<RiakPbcSocket> useFun)
        {
            return _endPoint.GetMultipleResultViaPbc(riakEndPointContext, useFun);
        }

        public Task GetSingleResultViaRest(Func<string, Task> useFun)
        {
            return _endPoint.GetSingleResultViaRest(useFun);
        }

        public Task<TResult> GetSingleResultViaRest<TResult>(Func<string, Task<TResult>> useFun)
        {
            return _endPoint.GetSingleResultViaRest(useFun);
        }

        public Task GetMultipleResultViaRest(Action<string> useFun)
        {
            return _endPoint.GetMultipleResultViaRest(useFun);
        }
    }
}
