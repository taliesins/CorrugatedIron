using System;
using System.Threading.Tasks;

namespace CorrugatedIron.Comms
{
    public interface IRiakNode : IDisposable
    {   
        RiakPbcSocket CreateSocket();
        void Release(RiakPbcSocket socket);

        Task GetSingleResultViaPbc(Func<RiakPbcSocket, Task> useFun);
        Task<TResult> GetSingleResultViaPbc<TResult>(Func<RiakPbcSocket, Task<TResult>> useFun);
        Task GetSingleResultViaPbc(RiakPbcSocket socket, Func<RiakPbcSocket, Task> useFun);
        Task<TResult> GetSingleResultViaPbc<TResult>(RiakPbcSocket socket, Func<RiakPbcSocket, Task<TResult>> useFun);
        Task GetMultipleResultViaPbc(Action<RiakPbcSocket> useFun);
        Task GetMultipleResultViaPbc(RiakPbcSocket socket, Action<RiakPbcSocket> useFun);

        Task GetSingleResultViaRest(Func<string, Task> useFun);
        Task<TResult> GetSingleResultViaRest<TResult>(Func<string, Task<TResult>> useFun);
        Task GetMultipleResultViaRest(Action<string> useFun);
    }
}