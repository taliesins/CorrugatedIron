using System.Net.Sockets;

namespace CorrugatedIron.Comms.AsyncSocket
{
    public static class SocketExtensions
    {
        public static SocketAwaitable AcceptAsync(this Socket socket, SocketAwaitable awaitable)
        {
            awaitable.Reset();
            if (!socket.AcceptAsync(awaitable.EventArgs))
            {
                awaitable.WasCompleted = true;
            }
            return awaitable;
        }

        public static SocketAwaitable ConnectAsync(this Socket socket, SocketAwaitable awaitable)
        {
            awaitable.Reset();
            if (!socket.ConnectAsync(awaitable.EventArgs))
            {
                awaitable.WasCompleted = true;
            }
            return awaitable;
        }

        public static SocketAwaitable DisconnectAsync(this Socket socket, SocketAwaitable awaitable)
        {
            awaitable.Reset();
            if (!socket.DisconnectAsync(awaitable.EventArgs))
            {
                awaitable.WasCompleted = true;
            }
            return awaitable;
        }

        public static SocketAwaitable ReceiveAsync(this Socket socket, SocketAwaitable awaitable)
        {
            awaitable.Reset();
            if (!socket.ReceiveAsync(awaitable.EventArgs))
            {
                awaitable.WasCompleted = true;
            }
            return awaitable;
        }

        public static SocketAwaitable ReceiveFromAsync(this Socket socket, SocketAwaitable awaitable)
        {
            awaitable.Reset();
            if (!socket.ReceiveFromAsync(awaitable.EventArgs))
            {
                awaitable.WasCompleted = true;
            }
            return awaitable;
        }

        public static SocketAwaitable ReceiveMessageFromAsync(this Socket socket, SocketAwaitable awaitable)
        {
            awaitable.Reset();
            if (!socket.ReceiveMessageFromAsync(awaitable.EventArgs))
            {
                awaitable.WasCompleted = true;
            }
            return awaitable;
        }

        public static SocketAwaitable SendAsync(this Socket socket, SocketAwaitable awaitable)
        {
            awaitable.Reset();
            if (!socket.SendAsync(awaitable.EventArgs))
            {
                awaitable.WasCompleted = true;
            }
            return awaitable;
        }

        public static SocketAwaitable SendPacketsAsync(this Socket socket, SocketAwaitable awaitable)
        {
            awaitable.Reset();
            if (!socket.SendPacketsAsync(awaitable.EventArgs))
            {
                awaitable.WasCompleted = true;
            }
            return awaitable;
        }

        public static SocketAwaitable SendToAsync(this Socket socket, SocketAwaitable awaitable)
        {
            awaitable.Reset();
            if (!socket.SendToAsync(awaitable.EventArgs))
            {
                awaitable.WasCompleted = true;
            }
            return awaitable;
        }
    }
}
