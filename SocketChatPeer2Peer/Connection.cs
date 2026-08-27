using System.Net;
using System.Net.Sockets;

//Chat para vários peers

namespace SocketChat
{

    /*
        Static -> não pode ser instanciada -> chamadas diretamente pelo nome da classe
    */
    public static class Connection
    {
        private static readonly TimeSpan ConnectTimeout = TimeSpan.FromSeconds(10);

        /*
            Abrindo para conexões.

            new Socket(...); -> Cria um socket cru.

            listener.SetSocketOption(...) -> permite que essa porta seja reutilizada rapidamente, mesmo que uma conexão anterior nela ainda esteja "fechando".

            listener.Bind(new IPEndPoint(IPAddress.Any, port)) -> associa o socket a porta específica e aceita conexões de qualquer endereço.

            listener.Listen(20) -> tamanho da fila de conexões pendentes -> quantas conexões podem estar esperando na porta para serem atendidas ao mesmo tempo.

        */
        public static Socket CreateListener(int port)
        {
            var listener = new Socket(
                AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            listener.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            listener.Bind(new IPEndPoint(IPAddress.Any, port));
            listener.Listen(20);
            return listener;
        }

        /*
            Chama para cada conexão nova.
        */
        public static async Task<Socket> AcceptAsync(Socket listener, CancellationToken ct)
        {
            var peer = await listener.AcceptAsync(ct);
            peer.NoDelay = true;
            return peer;
        }

        /*
            se conectando ativamente com algum peer.
        */
        public static async Task<Socket> ConnectAsync(string host, int port, CancellationToken ct)
        {
            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
            { NoDelay = true };
            try
            {
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
                timeout.CancelAfter(ConnectTimeout);
                await socket.ConnectAsync(host, port, timeout.Token);
                return socket;
            }
            catch
            {
                socket.Dispose();
                throw;
            }
        }
    }
}
