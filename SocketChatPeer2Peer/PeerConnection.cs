using System.Net.Sockets;
using System.Threading.Channels;

//Conversa Individual


namespace SocketChat
{
    /*
        Se quebrar, não quebra o resto da aplicação, apenas essa conexão
    */
    public sealed class PeerConnection
    {
        /*
            Prazo máximo para conseguir enviar uma mensagem para um peer.

            apelida, socket, quem começou a conexão;
        */
        private static readonly TimeSpan SendTimeout = TimeSpan.FromSeconds(5);

        public string Nick { get; }
        public Socket Socket { get; }

        public string InitiatorNick { get; }

        /*
            fila de saída -> armazenas as mensagens pendentes para envio até de fato serem enviadas.
        */
        private readonly Channel<byte[]> _outbox;

        public PeerConnection(string nick, string initiatorNick, Socket socket)
        {
            Nick = nick;
            InitiatorNick = initiatorNick;
            Socket = socket;

            /*
                Channel -> funciona como uma esteira com limite de tamanho que armazena as mensagens já convertidas em byte.
            */
            _outbox = Channel.CreateBounded<byte[]>(new BoundedChannelOptions(100)
            {
                /*
                    FullMode = BoundedChannelFullMode.DropOldest -> quando a fila enche, descarta a mensagem mais antiga e aceita uma nova, evitando de travar a fila.
                */
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = true,
                SingleWriter = false,
            });
        }

        /*
            Coloca mensagem na fila.
        */
        public void Enqueue(byte[] frame) => _outbox.Writer.TryWrite(frame);

        /*
            dispara os loops de escrita e leitura, esperando um dos lados terminarem.
        */
        public async Task RunAsync(CancellationToken appCt, Func<byte[], Task> onFrameReceived, Action onDead)
        {
            var writeTask = WriteLoopAsync(appCt, onDead);
            var readTask = ReadLoopAsync(appCt, onFrameReceived, onDead);
            await Task.WhenAll(writeTask, readTask);
        }

        /*
            await foreach -> tipo de loop que espera itens chegando de forma assíncrona.

            _outbox.Reader -> retira os itens da fila.

            .ReadAllAsync(appCt) -> espera os itens aparecerem na fila, processando cada um assim que chega.
        */
        private async Task WriteLoopAsync(CancellationToken appCt, Action onDead)
        {
            try
            {
                await foreach (var frame in _outbox.Reader.ReadAllAsync(appCt))
                {
                    using var cts = CancellationTokenSource.CreateLinkedTokenSource(appCt);
                    cts.CancelAfter(SendTimeout);
                    try
                    {
                        await Frames.WriteAsync(Socket, frame, cts.Token);
                    }
                    catch (OperationCanceledException) when (!appCt.IsCancellationRequested)
                    {
                        onDead();
                        return;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // encerramento normal da aplicação
            }
            catch
            {
                onDead();
            }
        }

        /*
            Recebe as mensagens.
        */
        private async Task ReadLoopAsync(CancellationToken appCt, Func<byte[], Task> onFrameReceived, Action onDead)
        {
            try
            {
                while (true)
                {
                    // Sem timeout de leitura: aguardar a próxima mensagem é o comportamento
                    // esperado de um chat. Só é limitado pelo CancellationToken geral da app.
                    var frame = await Frames.ReadAsync(Socket, appCt);
                    if (frame is null)
                    {
                        onDead();
                        return;
                    }
                    await onFrameReceived(frame);
                }
            }
            catch (OperationCanceledException)
            {
                // encerramento normal da aplicação
            }
            catch
            {
                onDead();
            }
        }

        /*
            Encerra essa conexão.
        */
        public void Close()
        {
            try { Socket.Shutdown(SocketShutdown.Both); } catch { }
            try { Socket.Dispose(); } catch { }
            _outbox.Writer.TryComplete();
        }
    }
}
