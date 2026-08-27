using System.Collections.Concurrent;
using System.Net.Sockets;

//Molde de cada peer - cérebro

namespace SocketChat
{
    //Sealed - Não permite que a classe seja herdada.
    public sealed class Node
    {
        /*
            Cria uma constante de configuração fixa que não muda durante a execução.

            TimeSpan - representa um intervalo de tempo.

            FromSeconds(5) - intervalo de 5 segundos.

            static - pertence à classe em si, e não aos objetos.

            readonly - só pode ser definido uma vez e nunca mais mudado.

            ReconnectDelay - é o tempo de espera entre as tentativas de reconexão. A cada 5 segundos tenta novamente.

            QuitSendTimeout - Prazo máximo para avisar que está saindo do chat, quando dá /quit.
        */
        private static readonly TimeSpan ReconnectDelay = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan QuitSendTimeout = TimeSpan.FromSeconds(2);

        /*
            apelido -> após definido, nunca mais muda.

            porta de escuta para o nó.

            lista de endereços conhecidos (peers).

            Socket -> escuta as novas ligações.

            CancellationToken -> serve para encerrar adequadamente.

            CourrentDictionary -> lista de quem está conectado agora.
                nick e detalhes de conexão.
            
            dedupLock -> objeto usado como trava.
        */
        private readonly string _nick;
        private readonly int _port;
        private readonly List<(string Host, int Port)> _peerAddresses;
        private readonly Socket _listener;
        private readonly CancellationTokenSource _cts = new();
        private readonly ConcurrentDictionary<string, PeerConnection> _peers = new();
        private readonly object _dedupLock = new();
        private volatile bool _quitting;


        /*
            Construtor

            Recebe os valores lá de Program.cs

            Parâmetros: apelido, porta, endereço dos peers conhecidos.

            listener -> cria o canal a partir da porta para receber conexões.
        */
        public Node(string nick, int port, List<(string Host, int Port)> peerAddresses)
        {
            _nick = nick;
            _port = port;
            _peerAddresses = peerAddresses;
            _listener = Connection.CreateListener(port);
        }


        /*
            Liga o Chat.

            ct = armazena o token de cancelamento que será utilizado em outras funções, para que o cancelamento venha pelo mesmo sinal.

            _ = AcceptLoopAsync() -> faz a função rodar em background sem esperar ela terminar e sem guardar o resultado. A função é responsável por ficar escutando e aceitando novas conexões.
        */
        public async Task RunAsync()
        {
            var ct = _cts.Token;

            _ = AcceptLoopAsync(ct);

            /*
                Para cada endereço da lista de peers, dispara uma tarefa separada e simultânea (paralelo) de tentativa de conexão.

                await ConsoleLoopAsync(ct) -> espera que ConsoleLoopAsync termine, ou seja, /quit ou fechando o input
            */
            foreach (var (host, port) in _peerAddresses)
                _ = ConnectWithRetryAsync(host, port, ct);

            await ConsoleLoopAsync(ct);
        }

        /*
            while (!ct.IsCancellationRequested) -> continua esperando enquanto não foi pedido o cancelamento.
        */
        private async Task AcceptLoopAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                /*
                    Fica esperando uma conexão nova chegar (await) por meio do listener criado no construtor.
                */
                Socket socket;
                try
                {
                    socket = await Connection.AcceptAsync(_listener, ct);
                }
                /*
                    Cancelamento de conexão -> return -> sai da função e encerra o loop.
                */
                catch (OperationCanceledException)
                {
                    return;
                }
                /*
                    Pega todos os outros erros e tenta aceitar a próxima conexão.
                */
                catch
                {
                    continue;
                }

                /*
                    Quando uma nova conexão tem sucesso, chama a função responsável por processar a conexão de verdade.

                    isOutBound: false -> significa que a coneão não foi iniciada pelo peer, ele apenas aceitou.
                */
                _ = HandleNewConnectionAsync(socket, isOutbound: false, ct);
            }
        }

        /*
            Função -> lidar com as conexões em que o peer de contato não estiver ativo.

            Tenta se conectar usando a função ConnectAsync do Connection.cs com timout de 10 segundos. 

            Se conseguir, processa a conexão e sai da função.

            Erro de cancelamento.

            Se a conexão falhar por outros motivos, espera-se 5s (Task.Delay(ReconnectDelay, ct)) e tenta de novo. O try/catch interno serve para tratar o cancelamento de operação caso ocorra durante o delay.
        */
        private async Task ConnectWithRetryAsync(string host, int port, CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var socket = await Connection.ConnectAsync(host, port, ct);
                    await HandleNewConnectionAsync(socket, isOutbound: true, ct);
                    return;
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch
                {
                    try { await Task.Delay(ReconnectDelay, ct); }
                    catch (OperationCanceledException) { return; }
                }
            }
        }

        /*
            Recepção de toda conexão nova, sendo aceita ou iniciada por esse nó.

            Primeira parte (try/catch) - conexão TCP crua.

            await Frames.WriteAsync(socket, Protocol.EncodeHello(_nick, _port), ct); -> monta o protocolo de entrada HELLO|nickname|port e depois transforma em bytes.

            var frame = await Frames.ReadAsync(socket, ct); -> fica esperando o mesmo tipo de mensagem do outro lado.

            frame is null -> a conexão foi fechada antes do outro lado mandar qualquer coisa -> o outro lado desconectou. Discarta o socket e sai da função.

            var hello = Protocol.Decode(frame); -> decodifica a mensagem recebida e verifica se houve algum erro. Se sim, descarta a conexão. Se não, armazena o apelido.
        */
        private async Task HandleNewConnectionAsync(Socket socket, bool isOutbound, CancellationToken ct)
        {
            string peerNick;
            try
            {
                await Frames.WriteAsync(socket, Protocol.EncodeHello(_nick, _port), ct);
                var frame = await Frames.ReadAsync(socket, ct);
                if (frame is null)
                {
                    socket.Dispose();
                    return;
                }

                var hello = Protocol.Decode(frame);
                if (hello.Type != MessageType.Hello)
                {
                    socket.Dispose();
                    return;
                }
                peerNick = hello.Nick;
            }
            catch
            {
                socket.Dispose();
                return;
            }

            /*
                isOutbound ? _nick : peerNick -> if -> se for true o valor é _nick, se for false é peerNick. Quer dizer que, se foi o peer que iniciou a conexão, o iniciador é ele mesmo (_nick), se foi o outro lado que iniciou a conexão, então o iniciador é peerNick

                pc -> objeto PeerConnection -> representa uma conversa individual com um peer.
            */
            var initiatorNick = isOutbound ? _nick : peerNick;
            var pc = new PeerConnection(peerNick, initiatorNick, socket);

            /*
                chama TryRegister -> desempacota o resultado em duas variáveis: ok -> deu certo registrar? e loser -> existe uma conexão perdedora que precisa ser fechada?

                if(!ok) -> não deu certo registrar -> a nova conexão deve ser descartada -> pc.Close().
            */
            var (ok, loser) = TryRegister(pc);
            if (!ok)
            {
                pc.Close();
                return;
            }

            /*
                Se o registro de certo e havia uma conexão antiga para ser substituida, fecha a conexão velha.
            */
            if (loser is not null)
            {
                Console.WriteLine($"[link duplicado com {pc.Nick} substituído]");
                loser.Close();
            }

            /*
                Anuncia entrada na conversa.
            */
            Console.WriteLine($"[{pc.Nick} entrou na conversa]");
            await pc.RunAsync(
                ct,
                frame => { Dispatch(pc, frame); return Task.CompletedTask; },
                () => RemovePeer(pc.Nick, pc));
        }


        /*
            retorna tupla - ok (conseguiu registrar?) e loser (existe um objeto antigo que precisa ser deletado?)
        */
        private (bool ok, PeerConnection? loser) TryRegister(PeerConnection pc)
        {

            /*
               lock (_dedupLock) -> trava -> garante que só uma conexão tente se registrar por vez, evitando discrepâncias. 
            */
            lock (_dedupLock)
            {
                /*
                    Verifica se existem duas conexões registradas com o mesmo nick.

                    Desempate de conexão duplicada: se A e B discam um pro outro ao mesmo tempo, cada nó decide sozinho (sem coordenação) qual conexão mantém, comparando o nick de quem iniciou cada uma em ordem alfabética. Iniciador "menor" vence; a conexão perdedora é fechada. Como os dois lados enxergam os mesmos 2 nicks candidatos, chegam à mesma decisão de forma independente.
                */
                if (_peers.TryGetValue(pc.Nick, out var existing))
                {
                    if (string.CompareOrdinal(pc.InitiatorNick, existing.InitiatorNick) < 0)
                    {
                        _peers[pc.Nick] = pc;
                        return (true, existing);
                    }
                    return (false, null);
                }

                _peers[pc.Nick] = pc;
                return (true, null);
            }
        }

        /*
            Tira um peer da lista e avisa que ele saiu.

            Removendo apenas pelo nick, pode remover acidentalmente uma conexão nova de mesmo nick erroneamente. Por isso, valida a combinação de nick e peerConnection.

            ((ICollection<KeyValuePair<string, PeerConnection>>)_peers) -> conversão de tipo para conseguir usar Remove() -> remove só aceita uma chave, então combina nick+pc.
        */
        private void RemovePeer(string nick, PeerConnection pc)
        {
            var removed = ((ICollection<KeyValuePair<string, PeerConnection>>)_peers)
                .Remove(new KeyValuePair<string, PeerConnection>(nick, pc));
            pc.Close();
            if (removed && !_quitting)
                Console.WriteLine($"[{nick} saiu da conversa]");
        }


        /*
            Decide o que fazer com cada mensagem recebida.

            Primeiro tenta decodificar os bytes recebidos em uma mensagem organizada.

            msg.Type -> tipo da mensagem -> Msg ([nick] texto), PrivMsg ([Pv de nick] texto), Bye (Remove o peer e avisa que ele saiu), Hello (mensagem de oi).
        */
        private void Dispatch(PeerConnection pc, byte[] frame)
        {
            ChatMessage msg;
            try
            {
                msg = Protocol.Decode(frame);
            }
            catch
            {
                return;
            }

            switch (msg.Type)
            {
                case MessageType.Msg:
                    Console.WriteLine($"[{msg.Nick}] {msg.Text}");
                    break;
                case MessageType.PrivMsg:
                    Console.WriteLine($"[PV de {msg.Nick}] {msg.Text}");
                    break;
                case MessageType.Bye:
                    RemovePeer(pc.Nick, pc);
                    break;
                case MessageType.Hello:
                    break; // já tratado no handshake
            }
        }

        /*
            Mandar mensagem para outro peer.

            Monta a mensagem codificada e passa para cada conexão registrada.
        */
        private void SendBroadcast(string text)
        {
            var frame = Protocol.EncodeMsg(_nick, text);
            foreach (var pc in _peers.Values)
                pc.Enqueue(frame);
            Console.WriteLine($"[{_nick}] {text}");
        }

        /*
            Mensagem privada.

            Tenta encontrar o peer com aquele apelido.

            Se não encontrar, avisa e segue em frente.
        */
        private void SendPrivate(string toNick, string text)
        {
            if (!_peers.TryGetValue(toNick, out var pc))
            {
                Console.WriteLine($"[aviso] participante '{toNick}' não encontrado.");
                return;
            }
            pc.Enqueue(Protocol.EncodePrivMsg(_nick, toNick, text));
            Console.WriteLine($"[PV para {toNick}] {text}");
        }

        /*
            Imprime os peers conectados (somente os nicks).
        */
        private void ListPeers()
        {
            if (_peers.IsEmpty)
            {
                Console.WriteLine("(nenhum participante conectado)");
                return;
            }
            Console.WriteLine("Participantes conectados: " + string.Join(", ", _peers.Keys));
        }

        /*
            Monta a mensagem BYE e manda para todos os peers conectados.
        */
        private async Task QuitAsync()
        {
            _quitting = true;

            var bye = Protocol.EncodeBye(_nick);
            foreach (var pc in _peers.Values)
            {
                try
                {
                    using var cts = new CancellationTokenSource(QuitSendTimeout);
                    await Frames.WriteAsync(pc.Socket, bye, cts.Token);
                }
                catch
                {
                }
            }
            _cts.Cancel();
        }

        /*
            Chamado a partir do handler de Ctrl+C. Envia BYE (melhor esforço) e encerra o processo.
        */
        public void RequestShutdown()
        {
            _ = Task.Run(async () =>
            {
                await QuitAsync();
                Environment.Exit(0);
            });
        }

        /*
            Lê o que o peer digita.

            Mostra as mensagens de boas vindas e instruções.

            while (!ct.IsCancellationRequested) -> fica em loop enquanto não foi pedido cancelamento.

            var line = await Task.Run(Console.ReadLine, ct); -> aguarda a leitura.

            if (line is null) break; -> quando RealLine devolve null (input encerrado de forma inesperada).

            line.Equals("...", StringComparison.OrdinalIgnoreCase) -> ignora maiúsculas/minúsculas.
        */
        private async Task ConsoleLoopAsync(CancellationToken ct)
        {
            Console.WriteLine($"Nó '{_nick}' escutando na porta {_port}.");
            Console.WriteLine("Comandos: /list | /msg <apelido> <texto> | /quit");
            Console.WriteLine();

            while (!ct.IsCancellationRequested)
            {
                var line = await Task.Run(Console.ReadLine, ct);
                if (line is null)
                    break; // stdin fechado (EOF)

                line = line.Trim();
                if (line.Length == 0)
                    continue;

                if (line.Equals("/quit", StringComparison.OrdinalIgnoreCase))
                {
                    await QuitAsync();
                    break;
                }
                else if (line.Equals("/list", StringComparison.OrdinalIgnoreCase))
                {
                    ListPeers();
                }
                else if (line.StartsWith("/msg ", StringComparison.OrdinalIgnoreCase))
                {
                    var rest = line[5..].Trim();
                    var idx = rest.IndexOf(' ');
                    if (idx < 0)
                    {
                        Console.WriteLine("uso: /msg <apelido> <texto>");
                        continue;
                    }
                    SendPrivate(rest[..idx], rest[(idx + 1)..]);
                }
                else if (line.StartsWith('/'))
                {
                    Console.WriteLine("comando desconhecido.");
                }
                else
                {
                    SendBroadcast(line);
                }
            }
        }
    }
}
