# SocketChat — chat P2P em malha completa

Chat distribuído em C#, sobre `Socket` TCP puro (`System.Net.Sockets`), sem servidor
central, sem coordenador e sem retransmissor. Cada nó mantém uma conexão direta
com todos os outros nós da rede (malha completa / full mesh) e envia suas
mensagens ponto a ponto para cada um.

## Build e execução

```
dotnet build
dotnet run -- --port <porta> --nick <apelido> [--peers host:porta,host:porta,...]
```

- `--port`: porta local de escuta.
- `--nick`: apelido do participante (deve ser único na rede; não pode conter `|`).
- `--peers`: lista opcional de pares conhecidos no formato `host:porta`, separados
  por vírgula. Pode ser vazia para o primeiro nó da rede.

### Exemplo com 3 participantes (malha completa)

Terminal 1:
```
dotnet run -- --port 9001 --nick alice --peers 127.0.0.1:9002,127.0.0.1:9003
```
Terminal 2:
```
dotnet run -- --port 9002 --nick bob --peers 127.0.0.1:9001,127.0.0.1:9003
```
Terminal 3:
```
dotnet run -- --port 9003 --nick carol --peers 127.0.0.1:9001,127.0.0.1:9002
```

A ordem de subida não importa: cada nó insiste em conectar aos peers configurados
a cada 5s até conseguir, então mesmo que um terminal suba antes dos outros a malha
se fecha sozinha assim que todos estiverem de pé.

Para N participantes, basta cada instância listar em `--peers` os endereços de
*todas* as outras — não existe descoberta automática nem propagação de lista de
peers por terceiros.

## Comandos

- Digitar texto sem `/` → broadcast (`MSG`) para todos os peers conectados, envio
  direto ponto a ponto (sem retransmissão via terceiros).
- `/msg <apelido> <texto>` → mensagem privada (`PRIVMSG`) só para aquele peer; se
  o apelido não for conhecido, avisa localmente sem quebrar a sessão.
- `/list` → lista os apelidos atualmente conectados.
- `/quit` (ou Ctrl+C) → anuncia saída (`BYE`) a todos os peers e encerra.

## Protocolo de aplicação

Uma linha de texto por frame (já delimitado pelo framing existente em `Frames.cs`),
campos separados por `|`:

- `HELLO|<nick>|<listenPort>` — trocado nos dois sentidos assim que uma conexão
  TCP é estabelecida (recebida ou iniciada), antes de qualquer outra mensagem.
- `MSG|<nick>|<texto>` — broadcast.
- `PRIVMSG|<deNick>|<paraNick>|<texto>` — mensagem privada.
- `BYE|<nick>` — saída anunciada, enviada antes de fechar a conexão.

O campo de texto livre é sempre o último do split (`Split('|', N)`), então pode
conter `|` sem quebrar o parsing. Apelidos não podem conter `|`.

## Formação da malha e link duplicado

`--peers` só lista `host:porta` — o apelido do outro lado só é conhecido depois
do handshake `HELLO`. Por isso a deduplicação não pode acontecer *antes* de
discar: cada nó tenta conectar a todos os seus `--peers` configurados
normalmente, e é comum que A e B disquem um para o outro ao mesmo tempo,
resultando em duas conexões TCP entre o mesmo par.

Regra aplicada depois do `HELLO`, sem nenhuma coordenação extra entre os nós:
cada conexão tem um "iniciador" (quem chamou `ConnectAsync` — o próprio nó, se
foi conexão de saída, ou o peer, se foi aceita). Ao registrar uma conexão cujo
par (`Nick`) já tem uma conexão ativa, mantém-se aquela cujo iniciador tem o
nick lexicograficamente menor e fecha-se a outra. Como os dois nós enxergam os
mesmos dois nicks candidatos a iniciador, os dois chegam à mesma decisão de
forma independente.

Se um peer configurado ainda não está de pé na primeira tentativa, o nó
reinsiste a cada 5s até conseguir conectar uma vez; depois disso não há
reconexão automática — se o link cair depois, é tratado como queda normal de
par (ver abaixo).

## Resiliência a queda de peer

Cada conexão (`PeerConnection`) roda sua própria task de leitura e sua própria
task de escrita, isoladas por `try/catch`: uma falha em uma conexão (peer caiu,
RST, timeout) nunca propaga nem afeta as demais. Ao detectar EOF, exceção,
timeout de envio, ou receber `BYE`, o peer é removido da tabela local e o nó
imprime `[apelido saiu da conversa]` — cada nó percebe a queda pela sua própria
conexão, não existe lista central nem propagação da notícia por terceiros.

## Backpressure / peer lento (política adotada)

Cada `PeerConnection` tem uma fila de saída limitada (capacidade 100,
`System.Threading.Channels.Channel` com `BoundedChannelOptions`). Se a fila
encher, a mensagem mais antiga é descartada para abrir espaço para a nova
(`BoundedChannelFullMode.DropOldest`). Se o envio ao socket falhar ou exceder o
timeout de escrita (5s), o peer é considerado morto e desconectado, exatamente
como uma queda normal.

Justificativa: descartar mensagens antigas é preferível a travar o broadcast
inteiro (o que afetaria todos os outros pares por causa de um único lento) ou a
derrubar a conexão só por uma lentidão pontual; a desconexão fica reservada
para quando a lentidão vira falha real (timeout de escrita).

## Timeouts

- Conexão de saída (`Connection.ConnectAsync`): 10s.
- Envio por peer (`Frames.WriteAsync` dentro do `PeerConnection`): 5s, via
  `CancellationTokenSource` vinculado ao token geral da aplicação.
- Leitura: sem timeout — aguardar a próxima mensagem é o comportamento esperado
  de um chat; fica vinculada apenas ao `CancellationToken` geral, que é
  cancelado no `/quit` ou no Ctrl+C, encerrando tudo de forma limpa.

## Arquivos

| Arquivo | Responsabilidade |
|---|---|
| `Frames.cs` | Framing por prefixo de tamanho de 4 bytes (reaproveitado, sem alterações). |
| `Connection.cs` | Criação do listener e `AcceptAsync`/`ConnectAsync` (reaproveitado). |
| `Protocol.cs` | Encode/decode das linhas `HELLO`/`MSG`/`PRIVMSG`/`BYE`. |
| `PeerConnection.cs` | Uma conexão: fila de saída com backpressure, task de escrita, task de leitura, timeouts, detecção de queda. |
| `Node.cs` | Tabela de peers, listener em loop, conexão com retry aos peers configurados, handshake + dedup, comandos do console, broadcast. |
| `Program.cs` | Parse de argumentos de linha de comando e start do nó. |
