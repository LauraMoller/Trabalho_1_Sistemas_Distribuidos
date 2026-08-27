using SocketChat;
//Função da Classe: ler o input e usar isso para ligar o programa.
//Ex: --port 9001 --nick alice


/*
    Função que imprime o modo correto de uso do programa.
*/
static void PrintUsage() => Console.WriteLine("""
    Uso:
      dotnet run -- --port <porta> --nick <apelido> [--peers host:porta,host:porta,...]

    Exemplo (3 nós formando malha completa):
      dotnet run -- --port 9001 --nick alice --peers 127.0.0.1:9002,127.0.0.1:9003
      dotnet run -- --port 9002 --nick bob   --peers 127.0.0.1:9001,127.0.0.1:9003
      dotnet run -- --port 9003 --nick carol --peers 127.0.0.1:9001,127.0.0.1:9002
    """);



/*
    Retorna (Porta, nickname, lista de Peers conhecidos contendo nome e host) empacotado numa tupla.
    
    string[] args - array de texto com o seu input -> cada palavra vira um item.
*/
(int Port, string Nick, List<(string Host, int Port)> Peers) ParseArgs(string[] args)
{
    /*
        ? - valor anulável, nesse caso usado para os valores que ainda não se sabe, que não foram lidos.
    */
    int? port = null;
    string? nick = null;
    var peers = new List<(string, int)>();

    /*
        loop para percorrer todo o input de dados armazenado em args.
    */
    for (var i = 0; i < args.Length; i++)
    {
        switch (args[i])
        {
            /*
                when i+1 < args.Length -> condição extra que chama um filtro de case. Significa que só entrará no case se existir uma posição além da atual dentro da lista, evitando que o usuário escreva "--port" sem o número da porta.
            */
            case "--port" when i + 1 < args.Length:
                /*
                    i -> aponta para o texto "--port"
                    i++ -> número da porta.
                */
                port = int.Parse(args[++i]);
                break;
            /*
                mesma lógica da porta só que para o apelido.
            */
            case "--nick" when i + 1 < args.Length:
                nick = args[++i];
                break;
            /*
                A lista dos peers vem como um texto só (ex: 127.0.0.1:9002,127.0.0.1:9003) e precisa ser quebrado em pedaços utilizáveis.

                raw = recebe o texto por inteiro.

                split - quebra o texto a cada vírgula.

                RemoveEmptyEntries - remove pedaços vazios.

                TrimEntries - remove espaços brancos extras.
            */
            case "--peers" when i + 1 < args.Length:
                var raw = args[++i];
                foreach (var entry in raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                {
                    var parts = entry.Split(':');
                    if (parts.Length != 2 || !int.TryParse(parts[1], out var peerPort))
                        throw new ArgumentException($"peer inválido: '{entry}' (esperado host:porta)");
                    peers.Add((parts[0], peerPort));
                }
                break;
        }
    }


    /*
        Exceções: porta nula, sem apelido, caractere inválido no apelido.
    */
    if (port is null)
        throw new ArgumentException("--port é obrigatório.");
    if (string.IsNullOrWhiteSpace(nick))
        throw new ArgumentException("--nick é obrigatório.");
    if (nick.Contains('|'))
        throw new ArgumentException("--nick não pode conter o caractere '|'.");

    return (port.Value, nick, peers);
}


/*
    Input vazio
*/
if (args.Length == 0)
{
    PrintUsage();
    return 1;
}

/*
   Cria uma variável tupla vazia chamada config. 
*/
(int Port, string Nick, List<(string Host, int Port)> Peers) config;
try
{
    config = ParseArgs(args); //Função incial que retorna tupla
}
catch (Exception ex) //Pega os erros
{
    Console.WriteLine($"Erro: {ex.Message}");
    PrintUsage();
    return 1;
}

/*
    Cria um novo objeto da classe Node.cs.
*/
var node = new Node(config.Nick, config.Port, config.Peers);

/*
    Tratamento de encerramento, evitando o encerrar cru ctrl+c
*/
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    node.RequestShutdown();
};

//faz o chat funcionar.
await node.RunAsync();
return 0;
