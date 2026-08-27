using System.Text;

//Manual de Tradução das mensagens

namespace SocketChat
{
    //Tipos de mensagens possíveis
    public enum MessageType { Hello, Msg, PrivMsg, Bye }

    //Estrutura de dados que guarda as informações sobre uma mensagem já decodificada.
    public sealed class ChatMessage
    {
        public required MessageType Type { get; init; }
        public string Nick { get; init; } = "";
        public string Text { get; init; } = "";
        public string ToNick { get; init; } = "";
        public int Port { get; init; }
    }

    /*
        Protocolo de texto trivial: uma linha por mensagem, campos separados por '|'.
    
        O texto livre (mensagem/PV) vai sempre no último campo, sem limite de split, então pode conter '|' sem quebrar o parsing. Apelidos não podem conter '|'.

        Encode -> transforma a mensagem em bytes para enviar.
    */
    public static class Protocol
    {
        public static byte[] EncodeHello(string nick, int port) =>
            Encode($"HELLO|{nick}|{port}");

        public static byte[] EncodeMsg(string nick, string text) =>
            Encode($"MSG|{nick}|{text}");

        public static byte[] EncodePrivMsg(string fromNick, string toNick, string text) =>
            Encode($"PRIVMSG|{fromNick}|{toNick}|{text}");

        public static byte[] EncodeBye(string nick) =>
            Encode($"BYE|{nick}");

        /*
            As quatro acima chamam a função privada abaixo que de fato tranforma em bytes no padrão UTF8.
        */
        private static byte[] Encode(string line) => Encoding.UTF8.GetBytes(line);


        /*
            Transforma os bytes de volta em texto.

            var line = Encoding.UTF8.GetString(frame) -> pega os bytes e transforma de volta.

            var head = line.IndexOf('|') -> acha a posição do primeiro | dentro do texto. Isso marca onde termina o rótulo de tipo e começa o resto da mensagem.

            var type = head < 0 ? line : line[..head]; -> Se não encontrou nenhum |, então type é a linha inteira. Senão, o type é antes de |. Serve de validação para mensagens malformadas.
        */
        public static ChatMessage Decode(byte[] frame)
        {
            var line = Encoding.UTF8.GetString(frame);
            var head = line.IndexOf('|');
            var type = head < 0 ? line : line[..head];

            switch (type)
            {
                case "HELLO":
                    {
                        var parts = line.Split('|', 3);
                        return new ChatMessage { Type = MessageType.Hello, Nick = parts[1], Port = int.Parse(parts[2]) };
                    }
                case "MSG":
                    {
                        var parts = line.Split('|', 3);
                        return new ChatMessage { Type = MessageType.Msg, Nick = parts[1], Text = parts[2] };
                    }
                case "PRIVMSG":
                    {
                        var parts = line.Split('|', 4);
                        return new ChatMessage { Type = MessageType.PrivMsg, Nick = parts[1], ToNick = parts[2], Text = parts[3] };
                    }
                case "BYE":
                    {
                        var parts = line.Split('|', 2);
                        return new ChatMessage { Type = MessageType.Bye, Nick = parts[1] };
                    }
                default:
                    throw new InvalidDataException($"Mensagem de protocolo desconhecida: '{line}'");
            }
        }
    }
}
