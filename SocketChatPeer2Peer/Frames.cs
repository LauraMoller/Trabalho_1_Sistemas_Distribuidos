using System.Buffers.Binary;
using System.Net.Sockets;

// Header com tamanho + mensagem = Frame

namespace SocketChat
{
    public static class Frames
    {
        public const int MaxFrameSize = 64 * 1024;


        /*
            Monta e envia um frame.

            ReadOnlyMemory<byte> payload -> o conteúdo da mensagem.

            if (payload.Length > MaxFrameSize) -> verifica se a mensagem não excede o tamanho limite.

            BinaryPrimitives.WriteInt32BigEndian(header, payload.Length) -> pega o tamanho da mensagem e escreve dentro dos 4 bytes de header.

            await SendAllAsync(socket, header, ct); -> manda o cabeçalho
            await SendAllAsync(socket, payload, ct); ->  manda o conteúdo
        */
        public static async Task WriteAsync(Socket socket, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
        {
            if (payload.Length > MaxFrameSize)
                throw new ArgumentException($"Payload of {payload.Length} bytes exceeds the limit.");

            var header = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(header, payload.Length);

            await SendAllAsync(socket, header, ct);
            await SendAllAsync(socket, payload, ct);
        }


        /*
            Task<byte[]?> -> ? retorno pode ser null (a conexão foi fechada antes mesmo de começar uma nova mensagem)

            Primeiro tenta ler os 4 bytes do cabeçalho(ReadExactlyAsync) -> retorna null se não.

            Decodifia os 4 bytes de volta e valida o tamanho da mensagem -> caso especial [] (mensagem vazia).

            cria um array do tamanho da mensagem e tenta ler a quantia exata de bytes.

        */
        public static async Task<byte[]?> ReadAsync(Socket socket, CancellationToken ct = default)
        {
            var header = new byte[4];
            if (!await ReadExactlyAsync(socket, header, ct))
                return null;

            var size = BinaryPrimitives.ReadInt32BigEndian(header);
            if (size < 0 || size > MaxFrameSize)
                throw new InvalidDataException($"Invalid frame size: {size}");

            if (size == 0)
                return [];

            var payload = new byte[size];
            if (!await ReadExactlyAsync(socket, payload, ct))
                throw new EndOfStreamException("Connection closed in the middle of a frame.");

            return payload;
        }


        /*
            Socket não garante que vai mandar a mensagem tudo de uma vez, então insisti até mandar tudo.
        */
        private static async Task SendAllAsync(Socket socket, ReadOnlyMemory<byte> data, CancellationToken ct)
        {
            var sent = 0;
            while (sent < data.Length)
            {
                var n = await socket.SendAsync(data[sent..], SocketFlags.None, ct);
                if (n == 0)
                    throw new SocketException((int)SocketError.ConnectionReset);
                sent += n;
            }
        }

        /*
            Socket não garante que vai mandar a mensagem tudo de uma vez, então insisti até ler tudo.
        */
        private static async Task<bool> ReadExactlyAsync(Socket socket, Memory<byte> destination, CancellationToken ct)
        {
            var read = 0;
            while (read < destination.Length)
            {
                var n = await socket.ReceiveAsync(destination[read..], SocketFlags.None, ct);
                if (n == 0)
                {
                    if (read == 0)
                        return false;

                    throw new EndOfStreamException(
                        $"Missing {destination.Length - read} bytes of the frame.");
                }
                read += n;
            }
            return true;
        }
    }
}
