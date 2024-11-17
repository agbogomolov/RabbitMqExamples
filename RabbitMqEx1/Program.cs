using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Threading.Channels;

namespace RabbitMqEx1
{
    internal class Program
    {
        const string queueKey = "helloEx";
        static async Task Main(string[] args)
        {
            var cts = new CancellationTokenSource();
            var token = cts.Token;
            var factory = new ConnectionFactory { HostName = "localhost" };
            using var connection = await factory.CreateConnectionAsync();
            using var channel = await connection.CreateChannelAsync();
            await channel.QueueDeclareAsync(queue: queueKey, durable: false, exclusive: false, autoDelete: false,
                    arguments: null);

            Task.Run(() => Producer(channel, token), token);
            Task.Run(() => Consumer(channel, token), token);

            Console.ReadKey();
            cts.Cancel();
        }

        private static async Task Producer(IChannel channel, CancellationToken cancellationToken)
        {
            var messageId = 0;
            do
            {
                var message = $"Message {messageId++}";
                var body = Encoding.UTF8.GetBytes(message);

                await channel.BasicPublishAsync(exchange: string.Empty, routingKey: queueKey, body: body, cancellationToken: cancellationToken);
                Console.WriteLine($" [x] Sent {message}");

                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken: cancellationToken);
            } while (!cancellationToken.IsCancellationRequested);
        }

        private static async Task Consumer(IChannel channel, CancellationToken cancellationToken)
        {
            Console.WriteLine(" [*] Waiting for messages.");
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine($" [x] Received {message}");
                return Task.CompletedTask;
            };

            do
            {
                await channel.BasicConsumeAsync(queueKey, autoAck: true, consumer: consumer, cancellationToken: cancellationToken);
            } while (!cancellationToken.IsCancellationRequested);
        }
    }
}
