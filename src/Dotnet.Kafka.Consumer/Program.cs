using Confluent.Kafka;
using System;

namespace Dotnet.Kafka.Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Informe o nome da fila:");
            var queue = Console.ReadLine();
            using var consumer = GetConsumer(queue);
            consumer.Subscribe(new string[] { queue });
            var result = consumer.Consume();
            if (result.IsPartitionEOF)
                Console.WriteLine($"Mensagem: {result.Value}");
        }

        private static IConsumer<Ignore, string> GetConsumer(string queue) => new ConsumerBuilder<Ignore, string>(new ConsumerConfig
        {
            BootstrapServers = "localhost:8080",
            GroupId = queue,
            EnableAutoCommit = false,
        })
            .SetErrorHandler((_, e) => Console.WriteLine($"Ocorreu um erro inesperado: {e.Reason}"))
            .Build();  
    }
}
