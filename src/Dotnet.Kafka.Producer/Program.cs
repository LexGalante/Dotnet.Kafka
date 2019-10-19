using System;
using Confluent.Kafka;

namespace Dotnet.Kafka.Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            //Constroi um producer
            using var producer = GetProducer();
            //Obtem dados da fila e mensagem que deseja enviar
            Console.WriteLine("Informe o nome da fila:");
            var queue = Console.ReadLine();
            Console.WriteLine("Informe a mensagem que deseja enviar:");
            var message = Console.ReadLine();
            //Envia mensagem
            producer.Produce(queue, new Message<Null, string>()
            {
                Value = message                
            });
            //Retorna ok
            Console.WriteLine($"Mensagem ({message}) enviada com sucesso!");
            Console.ReadKey();
            producer.Flush();
        }

        private static IProducer<Null, string> GetProducer() => new ProducerBuilder<Null, string>(new ProducerConfig()
        {
            BootstrapServers = "localhost:8080",
            EnableSslCertificateVerification = false,
            LogConnectionClose = true,
            LogQueue = true,
        }).Build();
    }
}
