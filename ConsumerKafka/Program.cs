using System;
using System.Reflection;
using ConsumerKafka.Parametros;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace ConsumerKafka
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Testando o consumo de mensagens com Apache Kafka...");

            if (args.Length <= 0)
            {
                Console.WriteLine("Informe o Topic que contém as mensagens no Kafka.");
                return;
            }

            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddSingleton(new ExecutionParameter(
                        bootstrapServers: hostContext.Configuration.GetSection("API.Kafka").Value.ToString(),
                        topic: args[0],
                        groupId: Assembly.GetEntryAssembly().GetName().Name)
                    );

                    services.AddHostedService<Worker>();
                });
    }
}
