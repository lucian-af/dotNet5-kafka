using System.Threading.Tasks;
using Confluent.Kafka;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;

namespace ProducerKafka
{
    class Producer
    {
        static async Task Main(string[] args)
        {
            var logger = new LoggerConfiguration()
                .WriteTo.Console(theme: AnsiConsoleTheme.Literate)
                .CreateLogger();

            logger.Information("Testando o envio de mensagens com Kafka...");

            if (args.Length < 3)
            {
                logger.Error(
                    "Informe ao menos 3 parâmetros: \n " +
                    "no primeiro o IP/Porta para testes com o Kafka, \n " +
                    "no segundo o Topic que receberá a mensagem, \n " +
                    "já no terceiro em diante as mensagens a serem enviadas a um Topic no Kafka...");
                return;
            }

            string bootstrapServers = args[0]; // poderia passar o IP do Kafka via uma config
            string nameTopic = args[1]; // criar uma lógica para um nome de tópico;

            logger.Information($"BootstrapServers = {bootstrapServers}");
            logger.Information($"Topic = {nameTopic}");

            try
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = bootstrapServers,

                };

                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    for (int i = 2; i < args.Length; i++)
                    {
                        var result = await producer.ProduceAsync(nameTopic, new Message<Null, string>() { Value = args[i] });

                        logger.Information(@$"Mensagem: {args[i]} | Status: {result.Status}");
                    }
                }
                logger.Information("Concluído o envio de mensaegens.");
            }
            catch (System.Exception ex)
            {
                logger.Error($"Exceção: {ex.GetType().FullName} | Mensagem: {ex.Message}");
            }
        }
    }
}
