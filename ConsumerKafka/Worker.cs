using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using ConsumerKafka.Parametros;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ConsumerKafka
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly ExecutionParameter _parameter;
        private readonly IConsumer<Ignore, string> _consumer;

        public Worker(ILogger<Worker> logger, ExecutionParameter parameter)
        {
            _logger = logger;
            _parameter = parameter;

            _logger.LogInformation($"Topic: {parameter.Topic}");
            _logger.LogInformation($"Group Id: {parameter.GroupId}");

            _consumer = new ConsumerBuilder<Ignore, string>(new ConsumerConfig()
            {
                BootstrapServers = parameter.BootstrapServers,
                GroupId = parameter.GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest // default FIFO (First In First Out)                
            }).Build();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Aguardando mensagens...");
            _consumer.Subscribe(_parameter.Topic);

            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Run(() =>
                {
                    var result = _consumer.Consume(stoppingToken);
                    _logger.LogInformation($"{_parameter.GroupId} | Nova Mensagem: {result.Message.Value}");

                });
            }
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            _consumer.Close();
            _logger.LogInformation("Conexão com Apache Kafka fechada.");

            return Task.CompletedTask;
        }
    }
}
