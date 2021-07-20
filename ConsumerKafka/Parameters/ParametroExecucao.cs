namespace ConsumerKafka.Parametros
{
    public class ExecutionParameter
    {
        public ExecutionParameter(string bootstrapServers, string topic, string groupId)
        {
            BootstrapServers = bootstrapServers;
            Topic = topic;
            GroupId = groupId;
        }

        public string BootstrapServers { get; }
        public string Topic { get; }
        public string GroupId { get; }
    }
}
