namespace RabbitMqNext
{
	public class AmqpQueueInfo
	{
		public string Name { get; set; }
		public uint Messages { get; set; }
		public uint Consumers { get; set; }

		public override string ToString()
		{
			return "Queue: " + Name + "  Messages: " + Messages + "  Consumers: " + Consumers;
		}
	}
}