namespace RabbitMqNext.MConsole
{
	using Newtonsoft.Json;

	public class UserInfo
	{
		[JsonProperty("name")]
		public string Name { get; set; }

		[JsonProperty("tags")]
		public string Tags { get; set; }
	}
}