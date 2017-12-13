namespace RabbitMqNext.Internals
{
	using System.Collections.Generic;
	using System.Text;

	public static class Protocol
	{
		public static readonly IDictionary<string, object> ClientProperties;

		static Protocol()
		{
			ClientProperties = new Dictionary<string, object>
			{
				{ "product", Encoding.UTF8.GetBytes("RabbitMQNext") },
				{ "version", Encoding.UTF8.GetBytes("0.0.0.1") },
				{ "platform", Encoding.UTF8.GetBytes(".net") },
				{ "copyright", Encoding.UTF8.GetBytes("Clear CTVM 2015,16") },
				{ "information", Encoding.UTF8.GetBytes("Licensed under LGPL") },
				{ "capabilities", new Dictionary<string, object>
				{
					{ "publisher_confirms", true },
					{ "exchange_exchange_bindings", true },
					{ "basic.nack", true },
					{ "consumer_cancel_notify", true },
					{ "connection.blocked", true },
					{ "authentication_failure_close", true }
				} }
			};
		}
	}
}