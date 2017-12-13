﻿namespace RecoveryScenariosApp
{
	using System;
	using System.Configuration;
	using System.IO;
	using System.Text;
	using System.Threading;
	using System.Threading.Tasks;
	using Microsoft.Extensions.Configuration;
	using RabbitMqNext;
	using RabbitMqNext.Recovery;

	class Program
	{
		private static string _host, _vhost, _username, _password;

		static void Main2(string[] args)
		{
			// Scenarios to test:
			// - idle connection 
			// - pending writing commands
			// - after declares
			// - after bindings
			// - after acks/nacks
			// - consumers
			// - rpc

			// 
			// correct behavior
			// 
			// * upon abrupt disconnection
			// 1 - writeloop and readloop threads area cancelled
			// 2 - reset pending commands
			//     reset ring buffer (what about pending reads?)
			// 3 - recovery kicks in
			// 
			// * upon reconnection
			// 1 - socketholder is kept (so does ring buffer)
			// 2 - recover entities

			LogAdapter.ExtendedLogEnabled = true;
			//			LogAdapter.ProtocolLevelLogEnabled = true;

			var configuration = new ConfigurationBuilder()
										   .SetBasePath(Directory.GetCurrentDirectory())
										   .AddJsonFile($"settings.json", optional: false, reloadOnChange: true)
										   .Build();

			var rabbitSettings = configuration.GetSection("RabbitMQSettings");

			_host = rabbitSettings["rabbitmqserver"];
			_vhost = rabbitSettings["vhost"];
			_username = rabbitSettings["username"];
			_password = rabbitSettings["password"];

			var t = new Task<bool>(() => Start().Result, TaskCreationOptions.LongRunning);
			t.Start();
			t.Wait();
		}

		private static Timer _timer;

		private static byte[] Buffer1 = Encoding.ASCII.GetBytes("Some message");
		private static byte[] Buffer2 = Encoding.ASCII.GetBytes("Some kind of other message");

		private static async Task<bool> Start()
		{
			var replier = await StartRpcReplyThread();


			var conn = // (RecoveryEnabledConnection)
				await ConnectionFactory
					.Connect(_host, _vhost,
							 _username, _password, 
							 recoverySettings: AutoRecoverySettings.All, 
							 connectionName: "main", heartbeat: 60);

//			conn.RecoveryCompleted += () =>
//			{
//				Console.WriteLine("Recovery completed!!");
//			};

			var channel1 = await conn.CreateChannel();
			await channel1.ExchangeDeclare("exchange_rec_1", "direct", false, autoDelete: true, arguments: null, waitConfirmation: true);
//			await channel1.QueueDeclare("qrpc1", false, durable: true, exclusive: false, autoDelete: false, arguments: null, waitConfirmation: true);
			await channel1.QueueDeclare("queue_rec_1", false, durable: true, exclusive: false, autoDelete: true, arguments: null, waitConfirmation: true);
			await channel1.QueueBind("queue_rec_1", "exchange_rec_1", "routing1", null, waitConfirmation: true);
			var rpcHelper = await channel1.CreateRpcHelper(ConsumeMode.ParallelWithBufferCopy, 1000);

			var channel2 = await conn.CreateChannel();
			await channel2.ExchangeDeclare("exchange_rec_2", "direct", false, autoDelete: true, arguments: null, waitConfirmation: true);
			await channel2.QueueDeclare("queue_rec_2", false, durable: true, exclusive: false, autoDelete: true, arguments: null, waitConfirmation: true);
			await channel2.QueueBind("queue_rec_2", "exchange_rec_2", "routing2", null, waitConfirmation: true);

			int counter = 0;

			_timer = new Timer(async state =>
			{
				Console.WriteLine("Sending message ..");

				try
				{
					var reply = await rpcHelper.Call("", "qrpc1", BasicProperties.Empty, BitConverter.GetBytes(counter++));
					Console.WriteLine("rpc reply received. size " + reply.bodySize);
					await channel1.BasicPublish("exchange_rec_1", "routing1", false, BasicProperties.Empty, Buffer1);
					await channel2.BasicPublish("exchange_rec_2", "routing2", false, BasicProperties.Empty, Buffer2);
				}
				catch (Exception ex)
				{
					Console.Error.WriteLine("Error sending message " + ex.Message);
				}

			}, null, TimeSpan.FromSeconds(4), TimeSpan.FromSeconds(1));


			var channel3 = await conn.CreateChannel();
			await channel3.BasicConsume(ConsumeMode.ParallelWithBufferCopy, new Consumer("1"), "queue_rec_1", "", true, false, null, true);
			await channel3.BasicConsume(ConsumeMode.ParallelWithBufferCopy, new Consumer("2"), "queue_rec_2", "", true, false, null, true);



			Console.WriteLine("Do stop rabbitmq service");



			await Task.Delay(TimeSpan.FromMinutes(10));



			_timer.Dispose();

			Console.WriteLine("Done. Disposing...");

			conn.Dispose();

			replier.Dispose();

			return true;
		}

		private static async Task<IDisposable> StartRpcReplyThread()
		{
			var conn = // (RecoveryEnabledConnection)
				await ConnectionFactory
					.Connect(_host, _vhost,
							 _username, _password, 
							 recoverySettings: AutoRecoverySettings.All, 
							 connectionName: "replier");

			var channel = await conn.CreateChannel();

			await channel.QueueDeclare("qrpc1", false, true, false, false, null, waitConfirmation: true);

			await channel.BasicConsume(ConsumeMode.ParallelWithBufferCopy, (delivery) =>
			{
				var prop = channel.RentBasicProperties();

				prop.CorrelationId = delivery.properties.CorrelationId;
				var buffer = new byte[4];
				delivery.stream.Read(buffer, 0, 4);

				Console.WriteLine("Got rpc request " + BitConverter.ToInt32(buffer, 0) + ". Sending rpc reply");

				channel.BasicPublishFast("", delivery.properties.ReplyTo, false, prop, Encoding.UTF8.GetBytes("Reply"));

				return Task.CompletedTask;

			}, "qrpc1", "", true, false, null, true);

			return (IDisposable) conn;
		}
	}
}
