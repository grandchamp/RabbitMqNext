namespace RabbitMqNext.Io
{
	using System;
	using System.Collections.Generic;
	using System.Threading.Tasks;
	using RabbitMqNext;
	using RabbitMqNext.Internals;


	public sealed class ChannelIO : AmqpIOBase
	{
		private const string LogSource = "ChannelIO";

		private readonly Channel _channel;
		public readonly ConnectionIO _connectionIo;

		private readonly ObjectPoolArray<FrameParameters.BasicPublishArgs> _basicPubArgsPool;

		public ChannelIO(Channel channel, ushort channelNumber, ConnectionIO connectionIo)
			: base(channelNumber)
		{
			_channel = channel;
			_connectionIo = connectionIo;

//			_taskLightPool = new ObjectPool<TaskSlim>(
//				() => new TaskSlim(i => _channel.GenericRecycler(i, _taskLightPool)), 10, preInitialize: true);

			_basicPubArgsPool = new ObjectPoolArray<FrameParameters.BasicPublishArgs>(
				() => new FrameParameters.BasicPublishArgs(i => _channel.GenericRecycler(i, _basicPubArgsPool)), 1000, preInitialize: true); 
		}

		public ushort ChannelNumber { get { return _channelNum; } }

		#region AmqpIOBase overrides

		public override async Task HandleFrame(int classMethodId)
		{
			switch (classMethodId)
			{
				case AmqpClassMethodChannelLevelConstants.ChannelClose:
					_connectionIo._frameReader.Read_Channel_Close2(base.HandleCloseMethodFromServer);
					break;

				case AmqpClassMethodChannelLevelConstants.BasicDeliver:
					_connectionIo._frameReader.Read_BasicDelivery(_channel, _channel.RentBasicProperties());
					break;

				case AmqpClassMethodChannelLevelConstants.BasicReturn:
					_connectionIo._frameReader.Read_BasicReturn(_channel, _channel.RentBasicProperties());
					break;

				// Basic Ack and NAck will be sent by the server if we enabled confirmation for this channel
				case AmqpClassMethodChannelLevelConstants.BasicAck:
					_connectionIo._frameReader.Read_BasicAck(_channel);
					break;

				case AmqpClassMethodChannelLevelConstants.BasicNAck:
					_connectionIo._frameReader.Read_BasicNAck(_channel);
					break;

				case AmqpClassMethodChannelLevelConstants.ChannelFlow:
					_connectionIo._frameReader.Read_ChannelFlow(_channel.HandleChannelFlow);
					break;

				case AmqpClassMethodChannelLevelConstants.BasicCancel:
					_connectionIo._frameReader.Read_BasicCancel(_channel.HandleCancelConsumerByServer);
					break;

				default:
					base.HandReplyToAwaitingQueue(classMethodId);
					break;
			}
		}

		public override Task SendCloseConfirmation()
		{
			return __SendChannelCloseOk();
		}

		public override Task SendStartClose()
		{
			return __SendChannelClose(AmqpConstants.ReplySuccess, "bye");
		}

		protected override void InternalDispose()
		{
		}

		protected override void DrainPending(AmqpError error)
		{
			base.DrainPending(error);

			this._channel.DrainPendingIfNeeded(error);
		}

		#endregion

		public Task Open()
		{
			var tcs = new TaskCompletionSource<bool>();

			var writer = AmqpChannelLevelFrameWriter.ChannelOpen();

			_connectionIo.SendCommand(_channelNum, 20, 10, 
				writer,
				reply: (channel, classMethodId, error) =>
				{
					if (classMethodId == AmqpClassMethodChannelLevelConstants.ChannelOpenOk)
					{
						_connectionIo._frameReader.Read_ChannelOpenOk((reserved) =>
						{
							tcs.SetResult(true);
						});
					}
					else
					{
						AmqpIOBase.SetException(tcs, error, classMethodId);
					}
				}, expectsReply: true);

			return tcs.Task;
		}

		#region Commands writing methods

		public Task __SendChannelClose(ushort replyCode, string message)
		{
			var tcs = new TaskCompletionSource<bool>();

			_connectionIo.SendCommand(_channelNum, 20, 40, 
				AmqpChannelLevelFrameWriter.ChannelClose,
				reply: (channel, classMethodId, error) =>
				{
					if (classMethodId == AmqpClassMethodChannelLevelConstants.ChannelCloseOk)
					{
						tcs.SetResult(true);
					}
					else
					{
						AmqpIOBase.SetException(tcs, error, classMethodId);
					}
				},
				expectsReply: true,
				optArg: new FrameParameters.CloseParams()
				{
					replyCode = replyCode,
					replyText = message
				}, 
				immediately: true);

			return tcs.Task;
		}

		public Task __SendChannelCloseOk()
		{
			var tcs = new TaskCompletionSource<bool>();

			_connectionIo.SendCommand(_channelNum, 20, 41, 
				AmqpChannelLevelFrameWriter.ChannelCloseOk, 
				reply: null, expectsReply: false, tcs: tcs, 
				immediately: true);

			return tcs.Task;
		}

		public void __SendChannelFlowOk(bool isActive)
		{
			_connectionIo.SendCommand(_channelNum, 20, 21,
				AmqpChannelLevelFrameWriter.ChannelFlowOk(isActive),
				reply: null, expectsReply: false, tcs: null,
				immediately: true);
		}

		public Task __SendConfirmSelect(bool noWait)
		{
			var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

			_connectionIo.SendCommand(_channelNum, 85, 10,
				AmqpChannelLevelFrameWriter.ConfirmSelect(noWait),
				reply: (channel, classMethodId, error) =>
				{
					if (classMethodId == AmqpClassMethodChannelLevelConstants.ConfirmSelectOk)
					{
						tcs.SetResult(true);
					}
					else
					{
						AmqpIOBase.SetException(tcs, error, classMethodId);
					}
				},
				expectsReply: true,
				tcs: tcs);

			return tcs.Task;
		}

		public Task __BasicQos(uint prefetchSize, ushort prefetchCount, bool global)
		{
			var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

			var writer = AmqpChannelLevelFrameWriter.BasicQos(prefetchSize, prefetchCount, global);

			_connectionIo.SendCommand(_channelNum, 60, 10, 
				writer,
				reply: (channel, classMethodId, error) =>
				{
					if (classMethodId == AmqpClassMethodChannelLevelConstants.BasicQosOk)
					{
						tcs.SetResult(true);
					}
					else
					{
						AmqpIOBase.SetException(tcs, error, classMethodId);
					}
				}, expectsReply: true);

			return tcs.Task;
		}

		public void __BasicAck(ulong deliveryTag, bool multiple)
		{
			var args = new FrameParameters.BasicAckArgs { deliveryTag = deliveryTag, multiple = multiple };

			_connectionIo.SendCommand(_channelNum, 60, 80,
				null, // writer
				reply: null,
				expectsReply: false,
				optArg: args);
		}

		public void __BasicNAck(ulong deliveryTag, bool multiple, bool requeue)
		{
			var args = new FrameParameters.BasicNAckArgs() { deliveryTag = deliveryTag, multiple = multiple, requeue = requeue };

			_connectionIo.SendCommand(_channelNum, 60, 120,
				null, // writer
				reply: null,
				expectsReply: false,
				optArg: args);
		}

		public Task __ExchangeDeclare(string exchange, string type, bool durable, bool autoDelete,
			IDictionary<string, object> arguments, bool waitConfirmation)
		{
			var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

			var writer = AmqpChannelLevelFrameWriter.ExchangeDeclare(exchange, type, durable, autoDelete,
				arguments, false, false, waitConfirmation);

			_connectionIo.SendCommand(_channelNum, 40, 10, 
				writer,
				reply: (channel, classMethodId, error) =>
				{
					if (!waitConfirmation || classMethodId == AmqpClassMethodChannelLevelConstants.ExchangeDeclareOk)
					{
						if (LogAdapter.ProtocolLevelLogEnabled)
							LogAdapter.LogDebug(LogSource, "< ExchangeDeclareOk " + exchange);

						tcs.SetResult(true);
					}
					else
					{
						AmqpIOBase.SetException(tcs, error, classMethodId);
					}
				}, 
				expectsReply: waitConfirmation);

			return tcs.Task;
		}

		public Task<AmqpQueueInfo> __QueueDeclare(string queue, bool passive, bool durable, bool exclusive, bool autoDelete,
			IDictionary<string, object> arguments, bool waitConfirmation)
		{
			var tcs = new TaskCompletionSource<AmqpQueueInfo>(TaskCreationOptions.RunContinuationsAsynchronously);

			var writer = AmqpChannelLevelFrameWriter.QueueDeclare(queue, passive, durable,
				exclusive, autoDelete, arguments, waitConfirmation);

			_connectionIo.SendCommand(_channelNum, 50, 10, 
				writer,
				reply: (channel, classMethodId, error) =>
				{
					if (waitConfirmation && classMethodId == AmqpClassMethodChannelLevelConstants.QueueDeclareOk)
					{
						_connectionIo._frameReader.Read_QueueDeclareOk((queueName, messageCount, consumerCount) =>
						{
						    tcs.SetResult(new AmqpQueueInfo()
						    {
						        Name = queueName,
						        Consumers = consumerCount,
						        Messages = messageCount
						    });
						});
					}
					else if (!waitConfirmation)
					{
						tcs.SetResult(new AmqpQueueInfo { Name = queue });
					}
					else
					{
						AmqpIOBase.SetException(tcs, error, classMethodId);
					}
				}, expectsReply: waitConfirmation);

			return tcs.Task;
		}

		public Task __QueueBind(string queue, string exchange, string routingKey, IDictionary<string, object> arguments, bool waitConfirmation)
		{
			var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

			var writer = AmqpChannelLevelFrameWriter.QueueBind(queue, exchange, routingKey, arguments, waitConfirmation);

			_connectionIo.SendCommand(_channelNum, 50, 20, 
				writer,
				reply: (channel, classMethodId, error) =>
				{
					if (!waitConfirmation || (classMethodId == AmqpClassMethodChannelLevelConstants.QueueBindOk))
					{
						tcs.SetResult(true);
					}
					else
					{
						AmqpIOBase.SetException(tcs, error, classMethodId);
					}
				}, expectsReply: waitConfirmation);

			return tcs.Task;
		}

		public Task<string> __BasicConsume(ConsumeMode mode, string queue, string consumerTag, bool withoutAcks, 
											 bool exclusive, IDictionary<string, object> arguments,
											 bool waitConfirmation, Action<string> confirmConsumerTag)
		{
			var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

			var writer = AmqpChannelLevelFrameWriter.BasicConsume(
				queue, consumerTag, withoutAcks, exclusive, arguments, waitConfirmation);

			_connectionIo.SendCommand(_channelNum, 60, 20, 
				writer,
				reply: (channel, classMethodId, error) =>
				{
					if (LogAdapter.ProtocolLevelLogEnabled)
						LogAdapter.LogDebug(LogSource, "< BasicConsumeOk for queue " + queue);

					if (waitConfirmation && classMethodId == AmqpClassMethodChannelLevelConstants.BasicConsumeOk)
					{
						_connectionIo._frameReader.Read_BasicConsumeOk((consumerTag2) =>
						{
							if (string.IsNullOrEmpty(consumerTag))
							{
								if (LogAdapter.ProtocolLevelLogEnabled)
									LogAdapter.LogDebug(LogSource, "< BasicConsumeOk consumerTag " + consumerTag);

								confirmConsumerTag(consumerTag2);
							}

							tcs.SetResult(consumerTag2);
						});
					}
					else if (!waitConfirmation)
					{
						tcs.SetResult(consumerTag);
					}
					else
					{
						AmqpIOBase.SetException(tcs, error, classMethodId);
					}

				}, expectsReply: waitConfirmation);

			return tcs.Task;
		}

		public Task __BasicPublishTask(string exchange, string routingKey, bool mandatory,
			BasicProperties properties, ArraySegment<byte> buffer)
		{
			if (properties == null)
				properties = BasicProperties.Empty;

			var tcs = new TaskCompletionSource<bool>();
			var args = _basicPubArgsPool.GetObject();
			args.exchange = exchange;
			args.routingKey = routingKey;
			args.mandatory = mandatory;
			args.properties = properties;
			args.buffer = buffer;

			_connectionIo.SendCommand(_channelNum, 60, 40,
				null, // AmqpChannelLevelFrameWriter.publicBasicPublish, 
				reply: (channel, classMethodId, error) =>
				{
					if (properties.IsReusable)
					{
						_channel.Return(properties); // the tcs is left for the confirmation keeper
					}

					if (error == null)
						tcs.TrySetResult(true);
					else
						AmqpIOBase.SetException(tcs, error, classMethodId);
				},
				expectsReply: false,
//				tcsL: null,
				optArg: args);

			return tcs.Task;
		}

		public Task __BasicPublishConfirm(string exchange, string routingKey, bool mandatory, 
											BasicProperties properties, ArraySegment<byte> buffer)
		{
			if (properties == null)
			{
				properties = BasicProperties.Empty;
			}

			var confirmationKeeper = _channel._confirmationKeeper;

			var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
			confirmationKeeper.WaitForSemaphore(); // make sure we're not over the limit
			
			var args = _basicPubArgsPool.GetObject();
			args.exchange = exchange;
			args.routingKey = routingKey;
			args.mandatory = mandatory;
			args.properties = properties;
			args.buffer = buffer;

			_connectionIo.SendCommand(_channelNum, 60, 40,
				null, // AmqpChannelLevelFrameWriter.publicBasicPublish, 
				reply: (channel, classMethodId, error) =>
				{
					if (properties.IsReusable)
					{
						_channel.Return(properties); // the tcs is left for the confirmation keeper
					}

					if (error != null)
						AmqpIOBase.SetException(tcs, error, classMethodId);
				},
				expectsReply: false,
//				tcsL: null, 
				optArg: args,
				prepare: () => _channel._confirmationKeeper.Add(tcs));

			return tcs.Task;
		}

		public void __BasicPublish(string exchange, string routingKey, bool mandatory, 
									 BasicProperties properties, ArraySegment<byte> buffer)
		{
			if (properties == null) properties = BasicProperties.Empty;

			var args = _basicPubArgsPool.GetObject();
			args.exchange = exchange;
			args.routingKey = routingKey;
			args.mandatory = mandatory;
			args.properties = properties;
			args.buffer = buffer;

			_connectionIo.SendCommand(_channelNum, 60, 40,
				null, // writer
				reply: (channel, classMethodId, error) =>
				{
					if (properties.IsReusable)
					{
						_channel.Return(properties);
					}
				},
				expectsReply: false,
				optArg: args);
		}

		public Task __BasicCancel(string consumerTag, bool waitConfirmation)
		{
			var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

			_connectionIo.SendCommand(_channelNum, 60, 30,
				AmqpChannelLevelFrameWriter.BasicCancel(consumerTag, waitConfirmation),
				reply: (channel, classMethodId, error) =>
				{
					if (waitConfirmation && classMethodId == AmqpClassMethodChannelLevelConstants.CancelOk)
					{
						_connectionIo._frameReader.Read_CancelOk(_ =>
						{
							tcs.SetResult(true);
						});
					}
					else if (!waitConfirmation)
					{
						tcs.SetResult(true);
					}
					else
					{
						AmqpIOBase.SetException(tcs, error, classMethodId);
					}
				}, 
				expectsReply: waitConfirmation);

			return tcs.Task;
		}

		public Task __BasicRecover(bool requeue)
		{
			var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

			_connectionIo.SendCommand(_channelNum, 60, 110,
				AmqpChannelLevelFrameWriter.Recover(requeue),
				reply: (channel, classMethodId, error) =>
				{
					if (classMethodId == AmqpClassMethodChannelLevelConstants.RecoverOk)
					{
						tcs.SetResult(true);
					}
					else
					{
						AmqpIOBase.SetException(tcs, error, classMethodId);
					}
				},
				expectsReply: true,
				tcs: tcs);

			return tcs.Task;
		}

		public Task __ExchangeBind(string source, string destination, string routingKey, 
									 IDictionary<string, object> arguments, bool waitConfirmation)
		{
			var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

			var writer = AmqpChannelLevelFrameWriter.ExchangeBind(source, destination, routingKey, arguments, waitConfirmation);

			_connectionIo.SendCommand(_channelNum, 
				Amqp.Channel.Exchange.ClassId, Amqp.Channel.Exchange.Methods.ExchangeBind,
				writer,
				reply: (channel, classMethodId, error) =>
				{
					if (!waitConfirmation || classMethodId == AmqpClassMethodChannelLevelConstants.ExchangeBindOk)
					{
						if (LogAdapter.ProtocolLevelLogEnabled)
							LogAdapter.LogDebug(LogSource, "< ExchangeBindOk " + source);

						tcs.SetResult(true);
					}
					else
					{
						AmqpIOBase.SetException(tcs, error, classMethodId);
					}
				},
				expectsReply: waitConfirmation);

			return tcs.Task;
		}

		public Task __ExchangeUnbind(string source, string destination, string routingKey, IDictionary<string, object> arguments, bool waitConfirmation)
		{
			var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

			var writer = AmqpChannelLevelFrameWriter.ExchangeUnbind(source, destination, routingKey, arguments, waitConfirmation);

			_connectionIo.SendCommand(_channelNum,
				Amqp.Channel.Exchange.ClassId, Amqp.Channel.Exchange.Methods.ExchangeUnBind,
				writer,
				reply: (channel, classMethodId, error) =>
				{
					if (!waitConfirmation || classMethodId == AmqpClassMethodChannelLevelConstants.ExchangeUnbindOk)
					{
						if (LogAdapter.ProtocolLevelLogEnabled)
							LogAdapter.LogDebug(LogSource, "< ExchangeUnbindOk " + source);

						tcs.SetResult(true);
					}
					else
					{
						AmqpIOBase.SetException(tcs, error, classMethodId);
					}
				},
				expectsReply: waitConfirmation);

			return tcs.Task;
		}

		public Task __ExchangeDelete(string exchange, bool waitConfirmation)
		{
			var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

			var writer = AmqpChannelLevelFrameWriter.ExchangeDelete(exchange, waitConfirmation);

			_connectionIo.SendCommand(_channelNum, Amqp.Channel.Exchange.ClassId, Amqp.Channel.Exchange.Methods.ExchangeDelete,
				writer,
				reply: (channel, classMethodId, error) =>
				{
					if (!waitConfirmation || classMethodId == AmqpClassMethodChannelLevelConstants.ExchangeDeleteOk)
					{
						if (LogAdapter.ProtocolLevelLogEnabled)
							LogAdapter.LogDebug(LogSource, "< ExchangeDeleteOk " + exchange);

						tcs.SetResult(true);
					}
					else
					{
						AmqpIOBase.SetException(tcs, error, classMethodId);
					}
				},
				expectsReply: waitConfirmation);

			return tcs.Task;
		}

		public Task __QueueUnbind(string queue, string exchange, string routingKey, 
									IDictionary<string, object> arguments)
		{
			var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

			var writer = AmqpChannelLevelFrameWriter.QueueUnbind(queue, exchange, routingKey, arguments);

			_connectionIo.SendCommand(_channelNum,
				Amqp.Channel.Queue.ClassId, Amqp.Channel.Queue.Methods.QueueUnbind,
				writer,
				reply: (channel, classMethodId, error) =>
				{
					if (classMethodId == AmqpClassMethodChannelLevelConstants.QueueUnbindOk)
					{
						if (LogAdapter.ProtocolLevelLogEnabled)
							LogAdapter.LogDebug(LogSource, "< QueueUnbindOk " + queue);

						tcs.SetResult(true);
					}
					else
					{
						AmqpIOBase.SetException(tcs, error, classMethodId);
					}
				},
				expectsReply: true);

			return tcs.Task;
		}

		public Task<uint> __QueueDelete(string queue, bool waitConfirmation)
		{
			var tcs = new TaskCompletionSource<uint>(TaskCreationOptions.RunContinuationsAsynchronously);

			var writer = AmqpChannelLevelFrameWriter.QueueDelete(queue, waitConfirmation);

			_connectionIo.SendCommand(_channelNum,
				Amqp.Channel.Queue.ClassId, Amqp.Channel.Queue.Methods.QueueDelete,
				writer,
				reply: (channel, classMethodId, error) =>
				{
					if (waitConfirmation && classMethodId == AmqpClassMethodChannelLevelConstants.QueuePurgeOk)
					{
						_connectionIo._frameReader.Read_GenericMessageCount(count =>
						{
							if (LogAdapter.ProtocolLevelLogEnabled)
								LogAdapter.LogDebug(LogSource, "< QueueDeleteOk " + queue);

							tcs.SetResult(count);
						});
					}
					else if (!waitConfirmation)
					{
						tcs.SetResult(0);
					}
					else
					{
						AmqpIOBase.SetException(tcs, error, classMethodId);
					}
				},
				expectsReply: waitConfirmation);

			return tcs.Task;
		}

		public Task<uint> __QueuePurge(string queue, bool waitConfirmation)
		{
			var tcs = new TaskCompletionSource<uint>(TaskCreationOptions.RunContinuationsAsynchronously);

			var writer = AmqpChannelLevelFrameWriter.QueuePurge(queue, waitConfirmation);

			_connectionIo.SendCommand(_channelNum,
				Amqp.Channel.Queue.ClassId, Amqp.Channel.Queue.Methods.QueuePurge,
				writer,
				reply: (channel, classMethodId, error) =>
				{
					if (waitConfirmation && classMethodId == AmqpClassMethodChannelLevelConstants.QueuePurgeOk)
					{
						_connectionIo._frameReader.Read_GenericMessageCount(count =>
						{
							if (LogAdapter.ProtocolLevelLogEnabled)
								LogAdapter.LogDebug(LogSource, "< QueuePurgeOk " + queue);

							tcs.SetResult(count);
						});
					}
					else if (!waitConfirmation)
					{
						tcs.SetResult(0);
					}
					else
					{
						AmqpIOBase.SetException(tcs, error, classMethodId);
					}
				},
				expectsReply: waitConfirmation);

			return tcs.Task;
		}

		#endregion
	}
}