namespace RabbitMqNext.Io
{
	using System;
	using System.Collections.Concurrent;
	using System.Collections.Generic;
	using System.Runtime.CompilerServices;
	using System.Text;
	using System.Threading;
	using System.Threading.Tasks;
	using Internals.RingBuffer;
	using RabbitMqNext;
	using RabbitMqNext.Internals;
	using Recovery;


	public class ConnectionIO : AmqpIOBase, IFrameProcessor
	{
		private const string LogSource = "ConnectionIO";

		public const string ReadFrameThreadNamePrefix = "ReadFramesLoop_";
		public const string WriteFrameThreadNamePrefix = "WriteFramesLoop_";

		private static int _counter = 0;

		private readonly Connection _conn;
		
		private readonly AutoResetEvent _commandOutboxEvent;
		private readonly ManualResetEventSlim _waitingServerReply;
		private readonly ConcurrentQueue<CommandToSend> _commandOutbox;
		private readonly ObjectPoolArray<CommandToSend> _cmdToSendObjPool;
		
		public readonly SocketHolder _socketHolder;

		private readonly AmqpPrimitivesWriter _amqpWriter; // main output writer
		private readonly AmqpPrimitivesReader _amqpReader; // main input reader
		public readonly FrameReader _frameReader; // interpreter of input

		private CancellationTokenSource _threadCancelSource;
		private CancellationToken _threadCancelToken;

		#region populated by server replies

		public IDictionary<string, object> ServerProperties { get; private set; }
		public string AuthMechanisms { get; private set; }
		public ushort Heartbeat { get; private set; }
		public string KnownHosts { get; private set; }
		private ushort _channelMax;
		private uint _frameMax;

		private volatile bool _readThreadRunning, _writeThreadRunning;
		private Action _connectionCleanPendingAction;

		#endregion
		
		public ConnectionIO(Connection connection) : base(channelNum: 0)
		{
			_conn = connection;
			_socketHolder = new SocketHolder();

			_commandOutboxEvent = new AutoResetEvent(false);
			_waitingServerReply = new ManualResetEventSlim(true);
			// _commandOutboxEvent = new AutoResetSuperSlimLock(false);
			_commandOutbox = new ConcurrentQueue<CommandToSend>();

			_cmdToSendObjPool = new ObjectPoolArray<CommandToSend>(() => new CommandToSend(i => _cmdToSendObjPool.PutObject(i)), 200, true);

			_amqpWriter = new AmqpPrimitivesWriter();
			_amqpReader = new AmqpPrimitivesReader();
			_frameReader = new FrameReader();
		}

		#region FrameProcessor

		Task IFrameProcessor.DispatchMethod(ushort channel, int classMethodId)
		{
			_conn.HeartbeatReceived(); // all incoming frames update "last received heartbeat" datetime

			if (channel != 0)
			{
				return _conn.ResolveChannel(channel).HandleFrame(classMethodId);
			}

			return this.HandleFrame(classMethodId);
		}

		void IFrameProcessor.DispatchHeartbeat()
		{
			_conn.HeartbeatReceived();
		}

		#endregion

		#region AmqpIOBase overrides

		public override Task HandleFrame(int classMethodId)
		{
			switch (classMethodId)
			{
				case AmqpClassMethodConnectionLevelConstants.ConnectionClose:
					_frameReader.Read_ConnectionClose(base.HandleCloseMethodFromServer);
					break;

				case AmqpClassMethodConnectionLevelConstants.ConnectionBlocked:
					_frameReader.Read_ConnectionBlocked(HandleConnectionBlocked);
					break;

				case AmqpClassMethodConnectionLevelConstants.ConnectionUnblocked:
					HandleConnectionUnblocked();
					break;

				default:
					// Any other connection level method?
					base.HandReplyToAwaitingQueue(classMethodId);
					break;
			}

			return Task.CompletedTask;
		}

		public override Task SendCloseConfirmation()
		{
			return __SendConnectionCloseOk();
		}

		public override Task SendStartClose()
		{
			return __SendConnectionClose(AmqpConstants.ReplySuccess, "bye"); ;
		}

		public void SendHeartbeat()
		{
			if (LogAdapter.ExtendedLogEnabled)
				LogAdapter.LogDebug(LogSource, "Sending Heartbeat");

			SendCommand(0, 0, 0, AmqpConnectionFrameWriter.ConnectionClose,
				reply: null,
				expectsReply: false,
				optArg: AmqpConnectionFrameWriter.HeartbeatFrameWriter, immediately: false);
		}

		public override async Task<bool> InitiateCleanClose(bool initiatedByServer, AmqpError error)
		{
			if (!initiatedByServer)
			{
				_conn.NotifyClosedByUser();

				while (!_commandOutbox.IsEmpty && !_socketHolder.IsClosed)
				{
					// give it some time to finish writing to the socket (if it's open)
					Thread.Sleep(250);
				}
			}
			else
			{
				if (_conn.NotifyClosedByServer() == RecoveryAction.WillReconnect)
				{
					CancelPendingCommands(error);

					return false;
				}
			}

			_conn.CloseAllChannels(initiatedByServer, error);

			await base.InitiateCleanClose(initiatedByServer, error).ConfigureAwait(false);

			CancelPendingCommands(error);

			_socketHolder.Close();

			OnSocketClosed();

			return true;
		}

		public override Task InitiateAbruptClose(Exception reason)
		{
			_conn.CloseAllChannels(reason);

			if (_conn.NotifyAbruptClose(reason) == RecoveryAction.WillReconnect)
			{
				CancelPendingCommands(reason);

				return Task.CompletedTask;
			}
			else
			{
				return base.InitiateAbruptClose(reason);
			}
		}

		protected override void InternalDispose()
		{
			// _cancellationTokenSource.Cancel();
			_socketHolder.Dispose();
		}

		#endregion

		private void OnSocketClosed()
		{
			if (_threadCancelSource != null)
			{
				LogAdapter.LogDebug(LogSource, "Socket closed. Stopping loops");

				_threadCancelSource.Cancel();
				_socketHolder.StopAndBlockBuffers();
				_commandOutboxEvent.Set(); // <- gets it out of the Wait state, and fast exit
			}

			base.HandleDisconnect(); // either a consequence of a close method, or unexpected disconnect
		}

		public async Task<bool> InternalDoConnectSocket(string hostname, int port, bool throwOnError)
		{
			var index = Interlocked.Increment(ref _counter);

			// read and write threads should not be running at this point
			if (_readThreadRunning) throw new Exception("Read thread should not be running");
			if (_writeThreadRunning) throw new Exception("Write thread should not be running");

			var result = await _socketHolder.Connect(hostname, port, index, throwOnError).ConfigureAwait(false);

			if (!result)
			{
				// Interlocked.Decrement(ref _counter);
				return false;
			}

			if (LogAdapter.IsDebugEnabled)
				LogAdapter.LogDebug(LogSource, "Connected socket to " + hostname + " port " + port);

			DrainCommandsAndResetErrorState();

			if (_threadCancelSource != null)
			{
				if (LogAdapter.IsDebugEnabled)
					LogAdapter.LogDebug(LogSource, "Disposing existing cancellation token source");

				_threadCancelSource.Dispose();
			}

			_threadCancelSource = new CancellationTokenSource();
			_threadCancelToken = _threadCancelSource.Token;

			_socketHolder.WireStreams(_threadCancelToken, OnSocketClosed);

			_amqpWriter.Initialize(_socketHolder.Writer);
			_amqpReader.Initialize(_socketHolder.Reader);
			_frameReader.Initialize(_socketHolder.Reader, _amqpReader, this);

			ThreadFactory.BackgroundThread(WriteFramesLoop, WriteFrameThreadNamePrefix + index);
			ThreadFactory.BackgroundThread(ReadFramesLoop, ReadFrameThreadNamePrefix + index);

			return true;
		}

		private void DrainCommandsAndResetErrorState()
		{
			_lastError = null;
			while (!_commandOutbox.IsEmpty)
			{
				CommandToSend cmd;
				_commandOutbox.TryDequeue(out cmd);
				if (cmd != null)
				{
					cmd.Dispose();
				}
			}
		}

		public async Task<bool> Handshake(string vhost, string username, string password, string connectionName, ushort heartbeat, bool throwOnError)
		{
			await __SendGreeting().ConfigureAwait(false);
			await __SendConnectionStartOk(username, password, connectionName).ConfigureAwait(false);
			await __SendConnectionTuneOk(_channelMax, _frameMax, heartbeat).ConfigureAwait(false); // disabling heartbeats for now
			_amqpWriter.FrameMaxSize = _frameMax;
			KnownHosts = await __SendConnectionOpen(vhost).ConfigureAwait(false);

			if (LogAdapter.ExtendedLogEnabled)
				LogAdapter.LogDebug(LogSource, "Known Hosts: " + KnownHosts);

			return true;
		}

//		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private void FlushCommand(CommandToSend cmdToSend)
		{
			if (_threadCancelToken.IsCancellationRequested)
			{
				if (cmdToSend.Tcs != null) cmdToSend.Tcs.TrySetCanceled();
				return;
			}

			var token = _threadCancelToken;

			if (!cmdToSend.Immediately)
			{
				_waitingServerReply.Wait(token); // Contention sadly required by the server/amqp
			}

			// The command will signal that we can send more commands...
			cmdToSend.Prepare(cmdToSend.ExpectsReply ? _waitingServerReply : null);

			if (cmdToSend.ExpectsReply) // enqueues as awaiting a reply from the server
			{
				_waitingServerReply.Reset(); // cannot send anything else

				var queue = cmdToSend.Channel == 0
					? _awaitingReplyQueue
					: _conn.ResolveChannel(cmdToSend.Channel)._awaitingReplyQueue;

				queue.Enqueue(cmdToSend);
			}

			// writes to socket
			var frameWriter = cmdToSend.OptionalArg as IFrameContentWriter;
			if (frameWriter != null)
			{
				frameWriter.Write(_amqpWriter, cmdToSend.Channel, cmdToSend.ClassId, cmdToSend.MethodId, cmdToSend.OptionalArg);
			}
			else
			{
				cmdToSend.commandGenerator(_amqpWriter, cmdToSend.Channel, cmdToSend.ClassId, cmdToSend.MethodId, cmdToSend.OptionalArg);
			}

			// if writing to socket is enough, set as complete
			if (!cmdToSend.ExpectsReply)
			{
				cmdToSend.RunReplyAction(0, 0, null);
			}
		}

		// Run on its own thread, and invokes user code from it (task continuations)
		private void WriteFramesLoop()
		{
			if (LogAdapter.IsDebugEnabled) LogAdapter.LogDebug(LogSource, "WriteFramesLoop starting");

			CommandToSend cmdToSend = null;

			_writeThreadRunning = true;

			try
			{
				var token = _threadCancelToken;

				while (!token.IsCancellationRequested)
				{
					_commandOutboxEvent.WaitOne(1000); // maybe it's better to _cancellationToken.Register(action) ?

					if (token.IsCancellationRequested) break; // perf hit?

					while (_commandOutbox.TryDequeue(out cmdToSend))
					{
						FlushCommand(cmdToSend);
					}
				}

				if (LogAdapter.IsErrorEnabled) LogAdapter.LogError(LogSource, "WriteFramesLoop exiting");
			}
			catch (ThreadAbortException)
			{
				// no-op
			}
			catch (Exception ex)
			{
				if (LogAdapter.IsErrorEnabled) LogAdapter.LogError(LogSource, "WriteFramesLoop error. Last command " + cmdToSend.ToDebugInfo(), ex);

				this.InitiateAbruptClose(ex).IntentionallyNotAwaited();
			}
			finally
			{
				_writeThreadRunning = false;

				TryTriggerClean();
			}
		}

		// Run on its own thread, and invokes user code from it
		private void ReadFramesLoop()
		{
			if (LogAdapter.IsDebugEnabled) LogAdapter.LogDebug(LogSource, "ReadFramesLoop starting");

			_readThreadRunning = true;

			try
			{
				var token = _threadCancelToken;

				while (!token.IsCancellationRequested)
				{
					_frameReader.ReadAndDispatch();
				}

				LogAdapter.LogError(LogSource, "ReadFramesLoop exiting");
			}
			catch (ThreadAbortException)
			{
				// no-op
			}
			catch (Exception ex)
			{
				LogAdapter.LogError(LogSource, "ReadFramesLoop error", ex);

				this.InitiateAbruptClose(ex).IntentionallyNotAwaited();
			}
			finally
			{
				_readThreadRunning = false;

				TryTriggerClean();
			}
		}

		public void SendCommand(ushort channel, ushort classId, ushort methodId,
			Action<AmqpPrimitivesWriter, ushort, ushort, ushort, object> commandWriter,
			Action<ushort, int, AmqpError> reply, bool expectsReply, TaskCompletionSource<bool> tcs = null,
			object optArg = null, Action prepare = null, bool immediately = false)
		{
			if (_lastError != null)
			{
				// ConnClose and Channel close el al, are allowed to move fwd. 
				var cmdId = (classId << 16) | methodId;
				if (cmdId != AmqpClassMethodConnectionLevelConstants.ConnectionClose &&
				    cmdId != AmqpClassMethodConnectionLevelConstants.ConnectionCloseOk &&
					cmdId != AmqpClassMethodChannelLevelConstants.ChannelClose &&
				    cmdId != AmqpClassMethodChannelLevelConstants.ChannelCloseOk)
				{
					SetErrorResultIfErrorPending(expectsReply, reply, tcs/*, tcsL*/); 
					return;
				}
			}

			// var cmd = _cmdToSendObjPool.GetObject();
			var cmd = new CommandToSend(null);
			cmd.BeginInit();

			cmd.Channel = channel;
			cmd.ClassId = classId;
			cmd.MethodId = methodId;
			cmd.ReplyAction = reply;
			cmd.commandGenerator = commandWriter;
			cmd.ExpectsReply = expectsReply;
			cmd.Tcs = tcs;
			cmd.OptionalArg = optArg;
			cmd.PrepareAction = prepare;
			cmd.Immediately = immediately;

			_commandOutbox.Enqueue(cmd);
			_commandOutboxEvent.Set();
		}

		private void SetErrorResultIfErrorPending(bool expectsReply, Action<ushort, int, AmqpError> replyFn, 
												  TaskCompletionSource<bool> tcs)
		{
			if (expectsReply)
			{
				replyFn(0, 0, _lastError);
			}
			else
			{
				AmqpIOBase.SetException(tcs, _lastError, 0);
//				AmqpIOBase.SetException(taskSlim, _lastError, 0);
			}
		}

		private void CancelPendingCommands(Exception reason)
		{
			CancelPendingCommands(new AmqpError() { ReplyText = reason.Message });
		}

		private void CancelPendingCommands(AmqpError error)
		{
			CommandToSend cmdToSend;
			while (_commandOutbox.TryDequeue(out cmdToSend))
			{
				cmdToSend.RunReplyAction(0, 0, error);
			}
		}

		#region Commands writing methods

		private Task __SendGreeting()
		{
			if (LogAdapter.ProtocolLevelLogEnabled)
				LogAdapter.LogDebug(LogSource, "__SendGreeting >");

			var tcs = new TaskCompletionSource<bool>();

			SendCommand(0, 0, 0,
				AmqpConnectionFrameWriter.Greeting(),
				reply: (channel, classMethodId, _) =>
				{
					if (classMethodId == AmqpClassMethodConnectionLevelConstants.ConnectionStart)
					{
						_frameReader.Read_ConnectionStart((versionMajor, versionMinor, serverProperties, mechanisms, locales) =>
						{
							this.ServerProperties = serverProperties;
							this.AuthMechanisms = mechanisms;

							if (LogAdapter.ProtocolLevelLogEnabled)
								LogAdapter.LogDebug(LogSource, "__SendGreeting completed");

							tcs.SetResult(true);
						});
					}
					else
					{
						// Unexpected
						tcs.SetException(new Exception("Unexpected result. Got " + classMethodId));
					}

				}, expectsReply: true, immediately: true);

			return tcs.Task;
		}

		private Task __SendConnectionTuneOk(ushort channelMax, uint frameMax, ushort heartbeat)
		{
			var tcs = new TaskCompletionSource<bool>();

			var writer = AmqpConnectionFrameWriter.ConnectionTuneOk(channelMax, frameMax, heartbeat);

			SendCommand(0, 10, 31, writer, reply: null, expectsReply: false, tcs: tcs, immediately: true);

			if (LogAdapter.ProtocolLevelLogEnabled)
				LogAdapter.LogDebug(LogSource, "__SendConnectionTuneOk >");

			return tcs.Task;
		}

		private Task<string> __SendConnectionOpen(string vhost)
		{
			if (LogAdapter.ProtocolLevelLogEnabled)
				LogAdapter.LogDebug(LogSource, "__SendConnectionOpen > vhost " + vhost);

			var tcs = new TaskCompletionSource<string>();
			var writer = AmqpConnectionFrameWriter.ConnectionOpen(vhost, string.Empty, false);

			SendCommand(0, 10, 40, writer,
				reply: (channel, classMethodId, error) =>
				{
					if (classMethodId == AmqpClassMethodConnectionLevelConstants.ConnectionOpenOk)
					{
						_frameReader.Read_ConnectionOpenOk((knowHosts) =>
						{
							if (LogAdapter.ProtocolLevelLogEnabled)
								LogAdapter.LogDebug(LogSource, "__SendConnectionOpen completed for vhost " + vhost);

							tcs.SetResult(knowHosts);
						});
					}
					else
					{
						AmqpIOBase.SetException(tcs, error, classMethodId);
					}
				}, expectsReply: true, immediately: true);

			return tcs.Task;
		}

		private Task __SendConnectionStartOk(string username, string password, string connectionName)
		{
			if (LogAdapter.ProtocolLevelLogEnabled)
				LogAdapter.LogDebug(LogSource, "__SendConnectionStartOk >");

			var tcs = new TaskCompletionSource<bool>();

			// Only supports PLAIN authentication for now

			var clientProperties = Protocol.ClientProperties;
			if (!string.IsNullOrEmpty(connectionName))
			{
				clientProperties = new Dictionary<string, object>(clientProperties);
				clientProperties["connection_name"] = connectionName;
			}

			var auth = Encoding.UTF8.GetBytes("\0" + username + "\0" + password);
			var writer = AmqpConnectionFrameWriter.ConnectionStartOk(clientProperties, "PLAIN", auth, "en_US");

			SendCommand(0, Amqp.Connection.ClassId, 30, 
				writer,
				reply: (channel, classMethodId, error) =>
				{
					if (classMethodId == AmqpClassMethodConnectionLevelConstants.ConnectionTune)
					{
						_frameReader.Read_ConnectionTune((channelMax, frameMax, heartbeat) =>
						{
							this._channelMax = channelMax;
							this._frameMax = frameMax;
							this.Heartbeat = heartbeat;

							if (LogAdapter.ProtocolLevelLogEnabled)
								LogAdapter.LogDebug(LogSource, "__SendConnectionStartOk completed.");

							if (LogAdapter.IsDebugEnabled)
								LogAdapter.LogDebug(LogSource, "Tune results: Channel max: " + channel + " Frame max size: " + frameMax + " heartbeat: " + heartbeat);

							tcs.SetResult(true);
						});
					}
					else
					{
						AmqpIOBase.SetException(tcs, error, classMethodId);
					}
				}, expectsReply: true, immediately: true);

			return tcs.Task;
		}

		private Task __SendConnectionClose(ushort replyCode, string message)
		{
			if (LogAdapter.ProtocolLevelLogEnabled)
				LogAdapter.LogDebug(LogSource, "__SendConnectionClose >");

			var tcs = new TaskCompletionSource<bool>();

			SendCommand(0, Amqp.Connection.ClassId, 50, AmqpConnectionFrameWriter.ConnectionClose,
				reply: (channel, classMethodId, error) =>
				{
					if (classMethodId == AmqpClassMethodConnectionLevelConstants.ConnectionCloseOk)
					{
						if (LogAdapter.ProtocolLevelLogEnabled)
							LogAdapter.LogDebug(LogSource, "__SendConnectionClose enabled");
						
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
				}, immediately: true);

			return tcs.Task;
		}

		private Task __SendConnectionCloseOk()
		{
			if (LogAdapter.ProtocolLevelLogEnabled)
				LogAdapter.LogDebug(LogSource, "__SendConnectionCloseOk >");

			var tcs = new TaskCompletionSource<bool>();

			this.SendCommand(0, Amqp.Connection.ClassId, 51,
				AmqpConnectionFrameWriter.ConnectionCloseOk,
				reply: null, expectsReply: false, tcs: tcs, immediately: true);

			return tcs.Task;
		}

		#endregion

		private void HandleConnectionBlocked(string reason)
		{
			_conn.BlockAllChannels(reason);
		}

		private void HandleConnectionUnblocked()
		{
			_conn.UnblockAllChannels();
		}

		public void TriggerOnceOnFreshState(Action action)
		{
			if (!_readThreadRunning && !_writeThreadRunning)
			{
				action();
			}
			else
			{
				_connectionCleanPendingAction = action;
			}
		}

		private void TryTriggerClean()
		{
			if (!_readThreadRunning && !_writeThreadRunning && _connectionCleanPendingAction != null)
			{
				_connectionCleanPendingAction();

				_connectionCleanPendingAction = null;
			}
		}
	}
}