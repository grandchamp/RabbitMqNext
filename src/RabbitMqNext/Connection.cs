namespace RabbitMqNext
{
	using System;
	using System.Collections.Generic;
	using System.Diagnostics;
	using System.Diagnostics.Contracts;
	using System.Runtime.CompilerServices;
	using System.Threading;
	using System.Threading.Tasks;
	using RabbitMqNext.Io;
	using RabbitMqNext.Internals;
	using Recovery;


	public sealed class Connection : IConnection
	{
		private const string LogSource = "Connection";

		public readonly ConnectionIO _io;

		private Channel[] _channels; // 1-based index
		
		private int _channelNumbers;
		private ConnectionInfo _connectionInfo;
		private CancellationTokenSource _channelCancellationTokenSource = new CancellationTokenSource();
		private readonly List<Func<AmqpError, Task>> _errorsCallbacks = new List<Func<AmqpError, Task>>();

		private DateTime _lastHearbeatReceived;
		private Timer _heartbeatTimer;

		public Connection()
		{
			_io = new ConnectionIO(this)
			{
				ErrorCallbacks = _errorsCallbacks
			};
		}

		public event Action<string> ConnectionBlocked;

		public event Action ConnectionUnblocked;

		public RecoveryEnabledConnection Recovery { get; set; }

		public void AddErrorCallback(Func<AmqpError, Task> errorCallback)
		{
			if (errorCallback == null) throw new ArgumentNullException("errorCallback");

			lock(_errorsCallbacks) _errorsCallbacks.Add(errorCallback);
		}

		public void RemoveErrorCallback(Func<AmqpError, Task> errorCallback)
		{
			if (errorCallback == null) throw new ArgumentNullException("errorCallback");

			lock (_errorsCallbacks) _errorsCallbacks.Remove(errorCallback);
		}

		public Task<bool> Connect(string hostname, string vhost, 
									string username, string password, 
									int port, string connectionName, 
									ushort heartbeat, bool throwOnError = true)
		{
			// Saves info for reconnection scenarios
			_connectionInfo = new ConnectionInfo 
			{ 
				hostname = hostname, 
				vhost = vhost, 
				username = username, 
				password = password, 
				port = port,
				connectionName = connectionName,
				heartbeat = heartbeat
			};

			return InternalConnect(hostname);
		}

		public void SetMaxChannels(int maxChannels)
		{
			_channels = new Channel[maxChannels + 1];
		}

		public async Task<bool> InternalConnect(string hostname, bool throwOnError = true)
		{
			if (LogAdapter.ExtendedLogEnabled)
				LogAdapter.LogDebug(LogSource, "Trying to connect to " + hostname);

			var result = await _io.InternalDoConnectSocket(hostname, _connectionInfo.port, throwOnError).ConfigureAwait(false);

			if (!result) return false;

			result = await _io.Handshake(_connectionInfo.vhost, 
				_connectionInfo.username, 
				_connectionInfo.password, 
				_connectionInfo.connectionName, _connectionInfo.heartbeat, throwOnError).ConfigureAwait(false);

			if (result && this.Recovery != null)
			{
				this.Recovery.NotifyConnected(hostname);
			}

			if (_connectionInfo.heartbeat != 0)
			{
				SetupHeartbeat(_connectionInfo.heartbeat);
			}

			return result;
		}

		public bool IsClosed { get { return _io.IsClosed; } }

		public Task<IChannel> CreateChannel(ChannelOptions options)
		{
			return InternalCreateChannel(options, null, withPubConfirm: false);
		}

		public Task<IChannel> CreateChannelWithPublishConfirmation(ChannelOptions options, int maxunconfirmedMessages = 100)
		{
			return InternalCreateChannel(options, null, maxunconfirmedMessages, withPubConfirm: true);
		}

		public void Dispose()
		{
			if (this.Recovery != null)
				this.Recovery.Dispose();

			this._io.Dispose();
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public ChannelIO ResolveChannel(ushort channel)
		{
			if (channel > _channels.Length)
			{
				LogAdapter.LogError(LogSource, "ResolveChannel for invalid channel " + channel);
				throw new Exception("Unexpected channel number " + channel);
			}

			var channelInst = _channels[channel];
			if (channelInst == null)
			{
				LogAdapter.LogError(LogSource, "ResolveChannel for non-initialized channel " + channel);
				throw new Exception("Channel not initialized " + channel);
			}

			return channelInst._io;
		}

		public void CloseAllChannels(Exception reason)
		{
			if (_channels == null) return;
			
			foreach (var channel in _channels)
			{
				if (channel == null) continue;

				channel._io.InitiateAbruptClose(reason).IntentionallyNotAwaited();
			}
		}

		public void CloseAllChannels(bool initiatedByServer, AmqpError error)
		{
			if (LogAdapter.IsDebugEnabled) LogAdapter.LogDebug(LogSource, "Closing all channels");

			if (_channels == null) return;

			foreach (var channel in _channels)
			{
				if (channel == null) continue;

#pragma warning disable 4014
				channel._io.InitiateCleanClose(initiatedByServer, error);
#pragma warning restore 4014
			}
		}

		public void Reset()
		{
			// For now this only consists of reseting the channel array

			for (int i = 0; i < _channels.Length; i++)
			{
				Interlocked.Exchange(ref _channels[i], null);
			}
		}

		public async Task<IChannel> InternalCreateChannel(ChannelOptions options, int? desiredChannelNum, int maxunconfirmedMessages = 0, bool withPubConfirm = false)
		{
			var channelNum = desiredChannelNum.HasValue ?
				(ushort) desiredChannelNum.Value : 
				(ushort) Interlocked.Increment(ref _channelNumbers);

			if (channelNum > _channels.Length - 1)
				throw new Exception("Exceeded channel limits");

			var channel = new Channel(options, channelNum, this._io, _channelCancellationTokenSource.Token);

			try
			{
				_channels[channelNum] = channel;
				await channel.Open().ConfigureAwait(false);
				if (withPubConfirm)
				{
					await channel.EnableConfirmation(maxunconfirmedMessages).ConfigureAwait(false);
				}
				return channel;
			}
			catch
			{
				// TODO: release channel number that wasnt used
				_channels[channelNum] = null;
				throw;
			}
		}

		public RecoveryAction NotifyAbruptClose(Exception reason)
		{
			StopHeartbeatTimerIfNeeded();

			if (this.Recovery != null)
				return this.Recovery.NotifyAbruptClose(reason);

			return RecoveryAction.NoAction;
		}

		public RecoveryAction NotifyClosedByServer()
		{
			StopHeartbeatTimerIfNeeded();

			if (this.Recovery != null)
				return this.Recovery.NotifyCloseByServer();

			return RecoveryAction.NoAction;
		}

		public void NotifyClosedByUser()
		{
			if (this.Recovery != null)
				this.Recovery.NotifyCloseByUser();
		}

		public class ConnectionInfo
		{
			public string hostname;
			public string vhost;
			public string username;
			public string password;
			public string connectionName;
			public int port;
			public ushort heartbeat;
		}

		public void BlockAllChannels(string reason)
		{
			LogAdapter.LogWarn(LogSource, "Blocking all channels: " + reason);

			foreach (var channel in _channels)
			{
				if (channel == null) continue;
				channel.BlockChannel(reason);
			}

			var ev = this.ConnectionBlocked;
			if (ev != null)
			{
				ev(reason);
			}
		}

		public void UnblockAllChannels()
		{
			LogAdapter.LogWarn(LogSource, "Unblocking all channels");

			foreach (var channel in _channels)
			{
				if (channel == null) continue;
				channel.UnblockChannel();
			}

			var ev = this.ConnectionUnblocked;
			if (ev != null)
			{
				ev();
			}
		}

		private void SetupHeartbeat(ushort heartbeat)
		{
			StopHeartbeatTimerIfNeeded();

			_lastHearbeatReceived = DateTime.Now;

			_heartbeatTimer = new Timer(OnHeartbeatCallback, null, TimeSpan.FromMilliseconds(300), TimeSpan.FromSeconds(heartbeat / 2));
		}

		private void StopHeartbeatTimerIfNeeded()
		{
			if (_heartbeatTimer != null)
			{
				_heartbeatTimer.Dispose();
				_heartbeatTimer = null;
			}
		}

		private void OnHeartbeatCallback(object state)
		{
			var timeoutTs = TimeSpan.FromSeconds(_connectionInfo.heartbeat * 1.5); // timeout with some tolerance
			var diff = DateTime.Now - _lastHearbeatReceived;
			
			if (diff > timeoutTs) // is past timeout?
			{
				// yes, so we assume connection is dead. autorecovery might still kick in

				this._io.InitiateCleanClose(true, new AmqpError() { ReplyText = "Heartbeat timeout" }).IntentionallyNotAwaited();

				return;
			}

			// If we got here, all is good and we need to send our hearbeat
			_io.SendHeartbeat();
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void HeartbeatReceived()
		{
			_lastHearbeatReceived = DateTime.Now;
		}
	}
}
