namespace RabbitMqNext.Io
{
	using System;
	using System.Collections.Concurrent;
	using System.Collections.Generic;
	using System.Runtime.CompilerServices;
	using System.Threading;
	using System.Threading.Tasks;
	using RabbitMqNext;
	using RabbitMqNext.Internals;


	/// <summary>
	/// Commonality of connection and channel
	/// </summary>
	public abstract class AmqpIOBase : IDisposable
	{
		private const string LogSource = "AmqpqIOBase";

		public readonly ConcurrentQueue<CommandToSend> _awaitingReplyQueue;
		public bool _isClosed, _isDisposed;
		public readonly ushort _channelNum;

		// if not null, it's the error that closed the channel or connection
		public AmqpError _lastError;

		protected AmqpIOBase(ushort channelNum)
		{
			_channelNum = channelNum;
			_awaitingReplyQueue = new ConcurrentQueue<CommandToSend>();
		}

		public List<Func<AmqpError, Task>> ErrorCallbacks;

		public bool IsClosed { get { return _isClosed; } }

		public async void Dispose()
		{
			if (_isDisposed) return;
			Thread.MemoryBarrier();
			_isDisposed = true;

			await InitiateCleanClose(false, null).ConfigureAwait(false);

			InternalDispose();
		}

		protected abstract void InternalDispose();

		public abstract Task HandleFrame(int classMethodId);

		public abstract Task SendCloseConfirmation();

		public abstract Task SendStartClose();

		// To be use in case of exceptions on our end. Close everything asap
		public virtual async Task InitiateAbruptClose(Exception reason)
		{
			if (_isClosed) return;
			Thread.MemoryBarrier();
			_isClosed = true;

			var syntheticError = new AmqpError {ReplyText = reason.Message};

			DrainPending(syntheticError);

			await FireErrorEvent(syntheticError);

			this.Dispose();
		}

		public virtual async Task<bool> InitiateCleanClose(bool initiatedByServer, AmqpError error)
		{
			if (_isClosed) return false;
			Thread.MemoryBarrier();
			_isClosed = true;

			if (LogAdapter.IsDebugEnabled)
				LogAdapter.LogDebug(LogSource, "Clean close initiated. By server? " + initiatedByServer);

			if (initiatedByServer)
				await SendCloseConfirmation().ConfigureAwait(false);
			else
				await SendStartClose().ConfigureAwait(false);

			DrainPending(error);

			if (error != null)
			{
				await FireErrorEvent(error);
			}

			return true;
		}

		public void HandReplyToAwaitingQueue(int classMethodId)
		{
			CommandToSend sent;

			if (_awaitingReplyQueue.TryDequeue(out sent))
			{
				sent.RunReplyAction(_channelNum, classMethodId, null);
			}
			// else
			{
				// nothing was really waiting for a reply.. exception? wtf?
				// TODO: log
			}
		}

		// A disconnect may be expected coz we send a connection close, etc.. 
		// or it may be something abruptal
		public void HandleDisconnect()
		{
			if (_isClosed) return; // we have initiated the close

			// otherwise

			_lastError = new AmqpError { ClassId = 0, MethodId = 0, ReplyCode = 0, ReplyText = "disconnected" };

			DrainPending(_lastError);
		}

		public Task<bool> HandleCloseMethodFromServer(AmqpError error) 
		{
			_lastError = error;
			return InitiateCleanClose(true, error);
		}

		protected virtual void DrainPending(AmqpError error)
		{
			// releases every task awaiting
			CommandToSend sent;
			while (_awaitingReplyQueue.TryDequeue(out sent))
			{
				if (error != null && sent.ClassId == error.ClassId && sent.MethodId == error.MethodId)
				{
					// if we find the "offending" command, then it gets a better error message
					sent.RunReplyAction(0, 0, error);
				}
				else
				{
					// any other task dies with a generic error.
					sent.RunReplyAction(0, 0, null);
				}
			}
		}

		private async Task FireErrorEvent(AmqpError error)
		{
			Func<AmqpError,Task>[] copy = null;
			lock (ErrorCallbacks)
			{
				if (ErrorCallbacks == null || ErrorCallbacks.Count == 0) return;

				copy = ErrorCallbacks.ToArray();
			}

			foreach (var errorCallback in copy)
			{
				await errorCallback(error);
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static void SetException<T>(TaskCompletionSource<T> tcs, AmqpError error, int classMethodId)
		{
			if (tcs == null) return;
			if (error != null)
			{
				tcs.TrySetException(new Exception("Error: " + error.ToErrorString()));
			}
			else if (classMethodId == 0)
			{
				tcs.TrySetException(new Exception("The server closed the connection"));
			}
			else
			{
				var classId = classMethodId >> 16;
				var methodId = classMethodId & 0x0000FFFF;

				LogAdapter.LogError(LogSource, "Unexpected situation: classId " + classId + " method " + methodId + " and error = null");

				tcs.TrySetException(new Exception("Unexpected reply from the server: classId = " + classId + " method " + methodId));
			}
		}

//		public static void SetException(TaskSlim tcs, AmqpError error, int classMethodId)
//		{
//			if (tcs == null) return;
//			if (error != null)
//			{
//				tcs.TrySetException(new Exception("Error: " + error.ToErrorString()));
//			}
//			else if (classMethodId == 0)
//			{
//				tcs.TrySetException(new Exception("The server closed the connection"));
//			}
//			else
//			{
//				var classId = classMethodId >> 16;
//				var methodId = classMethodId & 0x0000FFFF;
//
//				LogAdapter.LogError("AmqpIOBase", "Unexpected situation: classId " + classId + " method " + methodId + " and error = null");
//
//				tcs.TrySetException(new Exception("Unexpected reply from the server: classId = " + classId + " method " + methodId));
//			}
//		}
	}
}