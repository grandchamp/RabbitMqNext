﻿namespace RabbitMqNext.Internals
{
	using System;
	using System.Collections.Generic;
	using System.Text;
	using WriterDelegate = System.Action<AmqpPrimitivesWriter, ushort, ushort, ushort, object>;

	static class AmqpConnectionFrameWriter
	{
		private const string LogSource = "AmqpConnectionFrameWriter";

		public static readonly HeartbeatFrame HeartbeatFrameWriter = new HeartbeatFrame();

		private static readonly byte[] GreetingPayload;

		static AmqpConnectionFrameWriter()
		{
			var v = Encoding.ASCII.GetBytes("AMQP");
			GreetingPayload = new byte[8];
			// Buffer.BlockCopy(v, 0, GreetingPayload, 0, v.Length);
			BufferUtil.FastCopy(GreetingPayload, 0, v, 0, v.Length);
			GreetingPayload[4] = 0;
			GreetingPayload[5] = 0; // major
			GreetingPayload[6] = 9; // minor 
			GreetingPayload[7] = 1; // revision
		}

		public class HeartbeatFrame : IFrameContentWriter
		{
			public void Write(AmqpPrimitivesWriter writer, ushort channel, ushort classId, ushort methodId, object optionalArg)
			{
				writer.WriteFrameStart(AmqpConstants.FrameHeartbeat, 0, 0, null, null);
				writer.WriteOctet(AmqpConstants.FrameEnd);
			}
		}

		public static WriterDelegate Greeting()
		{
			return (writer, channel, classId, methodId, args) =>
			{
				writer.WriteRaw(GreetingPayload, 0, GreetingPayload.Length);
			};
		}

		public static WriterDelegate ConnectionTuneOk(ushort channelMax, uint frameMax, ushort heartbeat)
		{
			const int payloadSize = 12; // 4 shorts + 1 int

			return (writer, channel, classId, methodId, args) =>
			{
				if (LogAdapter.ProtocolLevelLogEnabled)
					LogAdapter.LogDebug(LogSource, "> ConnectionTuneOk");

				writer.WriteFrameStart(AmqpConstants.FrameMethod, 0, payloadSize, 10, 31);

				writer.WriteUShort(channelMax);
				writer.WriteLong(frameMax);
				writer.WriteUShort(heartbeat);

				writer.WriteOctet(AmqpConstants.FrameEnd);
			};
		}

		public static WriterDelegate ConnectionStartOk(
			IDictionary<string, object> clientProperties,
			string mechanism, byte[] response, string locale)
		{
			return (writer, channel, classId, methodId, args) =>
			{
				if (LogAdapter.ProtocolLevelLogEnabled)
					LogAdapter.LogDebug(LogSource, "> ConnectionStartOk");

				writer.WriteFrameWithPayloadFirst(AmqpConstants.FrameMethod, 0, (w) =>
				{
					w.WriteUShort((ushort)10);
					w.WriteUShort((ushort)11);

					w.WriteTable(clientProperties);
					// w.WriteTable(null);
					w.WriteShortstr(mechanism);
					w.WriteLongbyte(response);
					w.WriteShortstr(locale);
				});
			};
		}

		public static WriterDelegate ConnectionOpen(string vhost, string caps, bool insist)
		{
			return (writer, channel, classId, methodId, args) =>
			{
				if (LogAdapter.ProtocolLevelLogEnabled)
					LogAdapter.LogDebug(LogSource, "> ConnectionOpen");

				writer.WriteFrameWithPayloadFirst(AmqpConstants.FrameMethod, channel, (w) =>
				{
					w.WriteUShort(classId);
					w.WriteUShort(methodId);

					w.WriteShortstr(vhost);
					w.WriteShortstr(caps);
					w.WriteBit(insist);
				});
			};
		}

		public static void ConnectionClose(AmqpPrimitivesWriter writer, ushort channel, ushort classId, ushort methodId, object args)
		{
			var closeArgs = (FrameParameters.CloseParams)args;

			writer.WriteFrameWithPayloadFirst(AmqpConstants.FrameMethod, channel, (w) =>
			{
				if (LogAdapter.ProtocolLevelLogEnabled)
					LogAdapter.LogDebug(LogSource, "> ConnectionClose");

				w.WriteUShort(classId);
				w.WriteUShort(methodId);

				w.WriteUShort(closeArgs.replyCode);
				w.WriteShortstr(closeArgs.replyText);
				w.WriteUShort(classId);
				w.WriteUShort(methodId);
			});
		}

		public static void ConnectionCloseOk(AmqpPrimitivesWriter writer, ushort channel, ushort classId, ushort methodId, object args)
		{
			if (LogAdapter.ProtocolLevelLogEnabled)
				LogAdapter.LogDebug(LogSource, "> ConnectionCloseOk");

			WriteEmptyMethodFrame(writer, channel, classId, methodId);
		}

		public static void WriteEmptyMethodFrame(AmqpPrimitivesWriter writer, ushort channel, ushort classId, ushort methodId)
		{
			const uint payloadSize = 4;
			writer.WriteFrameStart(AmqpConstants.FrameMethod, channel, payloadSize, classId, methodId);
			writer.WriteOctet(AmqpConstants.FrameEnd);
		}
	}
}
