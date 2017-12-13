namespace RabbitMqNext.Internals
{
	public static class CommandToSendExtensions
	{
		public static string ToDebugInfo(this CommandToSend source)
		{
			if (source == null) return string.Empty;

			return "[Channel_" + source.Channel + "] Class " + source.ClassId + " Method " + source.MethodId + " Opt: " + source.OptionalArg + "";
		}
	}
}