﻿namespace RabbitMqNext.Utils
{
	using System.Diagnostics;
	using System.Runtime.CompilerServices;
	using Internals.RingBuffer;
	
	public static class Asserts
	{
		[DebuggerStepThrough]
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static void AssertNotReadFrameThread(string msg)
		{
#if ASSERT
			if (ThreadUtils.IsReadFrameThread())
			{
				LogAdapter.LogError("Asserts", msg + " - Details: Assert failed: AssertNotReadFrameThread at " + new StackTrace());
			}
#endif
		}
	}

	
}
