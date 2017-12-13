﻿namespace RabbitMqNext.Internals.RingBuffer
{
	using System;
	using System.Runtime.InteropServices;

	// From https://github.com/dotnet/corefx/blob/master/src/System.Threading.Tasks.Dataflow/src/public/Padding.cs
	// All kudos to them

	/// <summary>A placeholder class for common padding constants and eventually routines.</summary>
	public static class Padding
	{
		/// <summary>A size greater than or equal to the size of the most common CPU cache lines.</summary>
		// public const int CACHE_LINE_SIZE = 128;
		public const int CACHE_LINE_SIZE = 64;
	}

	/// <summary>Padding structure used to minimize false sharing in SingleProducerSingleConsumerQueue{T}.</summary>
	[StructLayout(LayoutKind.Explicit, Size = Padding.CACHE_LINE_SIZE - sizeof(Int32))] // Based on common case of 64-byte cache lines
	public struct PaddingForInt32
	{
	}

	[StructLayout(LayoutKind.Explicit, Size = Padding.CACHE_LINE_SIZE - sizeof(Int64))] // Based on common case of 64-byte cache lines
	public struct PaddingForInt64
	{
	}
}
