namespace RabbitMqNext.Internals.RingBuffer.Locks
{
	using System;
	using System.Runtime.CompilerServices;
	using System.Runtime.InteropServices;
	using System.Threading;


	/// <summary>
	/// heavily "inspired" by the counterpart ManualResetEventSlim
	/// with the difference that waiters try to atomically "unset" the set bit/flag
	/// </summary>
	public class AutoResetSuperSlimLock
	{
		[StructLayout(LayoutKind.Sequential)]
		private struct LockState
		{
			private PaddingForInt32 _pad0;
			public volatile int _state;
			private PaddingForInt32 _pad1;
		}

		private LockState _lockState;

		public const int OperationalStateMask = 0x4000;  // 0100 0000 0000 0000
		public const int OperationalStatePos = 14;
		public const int SignalledStateMask = 0x8000;    // 1000 0000 0000 0000
		public const int SignalledStatePos = 15;
		public const int NumWaitersStateMask = 0xFF;     // 0000 0000 1111 1111
		public const int NumWaitersStatePos = 0;
		public const int WaiterMax = 255; // 0xFF

		private readonly object _lock = new object();
		
//		private static readonly int ProcCounter = Environment.ProcessorCount;
		private const int SpinCount = 10;

		public AutoResetSuperSlimLock(bool initialState = false)
		{
			_lockState._state = (1 << OperationalStatePos) | (initialState ? SignalledStateMask : 0);
		}

		public void Restore()
		{
			lock (_lock)
			{
				// _lockState._state = 0;
				// _lockState.Operational = true; // allow waiters

				_lockState._state = (1 << OperationalStatePos); // operational again
			}
		}

		/// <summary>
		/// For advanced scenarios, since it releases waiters 
		/// without a corresponding 'Set'
		/// </summary>
		public void Reset()
		{
			// Free waiters
			lock (_lock)
			{
				Operational = false; // disallow waiters

				Monitor.PulseAll(_lock);
			}
		}

		public bool Wait()
		{
			return Wait(Timeout.Infinite);
		}

		public bool Wait(int millisecondsTimeout)
		{
			if (!Operational) // volatile read perf hit
				throw new Exception("Cannot wait when not operational");

			if (!CheckForIsSetAndResetIfTrue())
			{
				if (SpinAndTryToObtainLock()) 
					return true;

				lock (_lock)
				{
					if (!Operational) // volatile read perf hit
						throw new Exception("Cannot wait when not operational");

					// if (!CheckForIsSetAndResetIfTrue())
					{
						Waiters++;

						if (CheckForIsSetAndResetIfTrue())
						{
							Waiters--;
							return true;
						}

						try
						{
							while (true)
							{
								if (!Monitor.Wait(_lock, millisecondsTimeout))
								{
									return false; // timeout expired
								}
								else
								{
									var gained = CheckForIsSetAndResetIfTrue();

									if (gained) break;
								}
							}
						}
						finally
						{
							Waiters--;
						}
					}
				}
			}

			return true;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Set()
		{
			if (!Operational) // volatile read perf hit
				throw new Exception("Cannot Set when not operational");

			// shortcut - if already set
			if (IsSet) return;

			AtomicChange(1, SignalledStatePos, SignalledStateMask);

			lock (_lock)
			{
				if (Waiters > 0)
				{
					// The awakened thread will still check if it can Xor the state before continuing
					Monitor.Pulse(_lock);
				}
			}
		}

		public bool IsSet
		{
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			get { return (_lockState._state & SignalledStateMask) != 0; }
		}

		public void Dispose()
		{
			Reset();
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private bool CheckForIsSetAndResetIfTrue()
		{
			return TryAtomicXor(0, SignalledStatePos, SignalledStateMask) || !Operational;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void AtomicChange(int val, int shifts, int mask)
		{
			var spinWait = new SpinWait();
			while (true)
			{
				var curState = _lockState._state;

				// (1) zero the updateBits.  eg oldState = [11111111]    flag=00111000  newState= [11000111]
				// (2) map in the newBits.              eg [11000111] newBits=00101000, newState= [11101111]
				int newState = (curState & ~mask) | (val << shifts);

#pragma warning disable 420
				if (Interlocked.CompareExchange(ref _lockState._state, newState, curState) == curState)
#pragma warning restore 420
				{
					break;
				}

				spinWait.SpinOnce();
//				Thread.SpinWait(4);
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public bool TryAtomicXor(int val, int shifts, int mask)
		{
			var curState = _lockState._state;

			// (1) zero the updateBits.  eg oldState = [11111111]    flag=00111000  newState= [11000111]
			// (2) map in the newBits.              eg [11000111] newBits=00101000, newState= [11101111]
			int newState = (curState & ~mask) |  (val << shifts);

			// (1) zero the updateBits.  eg oldState = [11111111]    flag=00111000  newState= [11000111]
			// (2) map in the newBits.              eg [11000111] newBits=00101000, newState= [11101111]
			int expected = (newState ^ mask) | curState;

			// newState [100001]
			// expected [000001]

#pragma warning disable 420
			var result = (Interlocked.CompareExchange(ref _lockState._state, newState, expected) );
			return result == expected;
#pragma warning restore 420
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private bool SpinAndTryToObtainLock()
		{
			for (int i = 0; i < SpinCount; i++)
			{
				if (CheckForIsSetAndResetIfTrue())
				{
					return true;
				}
				Thread.SpinWait(1);
				// Thread.SpinWait(1 << i);
//				if (i < HowManySpinBeforeYield)
//				{
//					if (i == HowManySpinBeforeYield / 2)
//					{
//						Thread.Yield();
//					}
//					else
//					{
//						Thread.SpinWait(ProcCounter * (4 << i));
//					}
//				}
//				else if (i % HowManyYieldEverySleep1 == 0)
//				{
//					Thread.Sleep(1);
//				}
//				else if (i % HowManyYieldEverySleep0 == 0)
//				{
//					Thread.Sleep(0);
//				}
//				else
//				{
//					Thread.Yield();
//				}
			}
			return CheckForIsSetAndResetIfTrue();
		}

		public int Waiters
		{
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			get
			{
				return ExtractStatePortionAndShiftRight(_lockState._state, NumWaitersStateMask, NumWaitersStatePos);
			}
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			set
			{
				if (value >= WaiterMax) throw new ArgumentOutOfRangeException("waiters", "Waiters cannot be greater than "+ WaiterMax + ". Assigned value: " + value);
				AtomicChange(value, NumWaitersStatePos, NumWaitersStateMask);
			}
		}

		public bool Operational
		{
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			get
			{
				return ExtractStatePortionAndShiftRight(_lockState._state, OperationalStateMask, OperationalStatePos) == 1; 
			}
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			set { AtomicChange(value ? 1 : 0, OperationalStatePos, OperationalStateMask); }
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private static int ExtractStatePortionAndShiftRight(int state, int mask, int rightBitShiftCount)
		{
			//convert to uint before shifting so that right-shift does not replicate the sign-bit,
			//then convert back to int.
			return unchecked((int)(((uint)(state & mask)) >> rightBitShiftCount));
		}
	}
}