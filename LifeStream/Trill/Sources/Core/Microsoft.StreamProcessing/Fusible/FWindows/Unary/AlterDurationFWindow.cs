using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    public class AlterDurationFWindow<TPayload> : UnaryFWindow<TPayload, TPayload>
    {
        private Func<long, long, long> _durationSelector;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="input"></param>
        /// <param name="durationSelector"></param>
        public AlterDurationFWindow(FWindowable<TPayload> input, Expression<Func<long, long, long>> durationSelector)
            : base(input, input.Size, input.Period, input.Offset, -1)
        {
            _durationSelector = durationSelector.Compile();
            _Payload = Input.Payload as FSubWindow<TPayload>;
            _Sync = Input.Sync as FSubWindow<long>;
            _BV = Input.BV as BVFSubWindow;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override bool _Init()
        {
            var ret = Input.Init();
            SyncTime = Input.SyncTime;
            return ret;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override int _Compute()
        {
            var len = Input.Compute();
            var syncTime = Input.SyncTime - Input.Size;

            var period = Input.Period;
            var iother = Input.Other.Data;
            var iotherOffset = Input.Other.Offset;
            var other = Other.Data;
            var otherOffset = Other.Offset;
            var ibvOffset = Input.BV.Offset;
            var length = Length;

            unsafe
            {
                fixed (long* ibv = Input.BV.Data)
                {
                    for (int i = 0; i < length; i++)
                    {
                        var ibi = ibvOffset + i;
                        if ((ibv[ibi >> 6] & (1L << (ibi & 0x3f))) == 0)
                        {
                            other[otherOffset + i] = syncTime + _durationSelector(syncTime, iother[iotherOffset + i]);
                        }

                        syncTime += period;
                    }
                }
            }

            SyncTime = syncTime;
            return Length;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tsync"></param>
        /// <returns></returns>
        protected override bool _Slide(long tsync)
        {
            var ret = Input.Slide(tsync);
            SyncTime = Input.SyncTime;
            return ret;
        }
    }
}