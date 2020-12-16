using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    public class WhereFWindow<TPayload> : UnaryFWindow<TPayload, TPayload>
    {
        private Func<TPayload, bool> _filter;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="input"></param>
        /// <param name="filter"></param>
        public WhereFWindow(FWindowable<TPayload> input, Expression<Func<TPayload, bool>> filter)
            : base(input, input.Size, input.Period, input.Offset, input.Duration)
        {
            _filter = filter.Compile();
            _Payload = Input.Payload as FSubWindow<TPayload>;
            _Sync = Input.Sync as FSubWindow<long>;
            _Other = Input.Other as FSubWindow<long>;
            _BV = new BVFSubWindow(Length);
        }

        /// <summary>
        /// 
        /// </summary>
        protected override bool _Init()
        {
            var ret = Input.Init();
            SyncTime = Input.SyncTime;
            return ret;
        }

        /// <summary>
        /// 
        /// </summary>
        protected override int _Compute()
        {
            var len = Input.Compute();

            _BV.Unset();

            var payload = Payload.Data;
            var payloadOffset = Payload.Offset;
            var ibvOffset = Input.BV.Offset;
            var obvOffset = BV.Offset;
            var length = Length;
            unsafe
            {
                fixed (long* ibv = Input.BV.Data)
                fixed (long* bv = BV.Data)
                {
                    for (int i = 0; i < length; i++)
                    {
                        var ibi = ibvOffset + i;
                        var obi = obvOffset + i;
                        var pi = payloadOffset + i;

                        if (i >= len || ((ibv[ibi >> 6] & (1L << (ibi & 0x3f))) != 0) || !_filter(payload[pi]))
                        {
                            bv[obi >> 6] |= (1L << (obi & 0x3f));
                        }
                    }
                }
            }

            SyncTime = Input.SyncTime;
            return len;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override bool _Slide(long tsync)
        {
            var ret = Input.Slide(tsync);
            SyncTime = Input.SyncTime;
            return ret;
        }
    }
}