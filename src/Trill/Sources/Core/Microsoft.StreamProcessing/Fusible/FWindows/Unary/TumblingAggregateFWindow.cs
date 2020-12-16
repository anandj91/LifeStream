using System;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    /// <typeparam name="TAggState"></typeparam>
    public class TumblingAggregateFWindow<TPayload, TAggState, TResult> : UnaryFWindow<TPayload, TResult>
    {
        private long _window;
        private IAggregate<TPayload, TAggState, TResult> _aggregate;
        private Func<TAggState> _init;
        private Func<TAggState, long, TPayload, TAggState> _acc;
        private Func<TAggState, TResult> _res;
        private TAggState _state;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="input"></param>
        /// <param name="aggregate"></param>
        /// <param name="window"></param>
        public TumblingAggregateFWindow(
            FWindowable<TPayload> input,
            IAggregate<TPayload, TAggState, TResult> aggregate,
            long window
        ) : base(input, input.Size, window, input.Offset, window)
        {
            Invariant.IsTrue(input.Size % window == 0, "Input size need to be a multiple of window");
            _window = window;
            _aggregate = aggregate;
            _init = _aggregate.InitialState().Compile();
            _acc = _aggregate.Accumulate().Compile();
            _res = _aggregate.ComputeResult().Compile();
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
        /// <returns></returns>
        protected override int _Compute()
        {
            var ilen = Input.Compute();

            var period = Period;
            var iperiod = Input.Period;
            var ipayload = Input.Payload.Data;
            var ipayloadOffset = Input.Payload.Offset;
            var ibvOffset = Input.BV.Offset;
            var isyncOffset = Input.Sync.Offset;
            var payload = Payload.Data;
            var payloadOffset = Payload.Offset;
            var syncOffset = Sync.Offset;
            var otherOffset = Other.Offset;
            var bvOffset = BV.Offset;
            var syncTime = Input.Sync.Data[isyncOffset];

            var length = Length;
            var factor = Input.Length / Length;
            unsafe
            {
                fixed (long* ibv = Input.BV.Data)
                fixed (long* bv = _BV.Data)
                {
                    for (int j = 0; j < length; j++)
                    {
                        bool hasResult = false;
                        _state = _init();
                        for (int i = j * factor; i < (j + 1) * factor; i++)
                        {
                            var ibi = ibvOffset + i;
                            if ((ibv[ibi >> 6] & (1L << (ibi & 0x3f))) == 0)
                            {
                                var ipi = ipayloadOffset + i;
                                var item = ipayload[ipi];
                                _state = _acc(_state, syncTime + i * iperiod, item);
                                hasResult = true;
                            }
                        }

                        var bi = bvOffset + j;
                        if (hasResult)
                        {
                            payload[payloadOffset + j] = _res(_state);
                            _Sync.Data[syncOffset + j] = syncTime;
                            _Other.Data[otherOffset + j] = syncTime + period;
                            bv[bi >> 6] &= ~(1L << (bi & 0x3f));
                        }
                        else
                        {
                            bv[bi >> 6] |= (1L << (bi & 0x3f));
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