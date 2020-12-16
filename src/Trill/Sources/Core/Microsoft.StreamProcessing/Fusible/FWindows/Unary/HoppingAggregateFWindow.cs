using System;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    /// <typeparam name="TAggState"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    public class HoppingAggregateFWindow<TPayload, TAggState, TResult> : UnaryFWindow<TPayload, TResult>
    {
        private long _window;
        private IAggregate<TPayload, TAggState, TResult> _aggregate;
        private Func<TAggState> _init;
        private Func<TAggState, long, TPayload, TAggState> _acc;
        private Func<TAggState, TAggState, TAggState> _diff;
        private Func<TAggState, TResult> _res;
        private TAggState[] _states;
        private TAggState _state;
        private int _idx;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="input"></param>
        /// <param name="aggregate"></param>
        /// <param name="window"></param>
        /// <param name="period"></param>
        public HoppingAggregateFWindow(
            FWindowable<TPayload> input,
            IAggregate<TPayload, TAggState, TResult> aggregate,
            long window, long period
        ) : base(input, input.Size, period, input.Offset, period)
        {
            Invariant.IsTrue(period % Input.Period == 0, "Period must be a multiple of input period");
            Invariant.IsTrue(window % period == 0, "Window must be a multiple of period");
            _window = window;
            _aggregate = aggregate;
            _init = _aggregate.InitialState().Compile();
            _acc = _aggregate.Accumulate().Compile();
            _diff = aggregate.Difference().Compile();
            _res = _aggregate.ComputeResult().Compile();
            _states = new TAggState[(window / period) + 1];
            _idx = -1;
            _BV = new BVFSubWindow(Length);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override bool _Init()
        {
            var ret = Input.Init();
            SyncTime = Input.SyncTime;

            _idx = 0;
            for (int i = 0; i < _states.Length; i++)
            {
                _states[i] = _init();
            }

            _state = _init();

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
                        TAggState state = _init();
                        for (int i = j * factor; i < (j + 1) * factor; i++)
                        {
                            var ibi = ibvOffset + i;
                            if ((ibv[ibi >> 6] & (1L << (ibi & 0x3f))) == 0)
                            {
                                var ipi = ipayloadOffset + i;
                                var item = ipayload[ipi];
                                state = _acc(state, syncTime + i * iperiod, item);
                                _state = _acc(_state, syncTime + i * iperiod, item);
                                hasResult = true;
                            }
                        }

                        var bi = bvOffset + j;
                        _states[_idx] = state;
                        _idx = (_idx + 1) % _states.Length;
                        _state = _diff(_state, _states[_idx]);
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