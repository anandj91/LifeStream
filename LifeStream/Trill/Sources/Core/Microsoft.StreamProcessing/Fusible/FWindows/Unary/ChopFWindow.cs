namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    public class ChopFWindow<TPayload> : UnaryFWindow<TPayload, TPayload>
    {
        private long _sync;
        private long _other;
        private TPayload _payload;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="input"></param>
        /// <param name="period"></param>
        public ChopFWindow(FWindowable<TPayload> input, long period)
            : base(input, input.Size, period, input.Offset, period)
        {
            Invariant.IsTrue(input.Period % period == 0, "Input period must be a multiple of chop period");
            _sync = -2;
            _other = -1;
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
            _BV.Set();

            var period = Period;
            var payload = Payload.Data;
            var payloadOffset = Payload.Offset;
            var syncTime = SyncTime;
            var sync = Sync.Data;
            var syncOffset = Sync.Offset;
            var other = Other.Data;
            var otherOffset = Other.Offset;
            var bvOffset = BV.Offset;
            var length = Length;

            int i = 0;
            unsafe
            {
                fixed (long* bv = BV.Data)
                {
                    for (; i < length && syncTime < _other; i++)
                    {
                        sync[syncOffset + i] = syncTime;
                        payload[payloadOffset + i] = _payload;
                        _sync += period;
                        var bi = bvOffset + i;
                        bv[bi >> 6] &= ~(1L << (bi & 0x3f));
                        syncTime += period;
                        other[otherOffset + i] = syncTime;
                    }
                }
            }

            if (i < length)
            {
                var len = Input.Compute();
                syncTime = Input.SyncTime - Input.Size;
                var iperiod = Input.Period;
                int factor = (int) (iperiod / period);
                var ipayload = Input.Payload.Data;
                var ipayloadOffset = Input.Payload.Offset;
                var iother = Input.Other.Data;
                var iotherOffset = Input.Other.Offset;
                var ibvOffset = Input.BV.Offset;

                unsafe
                {
                    fixed (long* ibv = Input.BV.Data)
                    fixed (long* bv = BV.Data)
                    {
                        while (i < length)
                        {
                            var ibi = ibvOffset + i / factor;
                            if ((ibv[ibi >> 6] & (1L << (ibi & 0x3f))) == 0)
                            {
                                _payload = ipayload[ipayloadOffset + i / factor];
                                _sync = syncTime;
                                _other = iother[iotherOffset + i / factor];
                                for (; i < Length && _sync < _other; i++)
                                {
                                    payload[payloadOffset + i] = _payload;
                                    sync[syncOffset + i] = _sync;
                                    _sync += period;
                                    other[otherOffset + i] = _sync;
                                    syncTime += period;
                                    var bi = bvOffset + i;
                                    bv[bi >> 6] &= ~(1L << (bi & 0x3f));
                                }
                            }
                            else
                            {
                                syncTime += iperiod;
                                i += factor;
                            }
                        }
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
            tsync = SyncTime > tsync ? SyncTime : tsync;
            var ot = (_other / Size) * Size;
            var ret = true;
            if (ot <= tsync)
            {
                ret &= Input.Slide(tsync);
                if (ret)
                {
                    SyncTime = Input.SyncTime;
                }
            }

            return ret;
        }
    }
}