namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    public class AlterPeriodFWindow<TPayload> : UnaryFWindow<TPayload, TPayload>
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="input"></param>
        /// <param name="period"></param>
        public AlterPeriodFWindow(FWindowable<TPayload> input, long period)
            : base(input, input.Size, period, input.Offset, period)
        {
            Invariant.IsPositive(Input.Duration, "Input duration");
            Invariant.IsTrue(period % Input.Period == 0, "Period must be a multiple of input period");
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
            _BV.Unset();

            var len = Input.Compute();
            var syncTime = Input.SyncTime - Input.Size;

            var period = Period;
            var payload = Payload.Data;
            var payloadOffset = Payload.Offset;
            var sync = Sync.Data;
            var syncOffset = Sync.Offset;
            var other = Other.Data;
            var otherOffset = Other.Offset;
            var bvOffset = BV.Offset;
            var iperiod = Input.Period;
            int factor = (int) (period / iperiod);
            var ipayload = Input.Payload.Data;
            var ipayloadOffset = Input.Payload.Offset;
            var ibvOffset = Input.BV.Offset;
            var length = Length;

            unsafe
            {
                fixed (long* ibv = Input.BV.Data)
                fixed (long* bv = BV.Data)
                {
                    for (int i = 0; i < length; i++)
                    {
                        var ibi = ibvOffset + i * factor;
                        if ((ibv[ibi >> 6] & (1L << (ibi & 0x3f))) == 0)
                        {
                            sync[syncOffset + i] = syncTime;
                            other[otherOffset + i] = syncTime + period;
                            payload[payloadOffset + i] = ipayload[ipayloadOffset + i * factor];
                        }
                        else
                        {
                            var bi = bvOffset + i;
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