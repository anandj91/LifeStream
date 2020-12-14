namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    public class PairFWindow<TPayload, TResult> : UnaryFWindow<TPayload, TResult>
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="output"></param>
        public delegate void Joiner(TPayload left, TPayload right, out TResult output);

        private Joiner _joiner;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="input"></param>
        /// <param name="joiner"></param>
        public PairFWindow(FWindowable<TPayload> input, Joiner joiner)
            : base(input, input.Size, input.Period, input.Offset, -1)
        {
            Invariant.IsPositive(input.Duration, "Input duration");
            _joiner = joiner;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override bool _Init()
        {
            var ret = Input.Init();
            if (ret)
            {
                ret &= InitInput();
            }

            return ret;
        }

        private bool InitInput()
        {
            SyncTime = Input.SyncTime;
            Input.Compute();
            return Input.Slide(Input.SyncTime);
        }

        private int PreCompute()
        {
            Input.Sync.Copy(Sync);
            Input.BV.Copy(BV);

            var period = Period;
            var ipayload = Input.Payload.Data;
            var ipayloadOffset = Input.Payload.Offset;
            var payload = Payload.Data;
            var payloadOffset = Payload.Offset;
            var ibvOffset = Input.BV.Offset;
            var other = Other.Data;
            var otherOffset = Other.Offset;
            var isyncOffset = Input.Sync.Offset;
            long syncTime = -1;
            var length = Length;

            int previ = -1;
            unsafe
            {
                fixed (long* bv = Input.BV.Data)
                {
                    for (int i = 0; i < length; i++)
                    {
                        var ibi = ibvOffset + i;
                        if ((bv[ibi >> 6] & (1L << (ibi & 0x3f))) == 0)
                        {
                            if (previ != -1)
                            {
                                var prevpi = ipayloadOffset + previ;
                                var curpi = ipayloadOffset + i;
                                var pi = payloadOffset + previ;
                                var otheri = otherOffset + previ;
                                _joiner(ipayload[prevpi], ipayload[curpi], out payload[pi]);
                                other[otheri] = syncTime;
                            }
                            else
                            {
                                syncTime = Input.Sync.Data[isyncOffset + i];
                            }

                            previ = i;
                        }

                        syncTime += period;
                    }
                }
            }

            SyncTime = syncTime;

            return previ;
        }

        private void PostCompute(TPayload prevPayload, int previ)
        {
            var ipayload = Input.Payload.Data;
            var ipayloadOffset = Input.Payload.Offset;
            var payload = Payload.Data;
            var payloadOffset = Payload.Offset;
            var ibvOffset = Input.BV.Offset;
            var other = Other.Data;
            var otherOffset = Other.Offset;
            var isyncOffset = Input.Sync.Offset;
            var length = Length;
            unsafe
            {
                fixed (long* bv = Input.BV.Data)
                {
                    for (int i = 0; i < length; i++)
                    {
                        var ibi = ibvOffset + i;
                        if ((bv[ibi >> 6] & (1L << (ibi & 0x3f))) == 0)
                        {
                            var curpi = ipayloadOffset + i;
                            var pi = payloadOffset + previ;
                            _joiner(prevPayload, ipayload[curpi], out payload[pi]);
                            other[otherOffset + previ] = Input.Sync.Data[isyncOffset + i];
                            break;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override int _Compute()
        {
            int previ = PreCompute();
            var ipayload = Input.Payload.Data;
            var ipayloadOffset = Input.Payload.Offset;
            var prevPayload = ipayload[ipayloadOffset + previ];
            Input.Compute();
            PostCompute(prevPayload, previ);
            return Length;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tsync"></param>
        /// <returns></returns>
        protected override bool _Slide(long tsync)
        {
            var ret = true;
            if (tsync > SyncTime)
            {
                ret &= Input.Slide(tsync);
                ret &= InitInput();
            }
            else
            {
                ret &= Input.Slide(SyncTime + Size);
            }

            return ret;
        }
    }
}