namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    public class ClipFWindow<TPayload> : UnaryFWindow<TPayload, TPayload>
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="input"></param>
        public ClipFWindow(FWindowable<TPayload> input) : base(input, input.Size, input.Period, input.Offset, -1)
        {
            Invariant.IsPositive(input.Duration, "Input duration");
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
            Input.Payload.Copy(Payload);

            var period = Period;
            var ibvOffset = Input.BV.Offset;
            var other = Other.Data;
            var otherOffset = Other.Offset;
            var isyncOffset = Input.Sync.Offset;
            var syncTime = Input.Sync.Data[isyncOffset];
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
                                var otheri = otherOffset + previ;
                                other[otheri] = syncTime;
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

        private void PostCompute(int previ)
        {
            var period = Period;
            var ibvOffset = Input.BV.Offset;
            var other = Other.Data;
            var otherOffset = Other.Offset;
            var isyncOffset = Input.Sync.Offset;
            var syncTime = Input.Sync.Data[isyncOffset];
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
                            var otheri = otherOffset + previ;
                            other[otheri] = syncTime;
                            break;
                        }

                        syncTime += period;
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
            Input.Compute();
            PostCompute(previ);
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