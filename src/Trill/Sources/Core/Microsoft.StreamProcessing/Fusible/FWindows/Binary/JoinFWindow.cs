namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TLeft"></typeparam>
    /// <typeparam name="TRight"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    public class JoinFWindow<TLeft, TRight, TResult> : BinaryFWindow<TLeft, TRight, TResult>
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="output"></param>
        public delegate void Joiner(TLeft left, TRight right, out TResult output);

        private Joiner _joiner;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="joiner"></param>
        public JoinFWindow(FWindowable<TLeft> left, FWindowable<TRight> right, Joiner joiner)
            : base(left, right, left.Size, left.Period, left.Offset, left.Duration)
        {
            Invariant.IsTrue(right.Offset == left.Offset, "Left offset must match to right offset");
            Invariant.IsTrue(right.Period % left.Period == 0, "Right period must be a multiple of left period");
            Invariant.IsTrue(right.Size == left.Size, "Left size must match to right size");
            Invariant.IsTrue(right.Period == right.Duration, "Right: period and duration must be same");
            Invariant.IsTrue(left.Period == left.Duration, "Left: period and duration must be same");

            _joiner = joiner;

            _Payload = new FSubWindow<TResult>(Length);
            _Sync = Left.Sync as FSubWindow<long>;
            _Other = Left.Other as FSubWindow<long>;
            _BV = new BVFSubWindow(Length);
        }

        /// <summary>
        /// 
        /// </summary>
        protected override bool _Init()
        {
            var ret = Left.Init() && Right.Init();
            while (ret && Left.SyncTime != Right.SyncTime)
            {
                if (Right.SyncTime < Left.SyncTime)
                {
                    ret &= Right.Slide(Left.SyncTime);
                }
                else
                {
                    ret &= Left.Slide(Right.SyncTime);
                }
            }

            SyncTime = Left.SyncTime;
            return ret;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override int _Compute()
        {
            var llen = Left.Compute();
            var rlen = Right.Compute();
            int factor = Left.Length / Right.Length;

            var lpayload = Left.Payload.Data;
            var lpayloadOffset = Left.Payload.Offset;
            var rpayload = Right.Payload.Data;
            var rpayloadOffset = Right.Payload.Offset;
            var payload = Payload.Data;
            var payloadOffset = Payload.Offset;

            var lbvOffset = Left.BV.Offset;
            var rbvOffset = Right.BV.Offset;
            var bvOffset = BV.Offset;

            var rlength = Right.Length;

            unsafe
            {
                fixed (long* lbv = Left.BV.Data)
                fixed (long* rbv = Right.BV.Data)
                fixed (long* bv = BV.Data)
                {
                    for (int r = 0; r < rlength; r++)
                    {
                        var rbi = rbvOffset + r;
                        for (int l = r * factor; l < (r + 1) * factor; l++)
                        {
                            var lbi = lbvOffset + l;
                            var bi = bvOffset + l;
                            if (((lbv[lbi >> 6] & (1L << (lbi & 0x3f))) == 0)
                                && ((rbv[rbi >> 6] & (1L << (rbi & 0x3f))) == 0))
                            {
                                var lp = lpayload[lpayloadOffset + l];
                                var rp = rpayload[rpayloadOffset + r];
                                var po = payloadOffset + l;
                                _joiner(lp, rp, out payload[po]);
                                bv[bi >> 6] &= ~(1L << (bi & 0x3f));
                            }
                            else
                            {
                                bv[bi >> 6] |= (1L << (bi & 0x3f));
                            }
                        }
                    }
                }
            }

            SyncTime += Size;
            return Length;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override bool _Slide(long tsync)
        {
            bool ret = Left.Slide(tsync);
            ret &= Right.Slide(tsync);
            while (ret && Left.SyncTime != Right.SyncTime)
            {
                if (Right.SyncTime < Left.SyncTime)
                {
                    ret &= Right.Slide(Left.SyncTime);
                }
                else
                {
                    ret &= Left.Slide(Right.SyncTime);
                }
            }

            SyncTime = Left.SyncTime;
            return ret;
        }
    }
}