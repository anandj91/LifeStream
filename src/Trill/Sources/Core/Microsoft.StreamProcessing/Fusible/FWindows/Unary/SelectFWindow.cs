namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    public class SelectFWindow<TPayload, TResult> : UnaryFWindow<TPayload, TResult>
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="ts"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        public delegate void Selector(long ts, TPayload input, out TResult output);

        private Selector _selector;

        /// <summary>
        /// 
        /// </summary>
        public SelectFWindow(FWindowable<TPayload> input, Selector selector)
            : base(input, input.Size, input.Period, input.Offset, input.Duration)
        {
            _selector = selector;
            _Payload = new FSubWindow<TResult>(Length);
            _Sync = Input.Sync as FSubWindow<long>;
            _Other = Input.Other as FSubWindow<long>;
            _BV = Input.BV as BVFSubWindow;
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

            var period = Period;
            var ipayload = Input.Payload.Data;
            var ipayloadOffset = Input.Payload.Offset;
            var opayload = Payload.Data;
            var opayloadOffset = Payload.Offset;
            var ibvOffset = Input.BV.Offset;
            var syncTime = Input.SyncTime - Input.Size;
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
                            var pi = ipayloadOffset + i;
                            var po = opayloadOffset + i;
                            _selector(syncTime, ipayload[pi], out opayload[po]);
                        }

                        syncTime += period;
                    }
                }
            }

            SyncTime = syncTime;

            return len;
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