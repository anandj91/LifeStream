using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    public class W2WFWindow<TPayload, TResult> : UnaryFWindow<TPayload, TResult>
    {
        private long _window;
        private Action<int, TPayload[], int, TResult[], int> _transform;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="input"></param>
        /// <param name="window"></param>
        /// <param name="transform"></param>
        public W2WFWindow(FWindowable<TPayload> input, long window,
            Action<int, TPayload[], int, TResult[], int> transform)
            : base(input, input.Size, input.Period, input.Offset, input.Duration)
        {
            Invariant.IsTrue(Input.Size % window == 0, "window size must be multiple of input size");
            _window = window;
            _transform = transform;
            _Sync = Input.Sync as FSubWindow<long>;
            _Other = Input.Other as FSubWindow<long>;
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
            var ipayload = Input.Payload.Data;
            var ipayloadOffset = Input.Payload.Offset;
            var payload = Payload.Data;
            var payloadOffset = Payload.Offset;
            var length = Length;

            var wlen = (int) (_window / Period);
            for (int i = 0; i < length; i += wlen)
            {
                _transform(ipayloadOffset + i, ipayload, payloadOffset + i, payload, wlen);
            }

            SyncTime = Input.SyncTime;
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