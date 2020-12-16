using System;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TLeft"></typeparam>
    /// <typeparam name="TRight"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    public abstract class BinaryFWindow<TLeft, TRight, TResult> : FWindow<TResult>
    {
        /// <summary>
        /// 
        /// </summary>
        protected FWindowable<TLeft> Left;

        /// <summary>
        /// 
        /// </summary>
        protected FWindowable<TRight> Right;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="right"></param>
        /// <param name="size"></param>
        /// <param name="period"></param>
        /// <param name="offset"></param>
        /// <param name="left"></param>
        /// <param name="duration"></param>
        public BinaryFWindow(FWindowable<TLeft> left, FWindowable<TRight> right,
            long size, long period, long offset, long duration)
            : base(size, period, offset, duration)
        {
            Left = left;
            Right = right;
        }
    }
}