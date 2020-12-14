namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TLeft"></typeparam>
    /// <typeparam name="TRight"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    public abstract class BinaryFOperation<TLeft, TRight, TResult> : FOperation<TResult>
    {
        /// <summary>
        /// 
        /// </summary>
        protected FOperation<TLeft> Left;

        /// <summary>
        /// 
        /// </summary>
        protected FOperation<TRight> Right;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        public BinaryFOperation(FOperation<TLeft> left, FOperation<TRight> right)
        {
            Left = left;
            Right = right;
        }

        /// <summary>
        /// 
        /// </summary>
        public abstract long Size { get; }

        /// <summary>
        /// 
        /// </summary>
        public abstract long Period { get; }

        /// <summary>
        /// 
        /// </summary>
        public abstract long Offset { get; }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public abstract FWindowable<TResult> Compile(long offset, long size);
    }
}