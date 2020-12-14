namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    public abstract class UnaryFWindow<TPayload, TResult> : FWindow<TResult>
    {
        /// <summary>
        /// 
        /// </summary>
        protected FWindowable<TPayload> Input;

        /// <summary>
        /// 
        /// </summary>
        public UnaryFWindow(FWindowable<TPayload> input, long size, long period, long offset, long duration)
            : base(size, period, offset, duration)
        {
            Input = input;
        }
    }
}