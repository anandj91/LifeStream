namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    public abstract class UnaryFOperation<TPayload, TResult> : FOperation<TResult>
    {
        /// <summary>
        /// 
        /// </summary>
        protected FOperation<TPayload> Input;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="input"></param>
        public UnaryFOperation(FOperation<TPayload> input)
        {
            Input = input;
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