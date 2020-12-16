namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    public interface FOperation<TResult>
    {
        /// <summary>
        /// 
        /// </summary>
        public long Size { get; }

        /// <summary>
        /// 
        /// </summary>
        public long Period { get; }

        /// <summary>
        /// 
        /// </summary>
        public long Offset { get; }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public FWindowable<TResult> Compile(long offset, long size);
    }
}