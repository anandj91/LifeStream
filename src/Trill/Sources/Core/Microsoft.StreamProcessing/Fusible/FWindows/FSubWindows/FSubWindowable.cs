namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    public interface FSubWindowable<B, T>
    {
        /// <summary>
        /// 
        /// </summary>
        public B[] Data { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int Length { get; }

        /// <summary>
        /// 
        /// </summary>
        public int Offset { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool isInput { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool isOutput { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public T this[int i] { get; }

        /// <summary>
        /// 
        /// </summary>
        public void Copy(FSubWindowable<B, T> output);
    }
}