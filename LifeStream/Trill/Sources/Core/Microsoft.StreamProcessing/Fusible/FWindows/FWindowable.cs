namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    public interface FWindowable<TPayload>
    {
        /// <summary>
        /// 
        /// </summary>
        public long SyncTime { get; }

        /// <summary>
        /// 
        /// </summary>
        public long Duration { get; }

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
        public int Length { get; }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public FSubWindowable<TPayload, TPayload> Payload { get; }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public FSubWindowable<long, long> Sync { get; }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public FSubWindowable<long, long> Other { get; }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public FSubWindowable<long, bool> BV { get; }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Init();

        /// <summary>
        /// 
        /// </summary>
        public int Compute();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Slide(long tsync);
    }
}