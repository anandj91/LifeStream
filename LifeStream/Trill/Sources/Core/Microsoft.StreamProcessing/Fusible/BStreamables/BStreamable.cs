namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    public interface BStreamable<TPayload>
    {
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
        public TPayload GetPayload(BState state);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public long GetSyncTime(BState state);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public long GetOtherTime(BState state);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool GetBV(BState state);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public int GetHash(BState state);

        /// <summary>
        /// 
        /// </summary>
        public void Next(BState state);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public long BeatCorrection(long t);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool IsDone(BState state);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="state"></param>
        /// <returns></returns>
        public bool IsReady(BState state);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public BState Init();

        /// <summary>
        /// 
        /// </summary>
        public BState SetInput(StreamMessage batch, BState state);
    }
}