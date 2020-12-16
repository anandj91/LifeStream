namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    public abstract class FWindow<TPayload> : FWindowable<TPayload>
    {
        /// <summary>
        /// 
        /// </summary>
        protected FSubWindow<TPayload> _Payload;

        /// <summary>
        /// 
        /// </summary>
        protected FSubWindow<long> _Sync;

        /// <summary>
        /// 
        /// </summary>
        protected FSubWindow<long> _Other;

        /// <summary>
        /// 
        /// </summary>
        protected BVFSubWindow _BV;

        /// <summary>
        /// 
        /// </summary>
        public virtual long SyncTime { get; set; }

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
        /// <param name="size"></param>
        /// <param name="period"></param>
        /// <param name="offset"></param>
        /// <param name="duration"></param>
        public FWindow(long size, long period, long offset, long duration)
        {
            SyncTime = StreamEvent.MinSyncTime;
            Duration = duration;
            Size = size;
            Period = period;
            Offset = offset;
            Length = (int) (Size / Period);
            _Payload = new FSubWindow<TPayload>(Length);
            _Sync = new FSubWindow<long>(Length);
            _Other = new FSubWindow<long>(Length);
            _BV = new BVFSubWindow(Length);
        }

        /// <summary>
        /// 
        /// </summary>
        public FSubWindowable<TPayload, TPayload> Payload
        {
            get { return _Payload; }
        }

        /// <summary>
        /// 
        /// </summary>
        public FSubWindowable<long, long> Sync
        {
            get { return _Sync; }
        }

        /// <summary>
        /// 
        /// </summary>
        public FSubWindowable<long, long> Other
        {
            get { return _Other; }
        }

        /// <summary>
        /// 
        /// </summary>
        public FSubWindowable<long, bool> BV
        {
            get { return _BV; }
        }

        /// <summary>
        /// 
        /// </summary>
        public bool Init() => _Init();

        /// <summary>
        /// 
        /// </summary>
        protected abstract bool _Init();

        /// <summary>
        /// 
        /// </summary>
        public virtual int Compute() => _Compute();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected abstract int _Compute();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public virtual bool Slide(long tsync) => _Slide(tsync);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected abstract bool _Slide(long tsync);
    }
}