namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    public class TumblingWindowBStream<TPayload> : UnaryBStream<TPayload, UnaryBState, TPayload>
    {
        /// <summary>
        /// 
        /// </summary>
        protected long Window;

        /// <summary>
        /// 
        /// </summary>
        public TumblingWindowBStream(BStreamable<TPayload> stream, long window, long offset)
            : base(stream, window, offset)
        {
            Window = window;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override long _GetSyncTime(UnaryBState state) => BeatCorrection(Stream.GetSyncTime(state.i));

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override long _GetOtherTime(UnaryBState state)
            => BeatCorrection(Stream.GetSyncTime(state.i)) + Period;

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override TPayload Selector(TPayload payload) => payload;

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override void ProcessNextItem(UnaryBState state)
        {
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="state"></param>
        /// <returns></returns>
        protected override bool _IsReady(UnaryBState state) => Stream.IsReady(state.i);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override UnaryBState _Init()
        {
            return new UnaryBState(Stream.Init());
        }
    }
}