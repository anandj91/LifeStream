namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    public class UnaryBState : BState
    {
        /// <summary>
        /// 
        /// </summary>
        public BState i;

        /// <summary>
        /// 
        /// </summary>
        public UnaryBState(BState i)
        {
            this.i = i;
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public abstract class UnaryBStream<TInput, TState, TOutput> : BStream<TState, TOutput> where TState : UnaryBState
    {
        /// <summary>
        /// 
        /// </summary>
        protected BStreamable<TInput> Stream;

        /// <summary>
        /// 
        /// </summary>
        public UnaryBStream(BStreamable<TInput> stream, long period, long offset) : base(period, offset)
        {
            Stream = stream;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override long _GetSyncTime(TState state) => Stream.GetSyncTime(state.i);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override long _GetOtherTime(TState state) => Stream.GetOtherTime(state.i);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override bool _IsDone(TState state) => Stream.IsDone(state.i);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override int _GetHash(TState state) => Stream.GetHash(state.i);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override bool _GetBV(TState state) => true;

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override TOutput _GetPayload(TState state) => Selector(Stream.GetPayload(state.i));

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected abstract TOutput Selector(TInput payload);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override void _Next(TState state)
        {
            Stream.Next(state.i);
            if (!IsDone(state) && Stream.IsReady(state.i))
            {
                ProcessNextItem(state);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected abstract void ProcessNextItem(TState state);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override TState _SetInput(StreamMessage batch, TState state)
        {
            state.i = Stream.SetInput(batch, state.i);
            return state;
        }
    }
}