namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    public class InputBState<TPayload> : BState
    {
        /// <summary>
        /// 
        /// </summary>
        public int Idx;

        /// <summary>
        /// 
        /// </summary>
        public StreamMessage<Empty, TPayload> Batch;

        /// <summary>
        /// 
        /// </summary>
        public InputBState(StreamMessage<Empty, TPayload> batch, int idx)
        {
            this.Batch = batch;
            this.Idx = idx;
        }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    public class InputBStream<TPayload> : BStream<InputBState<TPayload>, TPayload>
    {
        /// <summary>
        /// 
        /// </summary>
        public InputBStream(long period, long offset) : base(period, offset)
        {
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override TPayload _GetPayload(InputBState<TPayload> state) => state.Batch.payload.col[state.Idx];

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override long _GetSyncTime(InputBState<TPayload> state) => state.Batch.vsync.col[state.Idx];

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override long _GetOtherTime(InputBState<TPayload> state) => state.Batch.vother.col[state.Idx];

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override bool _GetBV(InputBState<TPayload> state)
            => ((state.Batch.bitvector.col[state.Idx >> 6] & (1L << (state.Idx & 0x3f))) == 0);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override int _GetHash(InputBState<TPayload> state) => state.Batch.hash.col[state.Idx];

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override void _Next(InputBState<TPayload> state) => state.Idx++;

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override bool _IsDone(InputBState<TPayload> state)
            => (state.Idx >= state.Batch.Count || GetSyncTime(state) > StreamEvent.MaxSyncTime);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="state"></param>
        /// <returns></returns>
        protected override bool _IsReady(InputBState<TPayload> state)
            => state.Idx >= 0 && state.Idx < state.Batch.Count && _GetBV(state);

        /// <summary>
        /// 
        /// </summary>
        protected override InputBState<TPayload> _Init()
        {
            return new InputBState<TPayload>(null, -1);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="batch"></param>
        /// <param name="state"></param>
        /// <returns></returns>
        protected override InputBState<TPayload> _SetInput(StreamMessage batch, InputBState<TPayload> state)
        {
            state.Batch = batch as StreamMessage<Empty, TPayload>;
            state.Idx = 0;
            return state;
        }
    }
}