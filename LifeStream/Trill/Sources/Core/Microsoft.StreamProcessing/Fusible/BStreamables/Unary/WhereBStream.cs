using System;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    public class WhereBStream<TPayload> : UnaryBStream<TPayload, UnaryBState, TPayload>
    {
        private Func<TPayload, bool> Filter;

        /// <summary>
        /// 
        /// </summary>
        public WhereBStream(BStreamable<TPayload> stream, Func<TPayload, bool> filter)
            : base(stream, stream.Period, stream.Offset)
        {
            Filter = filter;
        }

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
        protected override bool _IsReady(UnaryBState state)
        {
            return Stream.IsReady(state.i) && Filter(Stream.GetPayload(state.i));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override UnaryBState _Init()
        {
            return new UnaryBState(Stream.Init());
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override UnaryBState _SetInput(StreamMessage batch, UnaryBState state)
        {
            state.i = Stream.SetInput(batch, state.i);
            return state;
        }
    }
}