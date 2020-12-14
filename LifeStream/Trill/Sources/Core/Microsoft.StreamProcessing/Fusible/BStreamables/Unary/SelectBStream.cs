using System;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    public class SelectBStream<TInput, TOutput> : UnaryBStream<TInput, UnaryBState, TOutput>
    {
        private Func<TInput, TOutput> Select;

        /// <summary>
        /// 
        /// </summary>
        public SelectBStream(BStreamable<TInput> stream, Func<TInput, TOutput> select)
            : base(stream, stream.Period, stream.Offset)
        {
            Select = select;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override TOutput Selector(TInput payload) => Select(payload);

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