namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    public class BinaryBState : BState
    {
        internal BState left;
        internal BState right;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        public BinaryBState(BState left, BState right)
        {
            this.left = left;
            this.right = right;
        }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TLeft"></typeparam>
    /// <typeparam name="TRight"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    /// <typeparam name="TState"></typeparam>
    public abstract class BinaryBStream<TLeft, TRight, TState, TResult>
        : BStream<TState, TResult> where TState : BinaryBState
    {
        /// <summary>
        /// 
        /// </summary>
        protected BStreamable<TLeft> Left;

        /// <summary>
        /// 
        /// </summary>
        protected BStreamable<TRight> Right;

        /// <summary>
        /// 
        /// </summary>
        public BinaryBStream(BStreamable<TLeft> left, BStreamable<TRight> right, long period, long offset)
            : base(period, offset)
        {
            Left = left;
            Right = right;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="state"></param>
        /// <returns></returns>
        protected override bool _GetBV(TState state) => true;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="state"></param>
        /// <returns></returns>
        protected override int _GetHash(TState state) => 0;

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override void _Next(TState state)
        {
            if (!Left.IsReady(state.left) || !Right.IsReady(state.right))
            {
                if (!Left.IsReady(state.left)) Left.Next(state.left);
                if (!Right.IsReady(state.right)) Right.Next(state.right);
            }
            else
            {
                if (Left.GetOtherTime(state.left) < Right.GetOtherTime(state.right))
                {
                    Left.Next(state.left);
                }
                else if (Left.GetOtherTime(state.left) >= Right.GetOtherTime(state.right))
                {
                    Right.Next(state.right);
                }
            }

            ProcessNextItem(state);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="state"></param>
        /// <returns></returns>
        protected abstract void ProcessNextItem(TState state);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override TState _SetInput(StreamMessage batch, TState state)
        {
            state.left = Left.SetInput(batch, state.left);
            state.right = Right.SetInput(batch, state.right);
            return state;
        }
    }
}