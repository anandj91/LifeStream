namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TLeft"></typeparam>
    /// <typeparam name="TRight"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    public class JoinFOperation<TLeft, TRight, TResult> : BinaryFOperation<TLeft, TRight, TResult>
    {
        private JoinFWindow<TLeft, TRight, TResult>.Joiner _joiner;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="joiner"></param>
        public JoinFOperation(FOperation<TLeft> left, FOperation<TRight> right,
            JoinFWindow<TLeft, TRight, TResult>.Joiner joiner
        ) : base(left, right)
        {
            _joiner = joiner;
        }

        /// <summary>
        /// 
        /// </summary>
        public override long Size
        {
            get { return Utility.LCM(Left.Size, Right.Size); }
        }

        /// <summary>
        /// 
        /// </summary>
        public override long Period
        {
            get { return Left.Period; }
        }

        /// <summary>
        /// 
        /// </summary>
        public override long Offset
        {
            get { return Left.Offset; }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override FWindowable<TResult> Compile(long offset, long size)
        {
            var lwin = Left.Compile(offset, size);
            var rwin = Right.Compile(offset, size);
            Invariant.IsTrue(rwin.Period % lwin.Period == 0, "Right period must be a multiple of left period");
            Invariant.IsTrue(rwin.Offset == lwin.Offset, "Right offset must be equal to left offset");

            return new JoinFWindow<TLeft, TRight, TResult>(lwin, rwin, _joiner);
        }
    }
}