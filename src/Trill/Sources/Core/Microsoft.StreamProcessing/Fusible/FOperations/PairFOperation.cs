namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    public class PairFOperation<TPayload, TResult> : UnaryFOperation<TPayload, TResult>
    {
        private PairFWindow<TPayload, TResult>.Joiner _joiner;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="input"></param>
        /// <param name="joiner"></param>
        public PairFOperation(FOperation<TPayload> input, PairFWindow<TPayload, TResult>.Joiner joiner)
            : base(input)
        {
            _joiner = joiner;
        }

        /// <summary>
        /// 
        /// </summary>
        public override long Size
        {
            get { return Input.Size; }
        }

        /// <summary>
        /// 
        /// </summary>
        public override long Period
        {
            get { return Input.Period; }
        }

        /// <summary>
        /// 
        /// </summary>
        public override long Offset
        {
            get { return Input.Offset; }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        public override FWindowable<TResult> Compile(long offset, long size)
        {
            return new PairFWindow<TPayload, TResult>(Input.Compile(offset, size), _joiner);
        }
    }
}