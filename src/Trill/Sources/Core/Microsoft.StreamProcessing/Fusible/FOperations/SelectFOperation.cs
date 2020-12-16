namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    public class SelectFOperation<TPayload, TResult> : UnaryFOperation<TPayload, TResult>
    {
        private SelectFWindow<TPayload, TResult>.Selector _selector;

        /// <summary>
        /// 
        /// </summary>
        public SelectFOperation(FOperation<TPayload> input, SelectFWindow<TPayload, TResult>.Selector selector)
            : base(input)
        {
            _selector = selector;
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
        /// <returns></returns>
        public override FWindowable<TResult> Compile(long offset, long size)
        {
            return new SelectFWindow<TPayload,TResult>(Input.Compile(offset, size), _selector);
        }
    }
}