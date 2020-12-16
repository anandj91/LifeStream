namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    public class MulticastFOperation<TPayload> : UnaryFOperation<TPayload, TPayload>
    {
        private MulticastFWindow<TPayload> _iwindow;
        private bool _isCompiled;
        private long _offset;
        private long _size;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="input"></param>
        public MulticastFOperation(FOperation<TPayload> input) : base(input)
        {
            _isCompiled = false;
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
        public override FWindowable<TPayload> Compile(long offset, long size)
        {
            if (!_isCompiled)
            {
                _iwindow = new MulticastFWindow<TPayload>(Input.Compile(offset, size));
                _isCompiled = true;
                _offset = offset;
                _size = size;
            }

            Invariant.IsTrue(offset == _offset, "Multicast offsets should be same");
            Invariant.IsTrue(size == _size, "Multicast sizes should be same");
            return _iwindow;
        }
    }
}