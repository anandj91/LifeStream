namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    public class AlterPeriodFOperation<TPayload> : UnaryFOperation<TPayload, TPayload>
    {
        private long _period;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="input"></param>
        /// <param name="period"></param>
        public AlterPeriodFOperation(FOperation<TPayload> input, long period) : base(input)
        {
            _period = period;
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
            get { return _period; }
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
        public override FWindowable<TPayload> Compile(long offset, long size)
        {
            return new AlterPeriodFWindow<TPayload>(Input.Compile(offset, size), _period);
        }
    }
}