namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    public class ClipFOperation<TPayload> : UnaryFOperation<TPayload, TPayload>
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="input"></param>
        public ClipFOperation(FOperation<TPayload> input) : base(input)
        {
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
        public override FWindowable<TPayload> Compile(long offset, long size)
        {
            return new ClipFWindow<TPayload>(Input.Compile(offset, size));
        }
    }
}