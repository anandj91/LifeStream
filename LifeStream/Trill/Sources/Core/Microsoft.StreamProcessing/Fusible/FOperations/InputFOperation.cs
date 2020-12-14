using System.Collections.Concurrent;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    public class FInputOperation<TPayload> : FOperation<TPayload>
    {
        private BlockingCollection<StreamMessage<Empty, TPayload>> _queue;
        private readonly long _period;
        private readonly long _offset;

        /// <summary>
        /// 
        /// </summary>
        public FInputOperation(long period, long offset)
        {
            _period = period;
            _offset = offset;
            _queue = new BlockingCollection<StreamMessage<Empty, TPayload>>(1);
        }

        /// <summary>
        /// 
        /// </summary>
        public long Size
        {
            get { return Period; }
        }

        /// <summary>
        /// 
        /// </summary>
        public long Period
        {
            get { return _period; }
        }

        /// <summary>
        /// 
        /// </summary>
        public long Offset
        {
            get { return _offset; }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public FWindowable<TPayload> Compile(long offset, long size)
        {
            return new InputFWindow<TPayload>(_queue, size, Period, Offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public BlockingCollection<StreamMessage<Empty, TPayload>> GetInputQueue()
        {
            return _queue;
        }
    }
}