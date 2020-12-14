using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    public class AlterDurationFOperation<TPayload> : UnaryFOperation<TPayload, TPayload>
    {
        private Expression<Func<long, long, long>> _durationSelector;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="input"></param>
        /// <param name="durationSelector"></param>
        public AlterDurationFOperation(FOperation<TPayload> input, Expression<Func<long, long, long>> durationSelector)
            : base(input)
        {
            _durationSelector = durationSelector;
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
            return new AlterDurationFWindow<TPayload>(Input.Compile(offset, size), _durationSelector);
        }
    }
}