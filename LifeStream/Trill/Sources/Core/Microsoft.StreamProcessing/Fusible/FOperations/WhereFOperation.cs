using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    public class WhereFOperation<TPayload> : UnaryFOperation<TPayload, TPayload>
    {
        private Expression<Func<TPayload, bool>> _filter;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="input"></param>
        /// <param name="filter"></param>
        public WhereFOperation(FOperation<TPayload> input, Expression<Func<TPayload, bool>> filter) : base(input)
        {
            _filter = filter;
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
            return new WhereFWindow<TPayload>(Input.Compile(offset, size), _filter);
        }
    }
}