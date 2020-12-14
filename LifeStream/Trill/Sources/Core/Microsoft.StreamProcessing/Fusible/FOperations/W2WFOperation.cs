using System;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    public class W2WFOperation<TPayload, TResult> : UnaryFOperation<TPayload, TResult>
    {
        private long _window;
        private Action<int, TPayload[], int, TResult[], int> _transform;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="input"></param>
        /// <param name="window"></param>
        /// <param name="transform"></param>
        public W2WFOperation(FOperation<TPayload> input, long window,
            Action<int, TPayload[], int, TResult[], int> transform) : base(input)
        {
            _window = window;
            _transform = transform;
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
            return new W2WFWindow<TPayload, TResult>(Input.Compile(offset, size), _window, _transform);
        }
    }
}