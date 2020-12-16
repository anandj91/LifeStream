using System;
using System.Collections.Concurrent;
using System.IO;
using System.Runtime.Serialization;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    [DataContract]
    public sealed class FStartPipe<TPayload> : IStreamObserver<Empty, TPayload>
    {
        private IStreamable<Empty, TPayload> Source;
        private BlockingCollection<StreamMessage<Empty, TPayload>> _queue;
        internal FInputOperation<TPayload> _iop;
        internal long Period;
        internal long Offset;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="period"></param>
        /// <param name="offset"></param>
        public FStartPipe(IStreamable<Empty, TPayload> source, long period, long offset)
        {
            Source = source;
            Period = period;
            Offset = offset;
            _iop = new FInputOperation<TPayload>(period, offset);
            _queue = _iop.GetInputQueue();
            this.ClassId = Guid.NewGuid();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDisposable Connect()
        {
            return Source.Subscribe(Config.StreamScheduler.RegisterStreamObserver(this));
        }

        /// <summary>
        /// 
        /// </summary>
        public int CurrentlyBufferedOutputCount
        {
            get { return 0; }
        }

        /// <summary>
        /// 
        /// </summary>
        public int CurrentlyBufferedInputCount
        {
            get { return 0; }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="previous"></param>
        /// <exception cref="NotImplementedException"></exception>
        public void ProduceQueryPlan(PlanNode previous)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// 
        /// </summary>
        public void OnFlush()
        {
            _queue.CompleteAdding();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="stream"></param>
        /// <exception cref="NotImplementedException"></exception>
        public void Checkpoint(Stream stream)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="stream"></param>
        /// <exception cref="NotImplementedException"></exception>
        public void Restore(Stream stream)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        public void Reset()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="error"></param>
        public void OnError(Exception error)
        {
            _queue.CompleteAdding();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="batch"></param>
        public void OnNext(StreamMessage<Empty, TPayload> batch)
        {
            this._queue.Add(batch);
        }

        /// <summary>
        /// 
        /// </summary>
        public void OnCompleted()
        {
            _queue.CompleteAdding();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public FOperation<TPayload> GetFOP()
        {
            return _iop;
        }

        /// <summary>
        /// 
        /// </summary>
        public Guid ClassId { get; }
    }
}