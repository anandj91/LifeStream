using System;
using System.Collections.Concurrent;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    public class InputFWindow<TPayload> : FWindow<TPayload>
    {
        private BlockingCollection<StreamMessage<Empty, TPayload>> _queue;
        private StreamMessage<Empty, TPayload> _batch;
        private StreamMessage<Empty, TPayload> _prevBatch;
        private int Idx;
        private int Count;
        private long _syncTime;
        private bool isNewBatch;

        /// <summary>
        /// 
        /// </summary>
        public InputFWindow(BlockingCollection<StreamMessage<Empty, TPayload>> queue,
            long size, long period, long offset
        ) : base(size, period, offset, period)
        {
            _queue = queue;
            Payload.isInput = true;
            Sync.isInput = true;
            Other.isInput = true;
            BV.isInput = true;
            _syncTime = -1;
            isNewBatch = false;
            _prevBatch = null;
        }

        /// <summary>
        /// 
        /// </summary>
        ~InputFWindow()
        {
            CheckAndRelease(_prevBatch);
            CheckAndRelease(_batch);
        }

        /// <summary>
        /// 
        /// </summary>
        public override long SyncTime
        {
            get { return _syncTime; }
        }

        private int NextShift(long tsync)
        {
            if (_batch.vsync.col[Idx] == StreamEvent.InfinitySyncTime) return 1;

            int len = (int) Math.Min((tsync - _batch.vsync.col[Idx]) / Period, Count - Idx);
            int s = 0;
            while (len > 0 && _batch.vsync.col[Idx + s + len - 1] > tsync)
            {
                len /= 2;
                if (_batch.vsync.col[Idx + s + len - 1] < tsync)
                {
                    s += len;
                }
            }

            if (len < 0)
            {
                len = 0;
            }

            var shift = s + len;

            if (Idx + shift > 0 && _batch.vsync.col[Idx + shift - 1] == tsync)
            {
                return shift - 1;
            }
            else
            {
                return shift;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override bool _Slide(long tsync)
        {
            tsync = Math.Max((tsync / Size) * Size, _syncTime);
            var shift = NextShift(tsync);
            if (Idx + shift >= Count)
            {
                return false;
            }

            Idx += shift;
            _syncTime = _batch.vsync.col[Idx];
            return _syncTime < StreamEvent.InfinitySyncTime;
        }

        /// <summary>
        /// 
        /// </summary>
        protected override bool _Init()
        {
            try
            {
                if (_batch == null)
                {
                    var batch = _queue.Take();
                    SetBatch(batch);
                    _prevBatch = null;
                    _Payload.Data = _batch.payload.col;
                    _Sync.Data = _batch.vsync.col;
                    _Other.Data = _batch.vother.col;
                    _BV.Data = _batch.bitvector.col;
                    isNewBatch = false;
                    _syncTime = _batch.vsync.col[0];
                }
            }
            catch (InvalidOperationException)
            {
                return false;
            }

            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        protected override int _Compute()
        {
            var len = Math.Min(Length, Count - Idx);
            if (isNewBatch)
            {
                CheckAndRelease(_prevBatch);
                _prevBatch = null;
                _Payload.Data = _batch.payload.col;
                _Sync.Data = _batch.vsync.col;
                _Other.Data = _batch.vother.col;
                _BV.Data = _batch.bitvector.col;
                isNewBatch = false;
            }

            Payload.Offset = Idx;
            Sync.Offset = Idx;
            Other.Offset = Idx;
            BV.Offset = Idx;
            _syncTime += Size;
            return len;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tsync"></param>
        /// <returns></returns>
        public override bool Slide(long tsync)
        {
            while (!_Slide(tsync))
            {
                try
                {
                    var batch = _queue.Take();
                    SetBatch(batch);
                }
                catch (InvalidOperationException)
                {
                    return false;
                }
            }

            return true;
        }

        private void CheckAndRelease(StreamMessage<Empty, TPayload> batch)
        {
            if (batch != null)
            {
                batch.Release();
                batch.Return();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void SetBatch(StreamMessage<Empty, TPayload> batch)
        {
            _prevBatch = _batch;
            _batch = batch;
            Idx = 0;
            Count = batch.Count;
            isNewBatch = true;
        }
    }
}