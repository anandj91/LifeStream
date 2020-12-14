using Microsoft.StreamProcessing.Internal;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    public class OutputFWindow<TResult> : FWindowable<TResult>
    {
        private StreamMessage<Empty, TResult> _obatch;
        private bool hasBatchChanged;
        private FWindowable<TResult> _fwindow;

        /// <summary>
        /// 
        /// </summary>
        public OutputFWindow(FWindowable<TResult> fwindow)
        {
            _fwindow = fwindow;
            hasBatchChanged = false;
        }

        /// <summary>
        /// 
        /// </summary>
        public long SyncTime
        {
            get { return _fwindow.SyncTime; }
        }

        /// <summary>
        /// 
        /// </summary>
        public long Duration
        {
            get { return _fwindow.Duration; }
        }

        /// <summary>
        /// 
        /// </summary>
        public long Size
        {
            get { return _fwindow.Size; }
        }

        /// <summary>
        /// 
        /// </summary>
        public long Period
        {
            get { return _fwindow.Period; }
        }

        /// <summary>
        /// 
        /// </summary>
        public long Offset
        {
            get { return _fwindow.Offset; }
        }

        /// <summary>
        /// 
        /// </summary>
        public int Length
        {
            get { return _fwindow.Length; }
        }

        /// <summary>
        /// 
        /// </summary>
        public FSubWindowable<TResult, TResult> Payload
        {
            get { return _fwindow.Payload; }
        }

        /// <summary>
        /// 
        /// </summary>
        public FSubWindowable<long, long> Sync
        {
            get { return _fwindow.Sync; }
        }

        /// <summary>
        /// 
        /// </summary>
        public FSubWindowable<long, long> Other
        {
            get { return _fwindow.Other; }
        }

        /// <summary>
        /// 
        /// </summary>
        public FSubWindowable<long, bool> BV
        {
            get { return _fwindow.BV; }
        }

        private static void UpdateInput<B, T>(FSubWindowable<B, T> fsubwin, ColumnBatch<B> output)
        {
            if (fsubwin.isInput) output.col = fsubwin.Data;
        }

        private static void UpdateOutput<B, T>(FSubWindowable<B, T> fsubwin, ColumnBatch<B> output, int offset)
        {
            if (!fsubwin.isInput)
            {
                fsubwin.Data = output.col;
                fsubwin.Offset = offset;
                fsubwin.isOutput = true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void SetBatch(StreamMessage<Empty, TResult> obatch)
        {
            _obatch = obatch;
            hasBatchChanged = true;
            UpdateOutput(Payload, _obatch.payload, _obatch.Count);
            UpdateOutput(Sync, _obatch.vsync, _obatch.Count);
            UpdateOutput(Other, _obatch.vother, _obatch.Count);
            UpdateOutput(BV, _obatch.bitvector, _obatch.Count);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Init()
        {
            return _fwindow.Init();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public int Compute()
        {
            var len = _fwindow.Compute();
            if (hasBatchChanged)
            {
                UpdateInput(Payload, _obatch.payload);
                UpdateInput(Sync, _obatch.vsync);
                UpdateInput(Other, _obatch.vother);
                UpdateInput(BV, _obatch.bitvector);
                hasBatchChanged = false;
            }

            _obatch.Count += len;
            return len;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Slide(long tsync)
        {
            if (Payload.isOutput) Payload.Offset = _obatch.Count;
            if (Sync.isOutput) Sync.Offset = _obatch.Count;
            if (Other.isOutput) Other.Offset = _obatch.Count;
            if (BV.isOutput) BV.Offset = _obatch.Count;
            return _fwindow.Slide(tsync);
        }
    }
}