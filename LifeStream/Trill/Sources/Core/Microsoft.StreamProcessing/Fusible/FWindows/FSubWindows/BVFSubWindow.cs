namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// 
    /// </summary>
    public class BVFSubWindow : FSubWindowable<long, bool>
    {
        /// <summary>
        /// 
        /// </summary>
        public long[] Data { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int Length { get; }

        /// <summary>
        /// 
        /// </summary>
        public int Offset { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool isInput { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool isOutput { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="i"></param>
        public virtual bool this[int i] => ((Data[(Offset + i) >> 6] & (1L << ((Offset + i) & 0x3f))) == 0);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="output"></param>
        public void Copy(FSubWindowable<long, bool> output)
        {
            _Copy(output as BVFSubWindow);
        }

        /// <summary>
        /// 
        /// </summary>
        public void _Copy(BVFSubWindow output)
        {
            unsafe
            {
                fixed (long* ibv = Data)
                fixed (long* obv = output.Data)
                {
                    for (int i = 0; i < Length; i++)
                    {
                        var ibi = Offset + i;
                        var obi = output.Offset + i;
                        if ((ibv[ibi >> 6] & (1L << (ibi & 0x3f))) == 0)
                        {
                            obv[obi >> 6] &= ~(1L << (obi & 0x3f));
                        }
                        else
                        {
                            obv[obi >> 6] |= (1L << (obi & 0x3f));
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="length"></param>
        /// <param name="offset"></param>
        /// <param name="data"></param>
        public BVFSubWindow(int length, int offset, long[] data)
        {
            Length = length;
            Offset = offset;
            Data = data;
            for (int i = Length; i < data.Length * (1 << 6); i++)
            {
                Data[i >> 6] |= (1L << (i & 0x3f));
            }

            isInput = false;
            isOutput = false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="length"></param>
        public BVFSubWindow(int length) : this(length, 0, new long[(length >> 6) + 1])
        {
        }

        /// <summary>
        /// 
        /// </summary>
        public void Set()
        {
            if (isOutput)
            {
                for (int i = Offset; i < Offset + Length; i++)
                {
                    Data[i >> 6] |= (1L << (i & 0x3f));
                }
            }
            else
            {
                for (int i = 0; i < Data.Length; i++)
                {
                    Data[i] = ~0;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Unset()
        {
            if (isOutput)
            {
                for (int i = Offset; i < Offset + Length; i++)
                {
                    Data[i >> 6] &= ~(1L << (i & 0x3f));
                }
            }
            else
            {
                for (int i = 0; i < Data.Length; i++)
                {
                    Data[i] = 0;
                }
            }
        }
    }
}