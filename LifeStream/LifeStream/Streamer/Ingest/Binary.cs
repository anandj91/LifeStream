using System;
using System.IO;

namespace Streamer.Ingest
{
    public class Binary
    {
        private byte[] bytes;
        private int off;
        private int lim;
        private int idx;

        public Binary(byte[] bytes) : this(bytes, 0, bytes.Length, 0)
        {
        }

        public Binary(byte[] bytes, int off) : this(bytes, off, bytes.Length, 0)
        {
        }

        public Binary(byte[] bytes, int off, int lim) : this(bytes, off, lim, 0)
        {
        }

        public Binary(byte[] bytes, int off, int lim, int idx)
        {
            this.bytes = bytes;
            this.off = off;
            this.lim = lim;
            this.idx = idx;
        }

        public Binary Split(int off, int lim)
        {
            return new Binary(this.bytes, off, lim);
        }

        public Binary Next(int lim)
        {
            return new Binary(this.bytes, this.GetGlobalIdx(), lim);
        }

        public MemoryStream Stream()
        {
            return new MemoryStream(this.bytes, off, lim, false);
        }

        public UInt32 NextUInt32(bool IsLittle = true)
        {
            int size = 4;
            if (IsLittle)
            {
                Array.Reverse(bytes, GetGlobalIdx(), size);
            }

            var num = BitConverter.ToUInt32(bytes, GetGlobalIdx());
            idx += size;
            return num;
        }

        public UInt16 NextUInt16(bool IsLittle = true)
        {
            int size = 2;
            if (IsLittle)
            {
                Array.Reverse(bytes, GetGlobalIdx(), size);
            }

            var num = BitConverter.ToUInt16(bytes, GetGlobalIdx());
            idx += size;
            return num;
        }

        public Int64 NextInt64(bool IsLittle = true)
        {
            int size = 8;
            if (IsLittle)
            {
                Array.Reverse(bytes, GetGlobalIdx(), size);
            }

            var num = BitConverter.ToInt64(bytes, GetGlobalIdx());
            idx += size;
            return num;
        }

        public Int32 NextInt32(bool IsLittle = true)
        {
            int size = 4;
            if (IsLittle)
            {
                Array.Reverse(bytes, GetGlobalIdx(), size);
            }

            var num = BitConverter.ToInt32(bytes, GetGlobalIdx());
            idx += size;
            return num;
        }

        public Int16 NextInt16(bool IsLittle = true)
        {
            int size = 2;
            if (IsLittle)
            {
                Array.Reverse(bytes, GetGlobalIdx(), size);
            }

            var num = BitConverter.ToInt16(bytes, GetGlobalIdx());
            idx += size;
            return num;
        }

        public SByte NextInt8(bool IsLittle = true)
        {
            var num = (sbyte) bytes[GetGlobalIdx()];
            idx++;
            return num;
        }

        public Byte NextByte(bool IsLittle = true)
        {
            var num = bytes[GetGlobalIdx()];
            idx++;
            return num;
        }

        public int GetIdx()
        {
            return this.idx;
        }

        private int GetGlobalIdx() => this.idx + this.off;

        public void Seek(int idx)
        {
            this.idx = idx;
        }

        public int Length()
        {
            return this.bytes.Length;
        }
    }
}