using System;
using System.IO;

namespace CorrugatedIron.Comms.Sockets
{
    public class ArraySegmentStream : System.IO.Stream
    {
        public ArraySegmentStream(byte[] data)
        {
            _buffer = data;
            _length = 0;
            _position = 0;
            _origin = 0;
        }

        private readonly byte[] _buffer;

        private readonly int _origin;

        private int _position;

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return true; }
        }

        public override bool CanWrite
        {
            get { return true; }
        }

        public override void Flush()
        {
        }

        private int _length = 0;

        public override long Length
        {
            get { return _length; }
        }

        public override long Position
        {
            get
            {
                return _position;
            }
            set
            {
                _position = (int)value;
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var numberOfBytesToCopy = _length - _position;
            if (numberOfBytesToCopy > count)
            {
                numberOfBytesToCopy = count;
            }
            if (numberOfBytesToCopy <= 0)
            {
                return 0;
            }
            if (numberOfBytesToCopy <= 8)
            {
                var indexOfBytesToCopy = numberOfBytesToCopy;
                while (--indexOfBytesToCopy >= 0)
                {
                    buffer[offset + indexOfBytesToCopy] = _buffer[_position + indexOfBytesToCopy];
                }
            }
            else
            {
                Buffer.BlockCopy(_buffer, _position, buffer, offset, numberOfBytesToCopy);
            }
            _position += numberOfBytesToCopy;
            return numberOfBytesToCopy;
        }

        public override long Seek(long offset, System.IO.SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    {
                        var position = _origin + (int)offset;
                        _position = position;
                        break;
                    }
                case SeekOrigin.Current:
                    {
                        var position = _position + (int)offset;
                        _position = position;
                        break;
                    }
                case SeekOrigin.End:
                    {
                        var position = _length + (int)offset;
                        _position = position;
                        break;
                    }

            }
            return _position;
        }

        public override void SetLength(long value)
        {
            var length = _origin + (int)value;
            _length = length;
            if (_position > length)
            {
                _position = length;
            }
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            var length = _position + count;
            if (length > _length)
            {
                _length = length;
            }
            if (count <= 8 && buffer != _buffer)
            {
                var indexOfBytesToCopy = count;
                while (--indexOfBytesToCopy >= 0)
                {
                    _buffer[_position + indexOfBytesToCopy] = buffer[offset + indexOfBytesToCopy];
                }
            }
            else
            {
                Buffer.BlockCopy(buffer, offset, _buffer, _position, count);
            }
            _position = length;
        }
    }
}
