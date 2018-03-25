// 2018/03/25
// io.dbf
// DbfDataTruncateException.cs

using System;
using System.Runtime.Serialization;

namespace io.dbf
{
    public class DbfDataTruncateException : Exception
    {
        public DbfDataTruncateException(string message) : base(message)
        {
        }

        public DbfDataTruncateException(string message, Exception exc)
            : base(message, exc)
        {
        }

        public DbfDataTruncateException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}