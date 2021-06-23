// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

namespace Microsoft.Azure.WebJobs.Extensions.Kafka
{
    /// <summary>
    /// Defines the message compression type
    /// </summary>
    public enum MessageCompressionType
    {
        NotSet = -1,
        None = 0,
        Gzip = 1,
        Snappy = 2,
        Lz4 = 3,
        Zstd = 4
    }
}
