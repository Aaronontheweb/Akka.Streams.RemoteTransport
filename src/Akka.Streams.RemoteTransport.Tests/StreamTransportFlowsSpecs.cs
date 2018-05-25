using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.IO;
using Akka.Streams.Dsl;
using FluentAssertions;
using FsCheck;
using FsCheck.Xunit;
using Xunit;

namespace Akka.Streams.RemoteTransport.Tests
{
    public class StreamTransportFlowsSpecs : TestKit.Xunit2.TestKit
    {
        [Theory]
        [InlineData("foo", ByteOrder.BigEndian)]
        [InlineData("foo", ByteOrder.LittleEndian)]
        public async Task ShouldFrameMessage(string str, ByteOrder endianness)
        {
            var input = ByteString.FromString(str);

            var output = (await Source.Single(input).Via(StreamTransportFlows.Encode(byteOrder:endianness))
                .RunAggregate(new List<ByteString>(), (list, s) =>
                {
                    list.Add(s);
                    return list;
                }, Sys.Materializer())).Single();

            // length should be input + 4 byte header
            output.Count.Should().Be(input.Count + 4);
            var measuredLength = BitConverter.ToInt32(output.Slice(0, 4).ToArray(), 0);
            if (endianness == ByteOrder.BigEndian)
                measuredLength = StreamTransportFlows.SwapInt(measuredLength);
            measuredLength.Should().Be(input.Count);
        }

        [Theory]
        [InlineData("foo", ByteOrder.BigEndian)]
        [InlineData("foo", ByteOrder.LittleEndian)]
        public async Task ShouldUnFrameMessage(string str, ByteOrder endianness)
        {
            var input = ByteString.FromString(str);

            var output = (await Source.Single(input).Via(StreamTransportFlows.Encode(byteOrder: endianness))
                .Via(StreamTransportFlows.Decode(byteOrder:endianness))
                .RunAggregate(new List<ByteString>(), (list, s) =>
                {
                    list.Add(s);
                    return list;
                }, Sys.Materializer())).Single();

            // length should be same
            output.Count.Should().Be(input.Count);
            output.Should().BeEquivalentTo(input);
        }
    }
}
