const std = @import("std");

const msgPackWriter = @import("writer.zig").msgPackWriter;
const msgPackReader = @import("reader.zig").msgPackReader;

const String = struct {
    value: []const u8,

    pub fn msgPackWrite(self: @This(), writer: anytype) !void {
        try writer.writeString(self.value);
    }

    pub fn msgPackRead(self: @This(), writer: anytype) !void {
        try writer.writeString(self.value);
    }
};

const Foo = struct {
    a: i64,
    b: []const u8,
    c: []const bool,
    d: Bar,
    e: String,
};

const Baz = enum {
    Eins,
    Zwei,
    Drei,
};

const Bar = struct {
    lol: f32,
    uiae: bool,
    baz: Baz,
};

pub fn main() anyerror!void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    var allocator = gpa.allocator();

    try testStuff(allocator);

    var encodingBuffer = try allocator.alloc(u8, 1024 * 1024);
    defer allocator.free(encodingBuffer);
    @memset(encodingBuffer, 0);

    var stream = std.io.fixedBufferStream(encodingBuffer);

    var fileContentBuffer = std.ArrayList(u8).init(allocator);
    defer fileContentBuffer.deinit();

    const cwd = try std.process.getCwdAlloc(allocator);
    defer allocator.free(cwd);
    const testInputPath = try std.fs.path.join(allocator, &[_][]const u8{ cwd, "tests" });
    defer allocator.free(testInputPath);
    std.log.info("{s}", .{testInputPath});

    var testDir = try std.fs.openIterableDirAbsolute(testInputPath, .{});
    defer testDir.close();

    // iterate over all files
    var iterator = testDir.iterate();
    var i: usize = 0;
    while (try iterator.next()) |entry| {
        defer i += 1;

        if (entry.kind != .File) continue;
        std.log.info("{s}", .{entry.name});

        var file = try testDir.dir.openFile(entry.name, .{ .mode = .read_only });
        var fileReader = file.reader();

        // read entire file
        fileContentBuffer.clearRetainingCapacity();
        try fileReader.readAllArrayList(&fileContentBuffer, std.math.maxInt(usize));

        var parser = std.json.Parser.init(allocator, .alloc_if_needed);
        defer parser.deinit();

        var valueTree: std.json.ValueTree = try parser.parse(fileContentBuffer.items);
        defer valueTree.deinit();

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        var arena_allocator = arena.allocator();

        // iterate over all test cases in one file
        for (valueTree.root.array.items) |testCaseJson| {
            const testObject = testCaseJson.object;
            const msgpackEncodingsJson = testObject.get("msgpack").?.array;

            // list of possible encodings for the value
            var possibleEncodings = try std.ArrayList([]const u8).initCapacity(arena_allocator, msgpackEncodingsJson.items.len);
            for (msgpackEncodingsJson.items) |encodingJson| {
                var encoding = std.ArrayList(u8).init(arena_allocator);
                var iter = std.mem.split(u8, encodingJson.string, "-");
                while (iter.next()) |byte| {
                    try encoding.append(try std.fmt.parseInt(u8, byte, 16));
                }
                try possibleEncodings.append(try encoding.toOwnedSlice());
            }

            var decodedEncodings = try std.ArrayList(std.json.Value).initCapacity(arena_allocator, msgpackEncodingsJson.items.len);
            for (possibleEncodings.items) |possibleEncoding| {
                //std.debug.print("READ: {}\n", .{std.fmt.fmtSliceHexLower(possibleEncoding)});
                var tempStream = std.io.fixedBufferStream(possibleEncoding);
                var reader = msgPackReader(tempStream.reader());
                const value = try reader.readJson(&arena);

                try decodedEncodings.append(value.root);

                tempStream = std.io.fixedBufferStream(possibleEncoding);
                reader = msgPackReader(tempStream.reader());
                const msgPackValue = try reader.readValue(&arena);
                std.debug.print("  TEST: ", .{});
                try msgPackValue.root.stringify(.{}, std.io.getStdErr().writer());
                std.debug.print("\n", .{});
            }

            // encode the value from the test
            stream.reset();
            var msgPack = msgPackWriter(stream.writer(), .{});
            const valueToEncode: std.json.Value = blk: {
                if (testObject.get("nil")) |value| {
                    try msgPack.writeJson(value);
                    break :blk value;
                } else if (testObject.get("bool")) |value| {
                    try msgPack.writeJson(value);
                    break :blk value;
                } else if (testObject.get("string")) |value| {
                    try msgPack.writeJson(value);
                    break :blk value;
                } else if (testObject.get("number")) |value| {
                    try msgPack.writeJson(value);
                    break :blk value;
                } else if (testObject.get("array")) |array| {
                    try msgPack.writeJson(array);
                    break :blk array;
                } else if (testObject.get("map")) |map| {
                    try msgPack.writeJson(map);
                    break :blk map;
                } else if (testObject.get("timestamp")) |timestampJson| {
                    const timestampArray = timestampJson.array.items;
                    const sec = timestampArray[0].integer;
                    const nsec = timestampArray[1].integer;
                    try msgPack.writeTimestamp(sec, @intCast(u32, nsec));
                    break :blk timestampJson;
                } else if (testObject.get("bignum")) |numberJson| {
                    switch (numberJson) {
                        .string => |stringValue| {
                            if (std.fmt.parseInt(i64, stringValue, 10)) |value| {
                                try msgPack.writeInt(value);
                            } else |_| {
                                try msgPack.writeInt(try std.fmt.parseInt(u64, stringValue, 10));
                            }
                        },

                        else => {
                            std.log.err("Failed to test number: {}", .{numberJson});
                        },
                    }
                    break :blk numberJson;
                } else if (testObject.get("binary")) |binaryJson| {
                    var encoding = std.ArrayList(u8).init(allocator);
                    defer encoding.deinit();
                    var iter = std.mem.tokenize(u8, binaryJson.string, "-");
                    while (iter.next()) |byte| {
                        try encoding.append(try std.fmt.parseInt(u8, byte, 16));
                    }
                    try msgPack.writeBytes(encoding.items);
                    break :blk binaryJson;
                } else if (testObject.get("ext")) |extJson| {
                    const extArray = extJson.array.items;
                    const typ = extArray[0].integer;
                    const bytes = extArray[1].string;
                    var encoding = std.ArrayList(u8).init(allocator);
                    defer encoding.deinit();
                    var iter = std.mem.tokenize(u8, bytes, "-");
                    while (iter.next()) |byte| {
                        try encoding.append(try std.fmt.parseInt(u8, byte, 16));
                    }
                    try msgPack.writeExt(@intCast(i8, typ), encoding.items);
                    break :blk extJson;
                } else {
                    return error.InvalidTestFile;
                }
            };

            { // Test decoding
                var valueString = std.ArrayList(u8).init(arena_allocator);
                var decodedString = std.ArrayList(u8).init(arena_allocator);

                try valueToEncode.jsonStringify(.{}, valueString.writer());

                // Compare value to all decoded values in decodedEncodings
                for (decodedEncodings.items, 0..) |decodedValue, k| {
                    decodedString.clearRetainingCapacity();
                    try decodedValue.jsonStringify(.{}, decodedString.writer());

                    // check if equal
                    if (!std.mem.eql(u8, valueString.items, decodedString.items)) {
                        std.debug.print("  DECODE FAIL: {} -> {s}, expected {s}\n", .{ std.fmt.fmtSliceHexUpper(possibleEncodings.items[k]), decodedString.items, valueString.items });
                    } else {
                        //std.debug.print("  DECODE OK  : {} -> {s}, expected {s}\n", .{ std.fmt.fmtSliceHexUpper(possibleEncodings.items[k]), decodedString.items, valueString.items });
                    }
                }
            }

            const myEncodedData = encodingBuffer[0..try stream.getPos()];

            // check if our encoded data matches any of the encodings in the test case
            var foundMatchingEncoding = false;
            for (possibleEncodings.items) |encoding| {
                if (std.mem.eql(u8, myEncodedData, encoding)) {
                    foundMatchingEncoding = true;
                    break;
                }
            }

            if (foundMatchingEncoding) {
                //std.debug.print("  ENCODE OK  : ", .{});
                //try testCaseJson.jsonStringify(.{}, std.io.getStdErr().writer());
                //std.debug.print("\n", .{});
            } else {
                std.debug.print("  ENCODE FAIL: ", .{});
                try testCaseJson.jsonStringify(.{}, std.io.getStdErr().writer());
                std.debug.print("\n", .{});

                std.log.info("our: {s}", .{myEncodedData});
                std.log.info("our: {}", .{std.fmt.fmtSliceHexLower(myEncodedData)});
                std.log.err("Encoding failed.", .{});
            }
        }
    }
}

pub fn testStuff(allocator: std.mem.Allocator) anyerror!void {
    var encodingBuffer = try allocator.alloc(u8, 1024 * 1024);
    defer allocator.free(encodingBuffer);
    @memset(encodingBuffer, 0);

    var stream = std.io.fixedBufferStream(encodingBuffer);

    var msgPack = msgPackWriter(stream.writer(), .{
        .writeBytesSliceAsString = true,
        .writeEnumAsString = false,
    });

    var foo = Foo{
        .a = 123,
        .b = "Hello",
        .c = &.{ true, false, false, true, true, false, true, true, false, false },
        .d = Bar{
            .lol = 123.456,
            .uiae = true,
            .baz = .Drei,
        },
        .e = String{ .value = "this is a String" },
    };

    const now_nano = std.time.nanoTimestamp();
    const now_sec = @intCast(i64, @divTrunc(now_nano, std.time.ns_per_s));
    const now_sec_nano = try std.math.mod(i128, now_nano, std.time.ns_per_s);
    try msgPack.writeTimestamp(now_sec, @intCast(u32, now_sec_nano));
    try msgPack.writeTimestamp(now_sec, 0);

    try msgPack.writeAny(foo);
    try msgPack.writeExt(72, std.mem.asBytes(&foo));
    try msgPack.writeExt(1, &.{0x12});
    try msgPack.writeExt(2, &.{ 0x34, 0x56 });
    try msgPack.writeExt(-1, &.{ 0x78, 0x9a, 0xbc, 0xde });
    try msgPack.beginArray(5);

    try msgPack.beginMap(3);
    std.debug.print("not suppport non string key map for now\n", .{});
    try msgPack.writeString("key1");
    //try msgPack.writeAny(@as(i64, 0));
    try msgPack.writeAny(@as(i64, 5));

    try msgPack.writeString("key2");
    //try msgPack.writeAny(@as(i64, 127));
    try msgPack.writeAny(@as(i64, -1));

    try msgPack.writeString("key3");
    //try msgPack.writeAny(@as(i64, -11));
    try msgPack.writeAny(@as(i64, -32));
    //--------end map

    try msgPack.writeAny(@as(u8, 123));
    try msgPack.writeAny(@as(i8, -123));

    try msgPack.writeAny(@as(u16, 456));
    try msgPack.writeAny(@as(i16, -789));
    //----end 5 array

    try msgPack.writeAny(@as(f32, 1.2345678987654321));
    try msgPack.writeAny(@as(f64, 1.2345678987654321));

    try msgPack.writeString("hello world");
    try msgPack.writeString("lol wassup? rtndui adtrn dutiarned trndutilrcdtugiaeduitarn");
    try msgPack.writeString(&.{ 0x12, 0x34, 0x56, 0x67, 0x89, 0xab, 0xcd, 0xef });

    try msgPack.writeBytes("hello world");
    try msgPack.writeBytes("lol wassup? rtndui adtrn dutiarned trndutilrcdtugiaeduitarn");
    try msgPack.writeBytes(&.{ 0x12, 0x34, 0x56, 0x67, 0x89, 0xab, 0xcd, 0xef });

    try msgPack.writeBool(true);
    try msgPack.writeBool(false);
    try msgPack.writeBool(true);
    try msgPack.writeBool(false);

    const writtenBuffer = encodingBuffer[0..try stream.getPos()];
    var i: usize = 0;
    while (true) : (i += 16) {
        if (i >= writtenBuffer.len) break;

        const line = writtenBuffer[i..std.math.min(writtenBuffer.len, i + 16)];

        var k: usize = 0;
        while (k < 16) : (k += 1) {
            if (k > 0) {
                if (@mod(k, 8) == 0) {
                    std.debug.print("  ", .{});
                }
            }
            if (k < line.len) {
                std.debug.print("{x:0>2} ", .{line[k]});
            } else {
                std.debug.print("   ", .{});
            }
        }
        std.debug.print("\t", .{});

        k = 0;
        while (k < 16) : (k += 1) {
            if (k > 0) {
                if (@mod(k, 8) == 0) {
                    std.debug.print("  ", .{});
                }
            }
            if (k < line.len) {
                std.debug.print("{:3} ", .{line[k]});
            } else {
                std.debug.print("    ", .{});
            }
        }
        std.debug.print("\t", .{});

        k = 0;
        while (k < 16) : (k += 1) {
            if (k >= line.len or line[k] == 0 or line[k] == 0xa or line[k] == 0xc) {
                std.debug.print(".", .{});
            } else {
                std.debug.print("{c}", .{line[k]});
            }
        }
        if (i > 0 and @mod(i, 16 * 4) == 0) std.debug.print("\n", .{});
        std.debug.print("\n", .{});
    }
    std.debug.print("\n", .{});

    var tempStream = std.io.fixedBufferStream(writtenBuffer);
    var reader = msgPackReader(tempStream.reader());
    var arena = std.heap.ArenaAllocator.init(allocator);
    while (true) {
        var value = reader.readValue(&arena) catch break;
        std.debug.print("value: ", .{});
        try value.root.stringify(.{}, std.io.getStdErr().writer());
        std.debug.print("\n ", .{});
    }
    std.debug.print("\n ", .{});
}
