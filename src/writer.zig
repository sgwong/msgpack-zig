const std = @import("std");
const builtin = @import("builtin");
const native_endian = builtin.cpu.arch.endian();

pub fn MsgPackWriter(comptime WriterType: type) type {
    return struct {
        const Self = @This();

        const Options = struct {
            writeBytesSliceAsString: bool = true,
            writeEnumAsString: bool = false,
        };

        writer: WriterType,
        options: Options,

        pub fn init(writer: WriterType, options: Options) Self {
            return Self{
                .writer = writer,
                .options = options,
            };
        }

        fn writeAnyEndianCorrected(self: *Self, value: anytype) !void {
            try self.writeBytesEndianCorrected(std.mem.asBytes(&value));
        }

        fn writeBytesEndianCorrected(self: *Self, bytes: []const u8) !void {
            switch (native_endian) {
                .Big => try self.writer.writeAll(bytes),
                .Little => {
                    var i: isize = @as(isize, @intCast(bytes.len)) - 1;
                    while (i >= 0) : (i -= 1) {
                        _ = try self.writer.write(&.{bytes[@intCast(i)]});
                    }
                },
            }
        }

        pub fn writeNil(self: *Self) !void {
            _ = try self.writer.writeAll(&.{0xc0});
        }

        pub fn writeString(self: *Self, string: []const u8) !void {
            // length
            if (string.len <= 31) {
                std.debug.assert(@as(u8, @intCast(string.len)) & 0b00011111 == @as(u8, @intCast(string.len)));
                _ = try self.writer.writeAll(&.{@as(u8, @intCast(string.len)) | 0b10100000});
            } else if (string.len <= std.math.pow(usize, 2, 8) - 1) {
                _ = try self.writer.writeAll(&.{ 0xd9, @as(u8, @intCast(string.len)) });
            } else if (string.len <= std.math.pow(usize, 2, 16) - 1) {
                _ = try self.writer.writeAll(&.{0xda});
                _ = try self.writeAnyEndianCorrected(@as(u16, @intCast(string.len)));
            } else if (string.len <= std.math.pow(usize, 2, 32) - 1) {
                _ = try self.writer.writeAll(&.{0xdb});
                _ = try self.writeAnyEndianCorrected(@as(u32, @intCast(string.len)));
            } else {
                return error.StringTooLong;
            }

            // content
            _ = try self.writer.writeAll(string);
        }

        pub fn writeBytes(self: *Self, bytes: []const u8) !void {
            // length
            if (bytes.len <= std.math.pow(usize, 2, 8) - 1) {
                _ = try self.writer.writeAll(&.{ 0xc4, @as(u8, @intCast(bytes.len)) });
            } else if (bytes.len <= std.math.pow(usize, 2, 16) - 1) {
                _ = try self.writer.writeAll(&.{0xc5});
                _ = try self.writeAnyEndianCorrected(@as(u16, @intCast(bytes.len)));
            } else if (bytes.len <= std.math.pow(usize, 2, 32) - 1) {
                _ = try self.writer.writeAll(&.{0xc6});
                _ = try self.writeAnyEndianCorrected(@as(u32, @intCast(bytes.len)));
            } else {
                return error.BytesTooLong;
            }

            // content
            _ = try self.writer.writeAll(bytes);
        }

        pub fn writeBool(self: *Self, value: bool) !void {
            _ = try self.writer.writeAll(&.{0xc2 + @as(u8, @intCast(@intFromBool(value)))});
        }

        pub fn writeInt(self: *Self, value: anytype) !void {
            const ValueType = @TypeOf(value);
            const typeInfo = @typeInfo(ValueType);

            // Special cases
            if (value >= 0 and value <= 127) {
                // positive fixint 0XXX XXXX
                _ = try self.writer.writeAll(&.{@as(u8, @intCast(value)) & 0b0111_1111});
                return;
            } else if (value >= -32 and value <= -1) {
                // negative fixint 111X XXXX
                _ = try self.writer.writeAll(&.{(@as(u8, @bitCast(@as(i8, @intCast(value)))) & 0b0001_1111) | 0b1110_0000});
                return;
            }

            //const bits = comptime std.mem.alignForward(@as(usize, typeInfo.Int.bits), 8);
            const bits = comptime std.mem.alignForward(usize, typeInfo.Int.bits, 8);
            const tag = if (typeInfo.Int.signedness == .signed) switch (bits) {
                8 => 0xd0,
                16 => 0xd1,
                32 => 0xd2,
                64 => 0xd3,
                else => unreachable,
            } else switch (bits) {
                8 => 0xcc,
                16 => 0xcd,
                32 => 0xce,
                64 => 0xcf,
                else => unreachable,
            };

            _ = try self.writer.writeAll(&.{tag});
            _ = try self.writeBytesEndianCorrected(std.mem.asBytes(&value));
        }

        pub fn writeFloat(self: *Self, value: anytype) !void {
            const ValueType = @TypeOf(value);
            const typeInfo = @typeInfo(ValueType);

            // const bits = comptime std.mem.alignForward(@as(usize, typeInfo.Float.bits), 8);
            const bits = comptime std.mem.alignForward(usize, typeInfo.Float.bits, 8);
            const tag = switch (bits) {
                32 => 0xca,
                64 => 0xcb,
                else => unreachable,
            };

            _ = try self.writer.writeAll(&.{tag});
            _ = try self.writeBytesEndianCorrected(std.mem.asBytes(&value));
        }

        pub fn writeExt(self: *Self, typ: i8, data: []const u8) !void {
            if (data.len == 1) {
                _ = try self.writer.writeAll(&.{ 0xd4, @as(u8, @bitCast(typ)), data[0] });
            } else if (data.len == 2) {
                _ = try self.writer.writeAll(&.{ 0xd5, @as(u8, @bitCast(typ)), data[0], data[1] });
            } else if (data.len == 4) {
                _ = try self.writer.writeAll(&.{ 0xd6, @as(u8, @bitCast(typ)) });
                _ = try self.writer.writeAll(data);
            } else if (data.len == 8) {
                _ = try self.writer.writeAll(&.{ 0xd7, @as(u8, @bitCast(typ)) });
                _ = try self.writer.writeAll(data);
            } else if (data.len == 16) {
                _ = try self.writer.writeAll(&.{ 0xd8, @as(u8, @bitCast(typ)) });
                _ = try self.writer.writeAll(data);
            } else if (data.len <= std.math.pow(usize, 2, 8) - 1) {
                _ = try self.writer.writeAll(&.{ 0xc7, @as(u8, @intCast(data.len)), @as(u8, @bitCast(typ)) });
                _ = try self.writer.writeAll(data);
            } else if (data.len <= std.math.pow(usize, 2, 16) - 1) {
                _ = try self.writer.writeAll(&.{0xc8});
                _ = try self.writeAnyEndianCorrected(@as(u16, @intCast(data.len)));
                _ = try self.writer.writeAll(&.{@as(u8, @bitCast(typ))});
                _ = try self.writer.writeAll(data);
            } else if (data.len <= std.math.pow(usize, 2, 32) - 1) {
                _ = try self.writer.writeAll(&.{0xc9});
                _ = try self.writeAnyEndianCorrected(@as(u32, @intCast(data.len)));
                _ = try self.writer.writeAll(&.{@as(u8, @bitCast(typ))});
                _ = try self.writer.writeAll(data);
            } else {
                return error.ArrayTooLong;
            }
        }

        pub fn writeTimestamp(self: *Self, unixSeconds: i64, nanoseconds: u32) !void {
            if ((unixSeconds >> 34) == 0) {
                const data64 = @as(u64, @bitCast((@as(i64, @intCast(nanoseconds)) << 34) | unixSeconds));
                if ((data64 & 0xffffffff00000000) == 0) {
                    // timestamp 32
                    _ = try self.writer.writeAll(&.{ 0xd6, 0xff });
                    try self.writeAnyEndianCorrected(@as(u32, @intCast(data64)));
                } else {
                    // timestamp 64
                    _ = try self.writer.writeAll(&.{ 0xd7, 0xff });
                    try self.writeAnyEndianCorrected(data64);
                }
            } else {
                // timestamp 96
                _ = try self.writer.writeAll(&.{ 0xc7, 12, 0xff });
                try self.writeAnyEndianCorrected(nanoseconds);
                try self.writeAnyEndianCorrected(unixSeconds);
            }
        }

        pub fn beginArray(self: *Self, len: usize) !void {
            if (len <= 15) {
                _ = try self.writer.writeAll(&.{@as(u8, @intCast(len)) | 0b1001_0000});
            } else if (len <= std.math.pow(usize, 2, 16) - 1) {
                _ = try self.writer.writeAll(&.{0xdc});
                const len16: u16 = @intCast(len);
                _ = try self.writeBytesEndianCorrected(std.mem.asBytes(&len16));
            } else if (len <= std.math.pow(usize, 2, 32) - 1) {
                _ = try self.writer.writeAll(&.{0xdd});
                const len32: u16 = @intCast(len);
                _ = try self.writeBytesEndianCorrected(std.mem.asBytes(&len32));
            } else {
                return error.ArrayTooLong;
            }
        }

        pub fn beginMap(self: *Self, len: usize) !void {
            if (len <= 15) {
                _ = try self.writer.writeAll(&.{@as(u8, @intCast(len)) | 0b1000_0000});
            } else if (len <= std.math.pow(usize, 2, 16) - 1) {
                _ = try self.writer.writeAll(&.{0xde});
                const len16: u16 = @intCast(len);
                _ = try self.writeBytesEndianCorrected(std.mem.asBytes(&len16));
            } else if (len <= std.math.pow(usize, 2, 32) - 1) {
                _ = try self.writer.writeAll(&.{0xdf});
                const len32: u16 = @intCast(len);
                _ = try self.writeBytesEndianCorrected(std.mem.asBytes(&len32));
            } else {
                return error.ArrayTooLong;
            }
        }

        pub fn writeAny(self: *Self, value: anytype) !void {
            try self.writeAnyPtr(&value);
        }

        pub fn writeAnyPtr(self: *Self, value: anytype) !void {
            const ptrInfo = @typeInfo(@TypeOf(value));
            if (ptrInfo != .Pointer) {
                @compileError("Parameter 'value' has to be a pointer but is " ++ @typeName(@TypeOf(value)));
            }
            const ValueType = ptrInfo.Pointer.child;
            const typeInfo = @typeInfo(ValueType);

            // special case: string ([]const u8)
            if (ValueType == []const u8 or ValueType == []u8) {
                if (self.options.writeBytesSliceAsString) {
                    try self.writeString(value.*);
                } else {
                    try self.writeBytes(value.*);
                }
                return;
            }
            switch (typeInfo) {
                .Null => try self.writeNil(),
                .Int, .ComptimeInt => try self.writeInt(value.*),
                .Float => try self.writeFloat(value.*),
                .Bool => try self.writeBool(value.*),

                .Enum => {
                    if (self.options.writeEnumAsString) {
                        try self.writeString(@tagName(value.*));
                    } else {
                        try self.writeInt(@intFromEnum(value.*));
                    }
                },
                .Optional => {
                    if (value.*) |unwrap_value| {
                        try self.writeAny(unwrap_value);
                    } else {
                        try self.writeNil();
                    }
                },
                .Struct => {
                    if (comptime std.meta.trait.hasFn("msgPackWrite")(ValueType)) {
                        return try value.msgPackWrite(self);
                    }

                    try self.beginMap(typeInfo.Struct.fields.len);
                    inline for (typeInfo.Struct.fields) |field| {
                        try self.writeString(field.name);
                        try self.writeAny(@field(value, field.name));
                    }
                },

                .Pointer => {
                    switch (typeInfo.Pointer.size) {
                        .Slice => {
                            try self.beginArray(value.*.len);
                            for (value.*) |*v| {
                                try self.writeAnyPtr(v);
                            }
                        },

                        else => {
                            std.log.err("Failed to write value of type {s} ({})", .{ @typeName(ValueType), typeInfo });
                            return error.MsgPackWriteError;
                        },
                    }
                },

                else => {
                    std.log.err("Failed to write value of type {s} ({})", .{ @typeName(ValueType), typeInfo });
                    return error.MsgPackWriteError;
                },
            }
        }
        pub fn writeJson(self: *Self, json: std.json.Value) anyerror!void {
            switch (json) {
                .null => try self.writeNil(),
                .bool => |value| try self.writeBool(value),
                .integer => |value| try self.writeInt(value),
                .float => |value| try self.writeFloat(value),
                .number_string => |value| {
                    _ = value;
                },
                .string => |value| try self.writeString(value),
                .array => |array| {
                    try self.beginArray(array.items.len);

                    for (array.items) |valueJson| {
                        try self.writeJson(valueJson);
                    }
                },
                .object => |object| {
                    try self.beginMap(object.count());

                    var iter = object.iterator();
                    while (iter.next()) |mapEntry| {
                        try self.writeString(mapEntry.key_ptr.*);
                        try self.writeJson(mapEntry.value_ptr.*);
                    }
                },
            }
        }
    };
}

pub fn msgPackWriter(writer: anytype, options: MsgPackWriter(@TypeOf(writer)).Options) MsgPackWriter(@TypeOf(writer)) {
    return MsgPackWriter(@TypeOf(writer)).init(writer, options);
}
