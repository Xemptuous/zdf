// https://arrow.apache.org/docs/format/Intro.html
const std = @import("std");
const Allocator = std.mem.Allocator;

const datatype = @import("datatype.zig");
const DataType = datatype.DataType;
const DataTag = datatype.DataTag;

pub const SeriesOptions = struct {
    name: []const u8 = "",
    len: usize = 0,
    nullable: bool = false,
};

pub fn Series(comptime T: DataTag) type {
    return struct {
        const Self = @This();

        gpa: Allocator,
        name: []const u8,
        dtype: DataType,
        len: usize,
        nullable: bool = false,
        values: std.ArrayListUnmanaged(u8) = .{},
        validity: ?[]u8 = null, // bitmap
        offsets: ?[]i32 = null,
        // children: ?[]*Self = null,

        pub fn init(gpa: Allocator, comptime opts: SeriesOptions) !Self {
            const dtype = T.toArrow();
            const value_size: ?usize = DataType.sizePerValue(dtype) catch |err| switch (err) {
                error.VariableLengthOrNested => null,
                // invalid or unsupported
                else => return err,
            };

            var values = std.ArrayListUnmanaged(u8){};
            if (value_size) |s| {
                const n = opts.len * s;
                try values.ensureTotalCapacityPrecise(gpa, n);
                // pre-fill capacity
                values.items.len = n;
            }

            const validity = if (opts.nullable) blk: {
                const nBytes = comptime (opts.len + 7) / 8; // ceil
                break :blk try gpa.alloc(u8, nBytes);
            } else null;

            return .{
                .gpa = gpa,
                .name = opts.name,
                .len = opts.len,
                .nullable = opts.nullable,
                .dtype = dtype,
                .values = values,
                .validity = validity,
            };
        }

        pub fn deinit(self: *Self) void {
            self.values.deinit(self.gpa);
            if (self.validity) |v|
                self.gpa.free(v);
            if (self.offsets) |v|
                self.gpa.free(v);
        }

        pub fn addSlice(self: *Self, data: anytype) !void {
            // get the native zig type from the slice
            const Type = switch (@typeInfo(@TypeOf(data))) {
                .Array => |a| a.child,
                .Pointer => |p| p.child,
                else => @compileError("Expected Slice or Array"),
            };
            // base minus optional
            const baseType = switch (@typeInfo(Type)) {
                .Optional => |o| o.child,
                else => Type,
            };

            const isOptional = @typeInfo(Type) == .Optional;

            // ensure nullability flag + type is compatible
            if (self.nullable and !isOptional)
                return error.ExpectedOptionalForNullableSeries;
            if (!self.nullable and isOptional)
                return error.UnexpectedOptionalForNonNullableSeries;

            const dtype = try datatype.nativeToArrow(Type);

            // TODO: implement equality check
            // if (!datatype.equal(self.dtype, dtype))
            //     return error.TypeMismatch;

            switch (dtype) {
                .Int => |_| {
                    // const stride: u8 = meta.bitWidth / 8;

                    for (data, 0..) |d, i| {
                        // runtime checks
                        if (comptime isOptional) {
                            if (d == null) {
                                self.setValidityBit(i, false);
                                continue;
                            }
                            self.setValidityBit(i, true);
                        }
                        const value: baseType = if (comptime isOptional)
                            d orelse unreachable
                        else
                            d;

                        const bytes = std.mem.asBytes(&value);
                        std.mem.copyForwards(
                            u8,
                            self.values.items[i * @sizeOf(baseType) ..],
                            bytes,
                        );
                    }
                },
                else => return error.UnsupportedDataType,
            }
        }

        /// Helper function for setting validity bits in the series' bitmap
        fn setValidityBit(self: *Self, idx: usize, value: bool) void {
            const index = idx / 8;
            const offset: u3 = @intCast(idx % 8);
            if (value)
                self.validity.?[index] |= @as(u8, 1) << offset
            else
                self.validity.?[index] &= ~(@as(u8, 1) << offset);
        }

        /// Helper function for getting validity bits in the series' bitmap
        fn getValidityBit(self: *Self, idx: usize) bool {
            const index = idx / 8;
            const offset: u3 = @intCast(idx % 8);
            return (self.validity.?[index] >> offset) & 1 == 1;
        }

        // Helper function to expand validity bitmap u8 to []u1
        pub fn expandValidityBitmapToU1(self: *Self) ![]u1 {
            const result = try self.gpa.alloc(u1, self.len);

            for (result, 0..) |_, i| {
                const byte = self.validity.?[i / 8];
                const offset: u3 = @intCast(i % 8);
                const res: u1 = @bitCast((byte >> offset) & 1 == 1);
                result[i] = res;
            }

            return result;
        }
    };
}
