const std = @import("std");
const series = @import("series.zig");
const datatype = @import("datatype.zig");
const Series = series.Series;
const TypedSeries = series.TypedSeries;
const DataType = datatype.DataType;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    {
        const idSlice: []?i32 = try allocator.alloc(?i32, 5);
        idSlice[0] = 1;
        idSlice[1] = 2;
        idSlice[2] = 23550250;
        idSlice[3] = null;
        idSlice[4] = 94380;
        defer allocator.free(idSlice);
        var s = try Series(.I32).init(allocator, .{
            .name = "id",
            .nullable = true,
            .len = 5,
        });
        defer s.deinit();
        try s.addSlice(idSlice);
    }
    _ = gpa.detectLeaks();
}

test "addSliceI32" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const idSlice: []?i32 = try allocator.alloc(?i32, 5);
    defer allocator.free(idSlice);
    idSlice[0] = 0;
    idSlice[1] = 2;
    idSlice[2] = 23550250;
    idSlice[3] = null;
    idSlice[4] = 94380;

    var s = try Series(.I32).init(allocator, .{
        .name = "id",
        .nullable = true,
        .len = 5,
    });
    defer s.deinit();
    try s.addSlice(idSlice);
    const expectedValues = [_]u8{ 0, 0, 0, 0, 2, 0, 0, 0, 42, 89, 103, 1, 170, 170, 170, 170, 172, 112, 1, 0 };
    const expectedValidity = [_]u1{ 1, 1, 1, 0, 1 };

    const vMap = try s.expandValidityBitmapToU1();
    defer allocator.free(vMap);

    try std.testing.expect(std.mem.eql(u8, &expectedValues, s.values.items));
    try std.testing.expect(std.mem.eql(u1, &expectedValidity, vMap));
}
