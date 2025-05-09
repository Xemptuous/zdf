// https://arrow.apache.org/docs/format/Columnar.html
const std = @import("std");

const IntMeta = struct {
    bitWidth: u8,
    isSigned: bool,
};

const MapMeta = struct {
    keysSorted: bool,
};

const PrecisionMeta = enum { Half, Single, Double };

const DecimalMeta = struct {
    bitWidth: u16,
    scale: u8,
    precision: u8,
};

const DateUnit = enum { Day, Millisecond };
const TimeUnit = enum { Second, Millisecond, Microsecond, Nanosecond };

const TimeMeta = struct {
    bitWidth: u8 = 32,
    unit: TimeUnit = .Millisecond,
};
const TimestampMeta = struct {
    unit: TimeUnit,
    timezone: ?[]const u8,
};

const IntervalUnit = enum { YearMonth, DayTime, MonthDayNano };

const UnionMeta = struct {
    mode: enum { Sparse, Dense },
    typeIds: ?[]const u8,
};

const DictionaryMeta = struct {
    id: i64,
    indexType: IntMeta,
    isOrdered: bool,
    kind: enum { DenseArray },
};

pub const DataType = union(enum) {
    Null,
    Int: IntMeta,
    FloatingPoint: PrecisionMeta,
    Binary,
    Utf8,
    Bool,
    Decimal: DecimalMeta,
    Date: DateUnit,
    Dictionary: DictionaryMeta,
    Time: TimeMeta,
    Timestamp: TimestampMeta,
    Interval: IntervalUnit,
    List,
    Struct,
    Union: UnionMeta,
    FixedSizeBinary: u32,
    FixedSizeList: u32,
    Map: MapMeta,
    Duration: TimeUnit,
    LargeBinary,
    LargeUtf8,
    LargeList,
    RunEndEncoded,
    BinaryView,
    Utf8View,
    ListView,
    LargeListView,

    pub fn sizePerValue(dtype: DataType) !usize {
        return switch (dtype) {
            .Int => |int| int.bitWidth / 8,
            .FloatingPoint => |prec| switch (prec) {
                .Half => 2, // f16
                .Single => 4, // f32
                .Double => 8, // f64
            },
            .Bool => 1,
            .Date => |unit| switch (unit) {
                .Day => 4, // date32
                .Millisecond => 8, // date64
            },
            .Time => |t| switch (t.bitWidth) {
                32 => 4,
                64 => 8,
                else => error.InvalidTimeUnit,
            },
            .Timestamp => 8, // always i64
            .Duration => 8, // always i64
            .Decimal => |d| switch (d.bitWidth) {
                128 => 16,
                256 => 32,
                else => error.UnsupportedDecimal,
            },
            .FixedSizeBinary => |width| width,
            else => error.VariableLengthOrNested,

            .Null => 0,
        };
    }

    pub fn toString(self: DataType) []const u8 {
        return switch (self) {
            .Null => "Null",
            .Int => "Int",
            .FloatingPoint => "FloatingPoint",
            .Binary => "Binary",
            .Utf8 => "Utf8",
            .Bool => "Bool",
            .Decimal => "Decimal",
            .Date => "Date",
            .Dictionary => "Dictionary",
            .Time => "Time",
            .Timestamp => "Timestamp",
            .Interval => "Interval",
            .List => "List",
            .Struct => "Struct",
            .Union => "Union",
            .FixedSizeBinary => "FixedSizeBinary",
            .FixedSizeList => "FixedSizeList",
            .Map => "Map",
            .Duration => "Duration",
            .LargeBinary => "LargeBinary",
            .LargeUtf8 => "LargeUtf8",
            .LargeList => "LargeList",
            .RunEndEncoded => "RunEndEncoded",
            .BinaryView => "BinaryView",
            .Utf8View => "Utf8View",
            .ListView => "ListView",
            .LargeListView => "LargeListView",
        };
    }
};

// https://arrow.apache.org/docs/cpp/api/datatype.html
pub const DataTag = enum {
    Null,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F16,
    F32,
    F64,
    Bool,
    String,
    LargeString,
    Binary,
    LargeBinary,
    Date32,
    Date64,
    Time32,
    Time64,
    Timestamp, // default ms no tz
    Duration,
    Decimal128,
    FixedSizeBinary,
    FixedSizeList,
    List,
    LargeList,
    Struct,
    Map,
    UnionDense,
    IntervalYearMonth,
    IntervalDayTime,
    IntervalMonthDayNano,
    RunEndEncoded,
    BinaryView,
    Utf8View,
    ListView,
    LargeListView,
    Dictionary, // use int32 and utf8

    pub fn toArrow(self: DataTag) DataType {
        return switch (self) {
            .Null => .Null,
            .I8 => .{ .Int = .{ .bitWidth = 8, .isSigned = true } },
            .I16 => .{ .Int = .{ .bitWidth = 16, .isSigned = true } },
            .I32 => .{ .Int = .{ .bitWidth = 32, .isSigned = true } },
            .I64 => .{ .Int = .{ .bitWidth = 64, .isSigned = true } },
            .U8 => .{ .Int = .{ .bitWidth = 8, .isSigned = false } },
            .U16 => .{ .Int = .{ .bitWidth = 16, .isSigned = false } },
            .U32 => .{ .Int = .{ .bitWidth = 32, .isSigned = false } },
            .U64 => .{ .Int = .{ .bitWidth = 64, .isSigned = false } },
            .F16 => .{ .FloatingPoint = .Half },
            .F32 => .{ .FloatingPoint = .Single },
            .F64 => .{ .FloatingPoint = .Double },
            .Bool => .Bool,
            .String => .Utf8,
            .LargeString => .LargeUtf8,
            .Binary => .Binary,
            .LargeBinary => .LargeBinary,
            .Date32 => .{ .Date = .Day },
            .Date64 => .{ .Date = .Millisecond },

            .Time32 => .{ .Time = .{ .bitWidth = 32, .unit = .Millisecond } },
            .Time64 => .{ .Time = .{ .bitWidth = 64, .unit = .Microsecond } },

            .Timestamp => .{ .Timestamp = .{
                .unit = .Millisecond,
                .timezone = null,
            } },
            .Duration => .{ .Duration = .Millisecond },

            .Decimal128 => .{ .Decimal = .{
                .bitWidth = 128,
                .precision = 10,
                .scale = 0,
            } },

            .FixedSizeBinary => .{ .FixedSizeBinary = 16 },
            .FixedSizeList => .{ .FixedSizeList = 4 },

            .List => .List,
            .LargeList => .LargeList,
            .Struct => .Struct,
            .Map => .{ .Map = .{ .keysSorted = false } },

            .UnionDense => .{ .Union = .{ .mode = .Dense, .typeIds = null } },

            .IntervalYearMonth => .{ .Interval = .YearMonth },
            .IntervalDayTime => .{ .Interval = .DayTime },
            .IntervalMonthDayNano => .{ .Interval = .MonthDayNano },

            .RunEndEncoded => .RunEndEncoded,
            .BinaryView => .BinaryView,
            .Utf8View => .Utf8View,
            .ListView => .ListView,
            .LargeListView => .LargeListView,

            .Dictionary => .{ .Dictionary = .{
                .id = 0,
                .indexType = .{ .bitWidth = 32, .isSigned = true },
                .isOrdered = false,
                .kind = .DenseArray,
            } },
        };
    }
};

pub fn nativeToArrow(t: type) !DataType {
    return switch (t) {
        i8, ?i8 => DataTag.I8.toArrow(),
        i16, ?i16 => DataTag.I16.toArrow(),
        i32, ?i32 => DataTag.I32.toArrow(),
        i64, ?i64 => DataTag.I64.toArrow(),
        u8, ?u8 => DataTag.U8.toArrow(),
        u16, ?u16 => DataTag.U16.toArrow(),
        u32, ?u32 => DataTag.U32.toArrow(),
        u64, ?u64 => DataTag.U64.toArrow(),
        f16, ?f16 => DataTag.F16.toArrow(),
        f32, ?f32 => DataTag.F32.toArrow(),
        f64, ?f64 => DataTag.F64.toArrow(),
        bool, ?bool => DataTag.Bool.toArrow(),
        []const u8, ?[]const u8 => DataTag.String.toArrow(),
        // TODO: implement the rest
        else => error.UnsupportedDataType,
    };
}

pub fn equal(_: DataType, _: DataType) bool {
    return true;
}
