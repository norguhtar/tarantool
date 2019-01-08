#!/usr/bin/env tarantool

package.path = "lua/?.lua;"..package.path

local tap = require('tap')
local common = require('serializer_test')

local function is_map(s)
    local b = string.byte(string.sub(s, 1, 1))
    return b >= 0x80 and b <= 0x8f or b == 0xde or b == 0xdf
end

local function is_array(s)
    local b = string.byte(string.sub(s, 1, 1))
    return b >= 0x90 and b <= 0x9f or b == 0xdc or b == 0xdd
end

local function test_offsets(test, s)
    test:plan(6)
    local arr1 = {1, 2, 3}
    local arr2 = {4, 5, 6}
    local dump = s.encode(arr1)..s.encode(arr2)
    test:is(dump:len(), 8, "length of part1 + part2")

    local a
    local offset = 1
    a, offset = s.decode(dump, offset)
    test:is_deeply(a, arr1, "decoded part1")
    test:is(offset, 5, "offset of part2")

    a, offset = s.decode(dump, offset)
    test:is_deeply(a, arr2, "decoded part2")
    test:is(offset, 9, "offset of end")

    test:ok(not pcall(s.decode, dump, offset), "invalid offset")
end

local function test_misc(test, s)
    test:plan(4)
    local ffi = require('ffi')
    local buffer = require('buffer')
    local buf = ffi.cast("const char *", "\x91\x01")
    local bufcopy = ffi.cast('const char *', buf)
    local bufend, result = s.ibuf_decode(buf)
    local st,e = pcall(s.ibuf_decode, buffer.ibuf().rpos)
    test:is(buf, bufcopy, "ibuf_decode argument is constant")
    test:is(buf + 2, bufend, 'ibuf_decode position')
    test:is_deeply(result, {1}, "ibuf_decode result")
    test:ok(not st and e:match("null"), "null ibuf")
end

local function test_check_array(test, s)
    local ffi = require('ffi')

    local good_cases = {
        {
            'fixarray',
            data = '\x94\x01\x02\x03\x04',
            exp_len = 4,
            exp_rewind = 1,
        },
        {
            'array 16',
            data = '\xdc\x00\x04\x01\x02\x03\x04',
            exp_len = 4,
            exp_rewind = 3,
        },
        {
            'array 32',
            data = '\xdd\x00\x00\x00\x04\x01\x02\x03\x04',
            exp_len = 4,
            exp_rewind = 5,
        },
    }

    local bad_cases = {
        {
            'fixmap',
            data = '\x80',
            exp_err = 'msgpack.check_array: wrong array header',
        },
        {
            'truncated array 16',
            data = '\xdc\x00',
            exp_err = 'msgpack.check_array: unexpected end of buffer',
        },
        {
            'truncated array 32',
            data = '\xdd\x00\x00\x00',
            exp_err = 'msgpack.check_array: unexpected end of buffer',
        },
        {
            'zero size buffer',
            data = '\x90',
            size = 0,
            exp_err = 'msgpack.check_array: unexpected end of buffer',
        },
        {
            'negative size buffer',
            data = '\x90',
            size = -1,
            exp_err = 'msgpack.check_array: unexpected end of buffer',
        },
    }

    local wrong_1_arg_not_cdata_err = 'expected cdata as 1 argument'
    local wrong_1_arg_err = "msgpack.check_array: 'char *' or " ..
        "'const char *' expected"
    local wrong_2_arg_err = 'msgpack.check_array: number expected as 2nd ' ..
        'argument'
    local wrong_3_arg_err = 'msgpack.check_array: number or nil expected as ' ..
        '3rd argument'

    local bad_api_cases = {
        {
            '1st argument: nil',
            args = {nil, 1},
            exp_err = wrong_1_arg_not_cdata_err,
        },
        {
            '1st argument: not cdata',
            args = {1, 1},
            exp_err = wrong_1_arg_not_cdata_err,
        },
        {
            '1st argument: wrong cdata type',
            args = {box.tuple.new(), 1},
            exp_err = wrong_1_arg_err,
        },
        {
            '2nd argument: nil',
            args = {ffi.cast('char *', '\x90'), nil},
            exp_err = wrong_2_arg_err,
        },
        {
            '2nd argument: wrong type',
            args = {ffi.cast('char *', '\x90'), 'eee'},
            exp_err = wrong_2_arg_err,
        },
        {
            '3rd argument: wrong type',
            args = {ffi.cast('char *', '\x90'), 1, 'eee'},
            exp_err = wrong_3_arg_err,
        },
    }

    -- Add good cases with wrong expected length to the bad cases.
    for _, case in ipairs(good_cases) do
        table.insert(bad_cases, {
            case[1],
            data = case.data,
            exp_len = case.exp_len + 1,
            exp_err = 'msgpack.check_array: expected array of length'
        })
    end

    test:plan(4 * #good_cases + 2 * #bad_cases + #bad_api_cases)

    -- Good cases: don't pass 2nd argument.
    for _, ctype in ipairs({'char *', 'const char *'}) do
        for _, case in ipairs(good_cases) do
            local buf = ffi.cast(ctype, case.data)
            local size = case.size or #case.data
            local new_buf, len = s.check_array(buf, size)
            local rewind = new_buf - buf
            local description = ('good; %s; %s; %s'):format(case[1], ctype,
                'do not pass 2nd argument')
            test:is_deeply({len, rewind}, {case.exp_len, case.exp_rewind},
                description)
        end
    end

    -- Good cases: pass right 2nd argument.
    for _, ctype in ipairs({'char *', 'const char *'}) do
        for _, case in ipairs(good_cases) do
            local buf = ffi.cast(ctype, case.data)
            local size = case.size or #case.data
            local new_buf, len = s.check_array(buf, size, case.exp_len)
            local rewind = new_buf - buf
            local description = ('good; %s; %s; %s'):format(case[1], ctype,
                'pass right 2nd argument')
            test:is_deeply({len, rewind}, {case.exp_len, case.exp_rewind},
                description)
        end
    end

    -- Bad cases.
    for _, ctype in ipairs({'char *', 'const char *'}) do
        for _, case in ipairs(bad_cases) do
            local buf = ffi.cast(ctype, case.data)
            local size = case.size or #case.data
            local n, err = s.check_array(buf, size, case.exp_len)
            local description = ('bad; %s; %s'):format(case[1], ctype)
            test:ok(n == nil and err:startswith(case.exp_err), description)
        end
    end

    -- Bad API usage cases.
    for _, case in ipairs(bad_api_cases) do
        local ok, err = pcall(s.check_array, unpack(case.args))
        local description = 'bad API usage; ' .. case[1]
        test:is_deeply({ok, err}, {false, case.exp_err},  description)
    end
end

tap.test("msgpack", function(test)
    local serializer = require('msgpack')
    test:plan(11)
    test:test("unsigned", common.test_unsigned, serializer)
    test:test("signed", common.test_signed, serializer)
    test:test("double", common.test_double, serializer)
    test:test("boolean", common.test_boolean, serializer)
    test:test("string", common.test_string, serializer)
    test:test("nil", common.test_nil, serializer)
    test:test("table", common.test_table, serializer, is_array, is_map)
    test:test("ucdata", common.test_ucdata, serializer)
    test:test("offsets", test_offsets, serializer)
    test:test("misc", test_misc, serializer)
    test:test("check_array", test_check_array, serializer)
end)
