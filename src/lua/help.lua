local doc = require('help.en_US')

-- The built-in tutorial contains links to the online
-- documentation with a <version> placeholder like
-- https://tarantool.io/en/doc/<version>/<...>. We should replace
-- the placeholders with a version of a documentation page that
-- corresponds to a tarantool version a user runs.
local DOCUMENTATION_VERSION = '1.10'

help = {}
help[1] = {}
help[1] = "To get help, see the Tarantool manual at https://tarantool.io/en/doc/"
help[2] = "To start the interactive Tarantool tutorial, type 'tutorial()' here"
tutorial = {}
tutorial[1] = help[2]

local help_function_data = {};
local help_object_data = {}

local function help_call(table, param)
    return help
end

setmetatable(help, { __call = help_call })

local screen_id = 1;

local function tutorial_call(table, action)
    if action == 'start' then
        screen_id = 1;
    elseif action == 'next' or action == 'more' then
        screen_id = screen_id + 1
    elseif action == 'prev' then
        screen_id = screen_id - 1
    elseif type(action) == 'number' and action % 1 == 0 then
        screen_id = tonumber(action)
    elseif action ~= nil then
        error('Usage: tutorial("start" | "next" | "prev" | 1 .. '..
            #doc.tutorial..')')
    end
    if screen_id < 1 then
        screen_id = 1
    elseif screen_id > #doc.tutorial then
        screen_id = #doc.tutorial
    end
    local res = doc.tutorial[screen_id]
    -- The parentheses are to discard return values except first
    -- one.
    return (res:gsub('<version>', DOCUMENTATION_VERSION))
end

setmetatable(tutorial, { __call = tutorial_call })

return {
    help = help;
    tutorial = tutorial;
}
