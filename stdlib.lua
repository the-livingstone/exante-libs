--- Symboldb lua standard library
-- @module stdlib

stdlib = {}

function stdlib.test()
    return 'LUA LIB';
end


-- @table month_data
stdlib.month_data = {
-- Short name | Futures | Call Options | Put Options
   {'Jan',        'F',        'A',        'M'},
   {'Feb',        'G',        'B',        'N'},
   {'Mar',        'H',        'C',        'O'},
   {'Apr',        'J',        'D',        'P'},
   {'May',        'K',        'E',        'Q'},
   {'Jun',        'M',        'F',        'R'},
   {'Jul',        'N',        'G',        'S'},
   {'Aug',        'Q',        'H',        'T'},
   {'Sep',        'U',        'I',        'U'},
   {'Oct',        'V',        'J',        'V'},
   {'Nov',        'X',        'K',        'W'},
   {'Dec',        'Z',        'L',        'X'}
}

---
-- Get month code based on instrument type
-- @param month num (1..12)
-- @param instrument
-- @return month code as a string
-- @usage stdlib.getMonthCode(7, instrument)
function stdlib.getMonthCode(month, instrument)
   local idx = 1
   if instrument.type == 'FUTURE' then
      idx = 2
   elseif instrument.type == 'OPTION' then
      if instrument.optionRight == 'CALL' then
         idx = 3
      else
         idx = 4
      end
   end
   return stdlib.month_data[month][idx]
end

---
-- Format maturity date
-- @param date is a table with required month and year and optional day
-- @return string representation of maturity date
-- @usage stdlib.maturityDate({year = 2016, month = 1})
function stdlib.maturityDate(date)
   return (date.day or '')..stdlib.month_data[date.month][2]..date.year
end

---
-- Pretty format maturity date
-- @param date is a table with required month and year and optional day
-- @return string pretty printed representation of maturity date
-- @usage stdlib.maturityDate3({year = 2016, month = 1})
function stdlib.maturityDate3(date)
   return (date.day and (date.day..' ') or '')..stdlib.month_data[date.month][1]..' '..date.year
end

---
-- Build maturity date for the instrument
-- @param instrument
-- @return string representation of maturity date or maturity name (if exists)
-- @usage stdlib.buildMaturityDate(instrument)
function stdlib.buildMaturityDate(instrument)
   return instrument.maturityName or stdlib.maturityDate(instrument.maturityDate)
end

---
-- Build pretty printed maturity date for the instrument
-- @param instrument
-- @return string pritty printed representation of maturity date
-- or maturity name (if exists)
-- @usage stdlib.buildMaturityDate3(instrument)
function stdlib.buildMaturityDate3(instrument)
   return instrument.maturityName or stdlib.maturityDate3(instrument.maturityDate)
end


---
-- Build maturity range for the instrument
-- @param instrument
-- @return string pritty printed representation of maturity date
-- or maturity name (if exists)
-- @usage stdlib.buildMaturityRange(instrument)
function stdlib.buildMaturityRange(instrument)
   return stdlib.maturityDate(instrument.nearMaturityDate)..'-'..stdlib.maturityDate(instrument.farMaturityDate)
end

---
-- Build pretty printed maturity range for the instrument
-- @param instrument
-- @return string pritty printed representation of maturity date
-- or maturity name (if exists)
-- @usage stdlib.buildMaturityRange3(instrument)
function stdlib.buildMaturityRange3(instrument)
   return stdlib.maturityDate3(instrument.nearMaturityDate)..'-'..stdlib.maturityDate3(instrument.farMaturityDate)
end

return stdlib
