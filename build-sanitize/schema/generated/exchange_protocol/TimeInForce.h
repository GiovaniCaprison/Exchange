/* Generated SBE (Simple Binary Encoding) message codec */
#ifndef _EXCHANGE_PROTOCOL_TIMEINFORCE_CXX_H_
#define _EXCHANGE_PROTOCOL_TIMEINFORCE_CXX_H_

#if !defined(__STDC_LIMIT_MACROS)
#  define __STDC_LIMIT_MACROS 1
#endif

#include <cstdint>
#include <iomanip>
#include <limits>
#include <ostream>
#include <stdexcept>
#include <sstream>
#include <string>

#define SBE_NULLVALUE_INT8 (std::numeric_limits<std::int8_t>::min)()
#define SBE_NULLVALUE_INT16 (std::numeric_limits<std::int16_t>::min)()
#define SBE_NULLVALUE_INT32 (std::numeric_limits<std::int32_t>::min)()
#define SBE_NULLVALUE_INT64 (std::numeric_limits<std::int64_t>::min)()
#define SBE_NULLVALUE_UINT8 (std::numeric_limits<std::uint8_t>::max)()
#define SBE_NULLVALUE_UINT16 (std::numeric_limits<std::uint16_t>::max)()
#define SBE_NULLVALUE_UINT32 (std::numeric_limits<std::uint32_t>::max)()
#define SBE_NULLVALUE_UINT64 (std::numeric_limits<std::uint64_t>::max)()

namespace exchange {
namespace protocol {

class TimeInForce
{
public:
    enum Value
    {
        GOOD_TILL_CANCEL = static_cast<std::uint8_t>(0),
        DAY = static_cast<std::uint8_t>(1),
        IMMEDIATE_OR_CANCEL = static_cast<std::uint8_t>(2),
        FILL_OR_KILL = static_cast<std::uint8_t>(3),
        NULL_VALUE = static_cast<std::uint8_t>(255)
    };

    static TimeInForce::Value get(const std::uint8_t value)
    {
        switch (value)
        {
            case static_cast<std::uint8_t>(0): return GOOD_TILL_CANCEL;
            case static_cast<std::uint8_t>(1): return DAY;
            case static_cast<std::uint8_t>(2): return IMMEDIATE_OR_CANCEL;
            case static_cast<std::uint8_t>(3): return FILL_OR_KILL;
            case static_cast<std::uint8_t>(255): return NULL_VALUE;
        }

        throw std::runtime_error("unknown value for enum TimeInForce [E103]");
    }

    static const char *c_str(const TimeInForce::Value value)
    {
        switch (value)
        {
            case GOOD_TILL_CANCEL: return "GOOD_TILL_CANCEL";
            case DAY: return "DAY";
            case IMMEDIATE_OR_CANCEL: return "IMMEDIATE_OR_CANCEL";
            case FILL_OR_KILL: return "FILL_OR_KILL";
            case NULL_VALUE: return "NULL_VALUE";
        }

        throw std::runtime_error("unknown value for enum TimeInForce [E103]:");
    }

    template<typename CharT, typename Traits>
    friend std::basic_ostream<CharT, Traits> & operator << (
        std::basic_ostream<CharT, Traits> &os, TimeInForce::Value m)
    {
        return os << TimeInForce::c_str(m);
    }
};

}
}

#endif
