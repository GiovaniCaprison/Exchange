/* Generated SBE (Simple Binary Encoding) message codec */
#ifndef _EXCHANGE_PROTOCOL_SESSIONSTATE_CXX_H_
#define _EXCHANGE_PROTOCOL_SESSIONSTATE_CXX_H_

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

class SessionState
{
public:
    enum Value
    {
        PRE_OPEN = static_cast<std::uint8_t>(0),
        OPENING_AUCTION = static_cast<std::uint8_t>(1),
        CONTINUOUS = static_cast<std::uint8_t>(2),
        CLOSING_AUCTION = static_cast<std::uint8_t>(3),
        HALTED = static_cast<std::uint8_t>(4),
        CLOSED = static_cast<std::uint8_t>(5),
        NULL_VALUE = static_cast<std::uint8_t>(255)
    };

    static SessionState::Value get(const std::uint8_t value)
    {
        switch (value)
        {
            case static_cast<std::uint8_t>(0): return PRE_OPEN;
            case static_cast<std::uint8_t>(1): return OPENING_AUCTION;
            case static_cast<std::uint8_t>(2): return CONTINUOUS;
            case static_cast<std::uint8_t>(3): return CLOSING_AUCTION;
            case static_cast<std::uint8_t>(4): return HALTED;
            case static_cast<std::uint8_t>(5): return CLOSED;
            case static_cast<std::uint8_t>(255): return NULL_VALUE;
        }

        throw std::runtime_error("unknown value for enum SessionState [E103]");
    }

    static const char *c_str(const SessionState::Value value)
    {
        switch (value)
        {
            case PRE_OPEN: return "PRE_OPEN";
            case OPENING_AUCTION: return "OPENING_AUCTION";
            case CONTINUOUS: return "CONTINUOUS";
            case CLOSING_AUCTION: return "CLOSING_AUCTION";
            case HALTED: return "HALTED";
            case CLOSED: return "CLOSED";
            case NULL_VALUE: return "NULL_VALUE";
        }

        throw std::runtime_error("unknown value for enum SessionState [E103]:");
    }

    template<typename CharT, typename Traits>
    friend std::basic_ostream<CharT, Traits> & operator << (
        std::basic_ostream<CharT, Traits> &os, SessionState::Value m)
    {
        return os << SessionState::c_str(m);
    }
};

}
}

#endif
