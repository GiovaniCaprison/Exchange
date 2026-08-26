/* Generated SBE (Simple Binary Encoding) message codec */
#ifndef _EXCHANGE_PROTOCOL_REMOVEREASON_CXX_H_
#define _EXCHANGE_PROTOCOL_REMOVEREASON_CXX_H_

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

class RemoveReason
{
public:
    enum Value
    {
        CANCELLED = static_cast<std::uint8_t>(0),
        REPLACED = static_cast<std::uint8_t>(1),
        MASS_CANCELLED = static_cast<std::uint8_t>(2),
        IMMEDIATE_OR_CANCEL_REMAINDER = static_cast<std::uint8_t>(3),
        SELF_MATCH_PREVENTED = static_cast<std::uint8_t>(4),
        NULL_VALUE = static_cast<std::uint8_t>(255)
    };

    static RemoveReason::Value get(const std::uint8_t value)
    {
        switch (value)
        {
            case static_cast<std::uint8_t>(0): return CANCELLED;
            case static_cast<std::uint8_t>(1): return REPLACED;
            case static_cast<std::uint8_t>(2): return MASS_CANCELLED;
            case static_cast<std::uint8_t>(3): return IMMEDIATE_OR_CANCEL_REMAINDER;
            case static_cast<std::uint8_t>(4): return SELF_MATCH_PREVENTED;
            case static_cast<std::uint8_t>(255): return NULL_VALUE;
        }

        throw std::runtime_error("unknown value for enum RemoveReason [E103]");
    }

    static const char *c_str(const RemoveReason::Value value)
    {
        switch (value)
        {
            case CANCELLED: return "CANCELLED";
            case REPLACED: return "REPLACED";
            case MASS_CANCELLED: return "MASS_CANCELLED";
            case IMMEDIATE_OR_CANCEL_REMAINDER: return "IMMEDIATE_OR_CANCEL_REMAINDER";
            case SELF_MATCH_PREVENTED: return "SELF_MATCH_PREVENTED";
            case NULL_VALUE: return "NULL_VALUE";
        }

        throw std::runtime_error("unknown value for enum RemoveReason [E103]:");
    }

    template<typename CharT, typename Traits>
    friend std::basic_ostream<CharT, Traits> & operator << (
        std::basic_ostream<CharT, Traits> &os, RemoveReason::Value m)
    {
        return os << RemoveReason::c_str(m);
    }
};

}
}

#endif
