/* Generated SBE (Simple Binary Encoding) message codec */
#ifndef _EXCHANGE_PROTOCOL_REJECTREASON_CXX_H_
#define _EXCHANGE_PROTOCOL_REJECTREASON_CXX_H_

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

class RejectReason
{
public:
    enum Value
    {
        NON_POSITIVE_QUANTITY = static_cast<std::uint8_t>(0),
        LOT_VIOLATION = static_cast<std::uint8_t>(1),
        NON_POSITIVE_PRICE = static_cast<std::uint8_t>(2),
        TICK_VIOLATION = static_cast<std::uint8_t>(3),
        STATIC_BAND_VIOLATION = static_cast<std::uint8_t>(4),
        DYNAMIC_BAND_VIOLATION = static_cast<std::uint8_t>(5),
        INVALID_FIELDS = static_cast<std::uint8_t>(6),
        MINIMUM_QUANTITY_ABOVE_ORDER = static_cast<std::uint8_t>(7),
        DISPLAY_QUANTITY_ABOVE_ORDER = static_cast<std::uint8_t>(8),
        MINIMUM_QUANTITY_NOT_MET = static_cast<std::uint8_t>(9),
        WOULD_CROSS = static_cast<std::uint8_t>(10),
        FILL_OR_KILL_UNFILLABLE = static_cast<std::uint8_t>(11),
        STATE_NOT_PERMITTED = static_cast<std::uint8_t>(12),
        UNKNOWN_ORDER = static_cast<std::uint8_t>(13),
        QUANTITY_BELOW_EXECUTED = static_cast<std::uint8_t>(14),
        NULL_VALUE = static_cast<std::uint8_t>(255)
    };

    static RejectReason::Value get(const std::uint8_t value)
    {
        switch (value)
        {
            case static_cast<std::uint8_t>(0): return NON_POSITIVE_QUANTITY;
            case static_cast<std::uint8_t>(1): return LOT_VIOLATION;
            case static_cast<std::uint8_t>(2): return NON_POSITIVE_PRICE;
            case static_cast<std::uint8_t>(3): return TICK_VIOLATION;
            case static_cast<std::uint8_t>(4): return STATIC_BAND_VIOLATION;
            case static_cast<std::uint8_t>(5): return DYNAMIC_BAND_VIOLATION;
            case static_cast<std::uint8_t>(6): return INVALID_FIELDS;
            case static_cast<std::uint8_t>(7): return MINIMUM_QUANTITY_ABOVE_ORDER;
            case static_cast<std::uint8_t>(8): return DISPLAY_QUANTITY_ABOVE_ORDER;
            case static_cast<std::uint8_t>(9): return MINIMUM_QUANTITY_NOT_MET;
            case static_cast<std::uint8_t>(10): return WOULD_CROSS;
            case static_cast<std::uint8_t>(11): return FILL_OR_KILL_UNFILLABLE;
            case static_cast<std::uint8_t>(12): return STATE_NOT_PERMITTED;
            case static_cast<std::uint8_t>(13): return UNKNOWN_ORDER;
            case static_cast<std::uint8_t>(14): return QUANTITY_BELOW_EXECUTED;
            case static_cast<std::uint8_t>(255): return NULL_VALUE;
        }

        throw std::runtime_error("unknown value for enum RejectReason [E103]");
    }

    static const char *c_str(const RejectReason::Value value)
    {
        switch (value)
        {
            case NON_POSITIVE_QUANTITY: return "NON_POSITIVE_QUANTITY";
            case LOT_VIOLATION: return "LOT_VIOLATION";
            case NON_POSITIVE_PRICE: return "NON_POSITIVE_PRICE";
            case TICK_VIOLATION: return "TICK_VIOLATION";
            case STATIC_BAND_VIOLATION: return "STATIC_BAND_VIOLATION";
            case DYNAMIC_BAND_VIOLATION: return "DYNAMIC_BAND_VIOLATION";
            case INVALID_FIELDS: return "INVALID_FIELDS";
            case MINIMUM_QUANTITY_ABOVE_ORDER: return "MINIMUM_QUANTITY_ABOVE_ORDER";
            case DISPLAY_QUANTITY_ABOVE_ORDER: return "DISPLAY_QUANTITY_ABOVE_ORDER";
            case MINIMUM_QUANTITY_NOT_MET: return "MINIMUM_QUANTITY_NOT_MET";
            case WOULD_CROSS: return "WOULD_CROSS";
            case FILL_OR_KILL_UNFILLABLE: return "FILL_OR_KILL_UNFILLABLE";
            case STATE_NOT_PERMITTED: return "STATE_NOT_PERMITTED";
            case UNKNOWN_ORDER: return "UNKNOWN_ORDER";
            case QUANTITY_BELOW_EXECUTED: return "QUANTITY_BELOW_EXECUTED";
            case NULL_VALUE: return "NULL_VALUE";
        }

        throw std::runtime_error("unknown value for enum RejectReason [E103]:");
    }

    template<typename CharT, typename Traits>
    friend std::basic_ostream<CharT, Traits> & operator << (
        std::basic_ostream<CharT, Traits> &os, RejectReason::Value m)
    {
        return os << RejectReason::c_str(m);
    }
};

}
}

#endif
