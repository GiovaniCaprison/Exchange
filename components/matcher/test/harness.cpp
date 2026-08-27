#include "harness.hpp"

#include <stdexcept>

namespace exchange::matcher::test {

namespace {

const char* rejectName(const std::int32_t reason) {
  switch (static_cast<sbe::RejectReason::Value>(reason)) {
    case sbe::RejectReason::NON_POSITIVE_QUANTITY:
      return "NON_POSITIVE_QUANTITY";
    case sbe::RejectReason::LOT_VIOLATION:
      return "LOT_VIOLATION";
    case sbe::RejectReason::NON_POSITIVE_PRICE:
      return "NON_POSITIVE_PRICE";
    case sbe::RejectReason::TICK_VIOLATION:
      return "TICK_VIOLATION";
    case sbe::RejectReason::STATIC_BAND_VIOLATION:
      return "STATIC_BAND_VIOLATION";
    case sbe::RejectReason::DYNAMIC_BAND_VIOLATION:
      return "DYNAMIC_BAND_VIOLATION";
    case sbe::RejectReason::INVALID_FIELDS:
      return "INVALID_FIELDS";
    case sbe::RejectReason::MINIMUM_QUANTITY_ABOVE_ORDER:
      return "MINIMUM_QUANTITY_ABOVE_ORDER";
    case sbe::RejectReason::DISPLAY_QUANTITY_ABOVE_ORDER:
      return "DISPLAY_QUANTITY_ABOVE_ORDER";
    case sbe::RejectReason::MINIMUM_QUANTITY_NOT_MET:
      return "MINIMUM_QUANTITY_NOT_MET";
    case sbe::RejectReason::WOULD_CROSS:
      return "WOULD_CROSS";
    case sbe::RejectReason::FILL_OR_KILL_UNFILLABLE:
      return "FILL_OR_KILL_UNFILLABLE";
    case sbe::RejectReason::STATE_NOT_PERMITTED:
      return "STATE_NOT_PERMITTED";
    case sbe::RejectReason::UNKNOWN_ORDER:
      return "UNKNOWN_ORDER";
    case sbe::RejectReason::QUANTITY_BELOW_EXECUTED:
      return "QUANTITY_BELOW_EXECUTED";
    default:
      return "UNKNOWN_REASON";
  }
}

const char* removeName(const std::int32_t reason) {
  switch (static_cast<sbe::RemoveReason::Value>(reason)) {
    case sbe::RemoveReason::CANCELLED:
      return "CANCELLED";
    case sbe::RemoveReason::REPLACED:
      return "REPLACED";
    case sbe::RemoveReason::MASS_CANCELLED:
      return "MASS_CANCELLED";
    case sbe::RemoveReason::IMMEDIATE_OR_CANCEL_REMAINDER:
      return "IOC_REMAINDER";
    case sbe::RemoveReason::SELF_MATCH_PREVENTED:
      return "SELF_MATCH_PREVENTED";
    case sbe::RemoveReason::EXPIRED:
      return "EXPIRED";
    default:
      return "UNKNOWN_REASON";
  }
}

const char* stateName(const std::int32_t state) {
  switch (static_cast<sbe::SessionState::Value>(state)) {
    case sbe::SessionState::PRE_OPEN:
      return "PRE_OPEN";
    case sbe::SessionState::OPENING_AUCTION:
      return "OPENING_AUCTION";
    case sbe::SessionState::CONTINUOUS:
      return "CONTINUOUS";
    case sbe::SessionState::CLOSING_AUCTION:
      return "CLOSING_AUCTION";
    case sbe::SessionState::HALTED:
      return "HALTED";
    case sbe::SessionState::CLOSED:
      return "CLOSED";
    default:
      return "UNKNOWN_STATE";
  }
}

template <typename Decoder, typename Context>
Decoder decodedAt(std::vector<char>& bytes, const std::size_t at, EventView& view,
                  Context (Decoder::*context)()) {
  sbe::MessageHeader header;
  header.wrap(bytes.data(), at, 0, bytes.size());
  Decoder decoder;
  decoder.wrapForDecode(bytes.data(), at + sbe::MessageHeader::encodedLength(),
                        header.blockLength(), header.version(), bytes.size());
  auto ctx = (decoder.*context)();
  view.sequence = ctx.sequence();
  view.inputSequence = ctx.inputSequence();
  view.timestamp = ctx.timestamp();
  view.instrumentId = ctx.instrumentId();
  return decoder;
}

}  // namespace

std::vector<EventView> readEvents(const std::vector<char>& bytes) {
  std::vector<char> mutableBytes = bytes;
  std::vector<EventView> events;
  std::size_t at = 0;
  while (at + sbe::MessageHeader::encodedLength() <= mutableBytes.size()) {
    sbe::MessageHeader header;
    header.wrap(mutableBytes.data(), at, 0, mutableBytes.size());
    EventView view;
    view.templateId = header.templateId();
    switch (header.templateId()) {
      case sbe::OrderAccepted::sbeTemplateId(): {
        auto decoder =
            decodedAt<sbe::OrderAccepted>(mutableBytes, at, view, &sbe::OrderAccepted::context);
        view.orderId = decoder.orderId();
        view.clientOrderId = decoder.clientOrderId();
        view.participantId = decoder.participantId();
        break;
      }
      case sbe::OrderRejected::sbeTemplateId(): {
        auto decoder =
            decodedAt<sbe::OrderRejected>(mutableBytes, at, view, &sbe::OrderRejected::context);
        view.clientOrderId = decoder.clientOrderId();
        view.participantId = decoder.participantId();
        view.reason = static_cast<std::int32_t>(decoder.reason());
        break;
      }
      case sbe::OrderRested::sbeTemplateId(): {
        auto decoder =
            decodedAt<sbe::OrderRested>(mutableBytes, at, view, &sbe::OrderRested::context);
        view.orderId = decoder.orderId();
        view.price = decoder.price();
        view.quantity = decoder.quantity();
        view.side = static_cast<std::int32_t>(decoder.side());
        break;
      }
      case sbe::OrderExecuted::sbeTemplateId(): {
        auto decoder =
            decodedAt<sbe::OrderExecuted>(mutableBytes, at, view, &sbe::OrderExecuted::context);
        view.executionId = decoder.executionId();
        view.aggressorOrderId = decoder.aggressorOrderId();
        view.restingOrderId = decoder.restingOrderId();
        view.price = decoder.price();
        view.quantity = decoder.quantity();
        break;
      }
      case sbe::OrderReduced::sbeTemplateId(): {
        auto decoder =
            decodedAt<sbe::OrderReduced>(mutableBytes, at, view, &sbe::OrderReduced::context);
        view.orderId = decoder.orderId();
        view.quantity = decoder.quantity();
        break;
      }
      case sbe::OrderRemoved::sbeTemplateId(): {
        auto decoder =
            decodedAt<sbe::OrderRemoved>(mutableBytes, at, view, &sbe::OrderRemoved::context);
        view.orderId = decoder.orderId();
        view.quantity = decoder.quantity();
        view.reason = static_cast<std::int32_t>(decoder.reason());
        break;
      }
      case sbe::OrderTriggered::sbeTemplateId(): {
        auto decoder =
            decodedAt<sbe::OrderTriggered>(mutableBytes, at, view, &sbe::OrderTriggered::context);
        view.orderId = decoder.orderId();
        break;
      }
      case sbe::SessionStateChanged::sbeTemplateId(): {
        auto decoder = decodedAt<sbe::SessionStateChanged>(mutableBytes, at, view,
                                                           &sbe::SessionStateChanged::context);
        view.state = static_cast<std::int32_t>(decoder.state());
        break;
      }
      case sbe::AuctionIndicative::sbeTemplateId(): {
        auto decoder = decodedAt<sbe::AuctionIndicative>(mutableBytes, at, view,
                                                         &sbe::AuctionIndicative::context);
        view.price = decoder.price();
        view.quantity = decoder.quantity();
        break;
      }
      default:
        throw std::runtime_error("template " + std::to_string(header.templateId()) +
                                 " is not an event");
    }
    events.push_back(view);
    at += sbe::MessageHeader::encodedLength() + header.blockLength();
  }
  return events;
}

std::string render(const EventView& event) {
  switch (event.templateId) {
    case sbe::OrderAccepted::sbeTemplateId():
      return "ACCEPTED #" + std::to_string(event.clientOrderId) +
             " order=" + std::to_string(event.orderId);
    case sbe::OrderRejected::sbeTemplateId():
      return "REJECTED #" + std::to_string(event.clientOrderId) + " " + rejectName(event.reason);
    case sbe::OrderRested::sbeTemplateId():
      return "RESTED order=" + std::to_string(event.orderId) +
             (event.side == 0 ? " BUY " : " SELL ") + std::to_string(event.price) + " " +
             std::to_string(event.quantity);
    case sbe::OrderExecuted::sbeTemplateId():
      return "EXECUTED exec=" + std::to_string(event.executionId) +
             " aggressor=" + std::to_string(event.aggressorOrderId) +
             " resting=" + std::to_string(event.restingOrderId) + " " +
             std::to_string(event.price) + " " + std::to_string(event.quantity);
    case sbe::OrderReduced::sbeTemplateId():
      return "REDUCED order=" + std::to_string(event.orderId) + " " +
             std::to_string(event.quantity);
    case sbe::OrderRemoved::sbeTemplateId():
      return "REMOVED order=" + std::to_string(event.orderId) + " " +
             std::to_string(event.quantity) + " " + removeName(event.reason);
    case sbe::OrderTriggered::sbeTemplateId():
      return "TRIGGERED order=" + std::to_string(event.orderId);
    case sbe::SessionStateChanged::sbeTemplateId():
      return std::string("STATE ") + stateName(event.state);
    case sbe::AuctionIndicative::sbeTemplateId():
      return "INDICATIVE " + std::to_string(event.price) + " " + std::to_string(event.quantity);
    default:
      return "UNKNOWN " + std::to_string(event.templateId);
  }
}

}  // namespace exchange::matcher::test
