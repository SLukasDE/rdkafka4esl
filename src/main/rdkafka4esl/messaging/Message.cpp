#include <rdkafka4esl/messaging/Message.h>

namespace rdkafka4esl {
namespace messaging {

namespace {
std::string getTopicName(rd_kafka_message_t& kafkaMessage) {
	return kafkaMessage.rkt == nullptr ? "" : rd_kafka_topic_name(kafkaMessage.rkt);
}
}

Message::Message(rd_kafka_message_t& kafkaMessage)
: esl::messaging::Message(getTopicName(kafkaMessage)),
  offset(kafkaMessage.offset),
  partition(kafkaMessage.partition),
  payload(kafkaMessage.payload),
  length(kafkaMessage.len),
  reader(*this)
{
	if(kafkaMessage.key) {
		key = std::string(static_cast<char*>(kafkaMessage.key), kafkaMessage.key_len);
	}
}

std::string Message::getValue(const std::string& key) const {
	if(key == "offset") {
		return std::to_string(getOffset());
	}
	if(key == "partition") {
		return std::to_string(getPartition());
	}
	if(key == "key") {
		return getKey();
	}

	return esl::messaging::Message::getValue(key);
}

esl::utility::Reader& Message::getReader() {
	return reader;
}

std::int64_t Message::getOffset() const noexcept {
	return offset;
}

std::int32_t Message::getPartition() const noexcept {
	return partition;
}

const std::string& Message::getKey() const noexcept {
	return key;
}

const void* Message::getPayload() const noexcept {
	return payload;
}

std::size_t Message::getPayloadLength() const noexcept {
	return length;
}

} /* namespace messaging */
} /* namespace rdkafka4esl */
