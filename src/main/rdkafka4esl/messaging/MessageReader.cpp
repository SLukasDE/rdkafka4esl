#include <rdkafka4esl/messaging/MessageReader.h>
#include <rdkafka4esl/messaging/Message.h>
#include <rdkafka4esl/Logger.h>

#include <cstring>
#include <cstdint>

namespace rdkafka4esl {
namespace messaging {

namespace {
Logger logger("rdkafka4esl::messaging::MessageReader");
}

MessageReader::MessageReader(const Message& aMessage)
: message(aMessage)
{ }

std::size_t MessageReader::read(void* data, std::size_t size) {
	if(getSizeReadable() == 0) {
		return esl::utility::Reader::npos;
	}

	if(size > getSizeReadable()) {
		size = getSizeReadable();
	}

	const std::uint8_t* payload = static_cast<const std::uint8_t*>(message.getPayload());
	std::memcpy(data, payload + pos, size);
	pos += size;

	return size;
}

std::size_t MessageReader::getSizeReadable() const {
	if(message.getPayloadLength() <= pos) {
		return 0;
	}
	return message.getPayloadLength() - pos;
}

} /* namespace messaging */
} /* namespace rdkafka4esl */
