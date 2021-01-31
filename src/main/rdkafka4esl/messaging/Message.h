#ifndef RDKAFKA4ESL_MESSAGING_MESSAGE_H_
#define RDKAFKA4ESL_MESSAGING_MESSAGE_H_

#include <rdkafka4esl/messaging/MessageReader.h>

#include <esl/messaging/Message.h>
#include <esl/utility/Reader.h>

#include <string>
#include <cstdint>

#include <librdkafka/rdkafka.h>

namespace rdkafka4esl {
namespace messaging {

class Message : public esl::messaging::Message {
public:
	Message(rd_kafka_message_t& kafkaMessage);

	std::string getValue(const std::string& key) const override;

	esl::utility::Reader& getReader() override;

	std::int64_t getOffset() const noexcept;
	std::int32_t getPartition() const noexcept;
	const std::string& getKey() const noexcept;
	const void* getPayload() const noexcept;
	std::size_t getPayloadLength() const noexcept;

private:
	std::int64_t offset;
	std::int32_t partition;
	std::string key;

	void* payload;
	std::size_t length;

	MessageReader reader;
};

} /* namespace messaging */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_MESSAGING_MESSAGE_H_ */
