#ifndef RDKAFKA4ESL_MESSAGING_MESSAGEREADER_H_
#define RDKAFKA4ESL_MESSAGING_MESSAGEREADER_H_

#include <esl/io/Reader.h>

#include <string>

namespace rdkafka4esl {
namespace messaging {

class Message;

class MessageReader : public esl::io::Reader {
public:
	MessageReader(const Message& message);

	std::size_t read(void* data, std::size_t size) override;

	std::size_t getSizeReadable() const override;
	bool hasSize() const override;
	std::size_t getSize() const override;

private:
	const Message& message;
	std::size_t pos = 0;
};

} /* namespace messaging */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_MESSAGING_MESSAGEREADER_H_ */
