#ifndef RDKAFKA4ESL_MESSAGING_SERVER_REQUEST_H_
#define RDKAFKA4ESL_MESSAGING_SERVER_REQUEST_H_

#include <esl/object/Values.h>

#include <vector>
#include <string>
#include <utility>

#include <librdkafka/rdkafka.h>

namespace rdkafka4esl {
namespace messaging {
namespace server {

class Request : public esl::object::Values<std::string> {
public:
	Request(rd_kafka_message_t& kafkaMessage);

	bool hasValue(const std::string& key) const override;
	std::string getValue(const std::string& key) const override;
	const std::vector<std::pair<std::string, std::string>>& getValues() const override;

private:
	rd_kafka_message_t& kafkaMessage;

	std::vector<std::pair<std::string, std::string>> values;
};

} /* namespace server */
} /* namespace messaging */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_MESSAGING_SERVER_REQUEST_H_ */
