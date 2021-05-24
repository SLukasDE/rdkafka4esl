#ifndef RDKAFKA4ESL_MESSAGING_SERVER_MESSAGECONTEXT_H_
#define RDKAFKA4ESL_MESSAGING_SERVER_MESSAGECONTEXT_H_

#include <rdkafka4esl/messaging/server/Socket.h>

#include <esl/messaging/server/MessageContext.h>
#include <esl/messaging/server/Connection.h>

#include <vector>
#include <string>
#include <utility>

#include <librdkafka/rdkafka.h>

namespace rdkafka4esl {
namespace messaging {
namespace server {

class MessageContext : public esl::messaging::server::MessageContext {
public:
	MessageContext(const Socket& socket, rd_kafka_message_t& kafkaMessage);

	esl::messaging::server::Connection& getConnection() const override;

	esl::object::Interface::Object* findObject(const std::string& id) const override;

	bool hasValue(const std::string& key) const override;
	std::string getValue(const std::string& key) const override;
	const std::vector<std::pair<std::string, std::string>>& getValues() const override;

private:
	const Socket& socket;
	rd_kafka_message_t& kafkaMessage;

	std::vector<std::pair<std::string, std::string>> values;
};

} /* namespace server */
} /* namespace messaging */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_MESSAGING_SERVER_MESSAGECONTEXT_H_ */
