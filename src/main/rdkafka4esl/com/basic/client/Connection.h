#ifndef RDKAFKA4ESL_COM_BASIC_CLIENT_CONNECTION_H_
#define RDKAFKA4ESL_COM_BASIC_CLIENT_CONNECTION_H_

#include <esl/com/basic/client/Interface.h>
#include <esl/io/Output.h>

#include <vector>
#include <string>
#include <utility>
#include <cstdint>

#include <librdkafka/rdkafka.h>

namespace rdkafka4esl {
namespace com {
namespace basic {

namespace broker {
class Client;
} /* namespace broker */

namespace client {

class Connection final : public esl::com::basic::client::Interface::Connection {
public:
	Connection(broker::Client& client, rd_kafka_t& producerRdKafkaHandle, rd_kafka_topic_t& rdKafkaTopic, const std::vector<std::pair<std::string, std::string>>& parameters);
	~Connection();

	esl::io::Output send(esl::io::Output output, std::vector<std::pair<std::string, std::string>> parameters) override;

	void flush();// override;
	bool wait(std::uint32_t ms);// override;

private:
	broker::Client& client;
	std::string topicName;

	std::string defaultKey;
	std::int32_t defaultPartition = RD_KAFKA_PARTITION_UA;

	rd_kafka_t& producerRdKafkaHandle;
	rd_kafka_topic_t& rdKafkaTopic;
};

} /* namespace client */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_COM_BASIC_CLIENT_CONNECTION_H_ */
