#ifndef RDKAFKA4ESL_MESSAGING_PRODUCER_H_
#define RDKAFKA4ESL_MESSAGING_PRODUCER_H_

#include <esl/messaging/Interface.h>
#include <esl/messaging/Producer.h>

#include <cstdint>
#include <utility>
#include <string>
#include <string>

#include <librdkafka/rdkafka.h>

namespace rdkafka4esl {
namespace messaging {

class Client;

class Producer : public esl::messaging::Producer {
public:
	Producer(Client& client, const std::string& id, rd_kafka_t& producerRdKafkaHandle, rd_kafka_topic_t& rdKafkaTopic, const std::vector<std::pair<std::string, std::string>>& parameters);
	~Producer();

	void send(const void* data, std::size_t length, std::vector<std::pair<std::string, std::string>> parameters) override;
	void flush() override;
	bool wait(std::uint32_t ms) override;

private:
	Client& client;
	std::string id;

	std::string defaultKey;
	std::int32_t defaultPartition = RD_KAFKA_PARTITION_UA;


	rd_kafka_t& producerRdKafkaHandle;
	rd_kafka_topic_t& rdKafkaTopic;
};

} /* namespace messaging */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_MESSAGING_PRODUCER_H_ */
