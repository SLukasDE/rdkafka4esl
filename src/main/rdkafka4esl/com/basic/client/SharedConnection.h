#ifndef RDKAFKA4ESL_COM_BASIC_CLIENT_SHAREDCONNECTION_H_
#define RDKAFKA4ESL_COM_BASIC_CLIENT_SHAREDCONNECTION_H_

#include <esl/com/basic/client/Interface.h>
#include <esl/com/basic/client/Request.h>
#include <esl/com/basic/client/Response.h>
#include <esl/io/Input.h>
#include <esl/io/Output.h>

#include <vector>
#include <string>
#include <utility>
#include <cstdint>
#include <memory>

#include <librdkafka/rdkafka.h>

namespace rdkafka4esl {
namespace com {
namespace basic {
namespace client {

class SharedConnectionFactory;

class SharedConnection final {
public:
	SharedConnection(std::shared_ptr<SharedConnectionFactory> sharedConnectionFactory, rd_kafka_t& producerRdKafkaHandle, rd_kafka_topic_t& rdKafkaTopic, const std::string& topicName, const std::string& defaultKey, std::int32_t defaultPartition);
	~SharedConnection();

	void send(const esl::com::basic::client::Request& request, esl::io::Output output) const;

	//called by SharedConnectionFactory
	void release();

/*
	void flush();// override;
	bool wait(std::uint32_t ms);// override;
*/
private:
	bool isReleasing = false;
	std::shared_ptr<SharedConnectionFactory> sharedConnectionFactory;
	const std::string topicName;
	const std::string defaultKey;
	const std::int32_t defaultPartition;

	rd_kafka_t& producerRdKafkaHandle;
	rd_kafka_topic_t& rdKafkaTopic;
};

} /* namespace client */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_COM_BASIC_CLIENT_SHAREDCONNECTION_H_ */
