#ifndef RDKAFKA4ESL_COM_BASIC_CLIENT_CONNECTION_H_
#define RDKAFKA4ESL_COM_BASIC_CLIENT_CONNECTION_H_

#if 1
#include <rdkafka4esl/com/basic/client/SharedConnection.h>

#include <esl/com/basic/client/Interface.h>
#include <esl/com/basic/client/Request.h>
#include <esl/com/basic/client/Response.h>
#include <esl/io/Input.h>
#include <esl/io/Output.h>

#include <memory>
#else
#include <esl/com/basic/client/Interface.h>
#include <esl/com/basic/client/Request.h>
#include <esl/com/basic/client/Response.h>
#include <esl/io/Input.h>
#include <esl/io/Output.h>

#include <vector>
#include <string>
#include <utility>
#include <cstdint>

#include <librdkafka/rdkafka.h>
#endif

namespace rdkafka4esl {
namespace com {
namespace basic {
namespace client {

#if 1
class Connection final : public esl::com::basic::client::Interface::Connection {
public:
	Connection(std::shared_ptr<SharedConnection> sharedConnection);

	esl::com::basic::client::Response send(const esl::com::basic::client::Request& request, esl::io::Output output, esl::com::basic::client::Interface::CreateInput createInput) const override;
	esl::com::basic::client::Response send(const esl::com::basic::client::Request& request, esl::io::Output output, esl::io::Input input) const override;

private:
	std::shared_ptr<SharedConnection> sharedConnection;
};

#else
class ConnectionFactory;

class Connection final : public esl::com::basic::client::Interface::Connection {
public:
	Connection(const ConnectionFactory& connectionFactory, rd_kafka_t& producerRdKafkaHandle, rd_kafka_topic_t& rdKafkaTopic, const std::string& topicName, const std::string& defaultKey, std::int32_t defaultPartition);
	~Connection();

	esl::com::basic::client::Response send(const esl::com::basic::client::Request& request, esl::io::Output output, esl::com::basic::client::Interface::CreateInput createInput) const override;
	esl::com::basic::client::Response send(const esl::com::basic::client::Request& request, esl::io::Output output, esl::io::Input input) const override;

	void flush();// override;
	bool wait(std::uint32_t ms);// override;

private:
	const ConnectionFactory& connectionFactory;
	const std::string topicName;
	const std::string defaultKey;
	const std::int32_t defaultPartition;

	rd_kafka_t& producerRdKafkaHandle;
	rd_kafka_topic_t& rdKafkaTopic;

	void send(const esl::com::basic::client::Request& request, esl::io::Output output) const;
};
#endif

} /* namespace client */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_COM_BASIC_CLIENT_CONNECTION_H_ */
