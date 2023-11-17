#ifndef RDKAFKA4ESL_COM_BASIC_CLIENT_CONNECTIONFACTORY_H_
#define RDKAFKA4ESL_COM_BASIC_CLIENT_CONNECTIONFACTORY_H_

#include <rdkafka4esl/com/basic/client/SharedConnectionFactory.h>

#include <esl/com/basic/client/Connection.h>
#include <esl/com/basic/client/ConnectionFactory.h>
#include <esl/com/basic/client/KafkaConnectionFactory.h>
#include <esl/object/Context.h>
#include <esl/object/InitializeContext.h>

#include <librdkafka/rdkafka.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace rdkafka4esl {
namespace com {
namespace basic {

namespace broker {
class Client;
}

namespace client {

class ConnectionFactory : public esl::com::basic::client::ConnectionFactory, public esl::object::InitializeContext {
public:
	ConnectionFactory(const esl::com::basic::client::KafkaConnectionFactory::Settings& settings);

	void initializeContext(esl::object::Context& objectContext) override;

	std::unique_ptr<esl::com::basic::client::Connection> createConnection() const override;

private:
	esl::com::basic::client::KafkaConnectionFactory::Settings settings;

	std::shared_ptr<SharedConnectionFactory> sharedConnectionFactory;
	//std::unique_ptr<esl::com::basic::client::Interface::ConnectionFactory> connectionFactory;
};

} /* namespace client */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_COM_BASIC_CLIENT_CONNECTIONFACTORY_H_ */
