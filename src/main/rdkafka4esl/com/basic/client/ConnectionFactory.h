#ifndef RDKAFKA4ESL_COM_BASIC_CLIENT_CONNECTIONFACTORY_H_
#define RDKAFKA4ESL_COM_BASIC_CLIENT_CONNECTIONFACTORY_H_

#include <rdkafka4esl/com/basic/client/SharedConnectionFactory.h>

#include <esl/com/basic/client/Interface.h>
#include <esl/com/basic/client/Connection.h>
#include <esl/object/InitializeContext.h>
#include <esl/object/Interface.h>
#include <esl/module/Interface.h>

#include <librdkafka/rdkafka.h>

#include <memory>
#include <string>

namespace rdkafka4esl {
namespace com {
namespace basic {
namespace broker {
class Client;
}
namespace client {

class ConnectionFactory : public virtual esl::com::basic::client::Interface::ConnectionFactory, public esl::object::InitializeContext {
public:
	static std::unique_ptr<esl::com::basic::client::Interface::ConnectionFactory> create(const esl::module::Interface::Settings& settings);

	static inline const char* getImplementation() {
		return "rdkafka4esl";
	}

	ConnectionFactory(const esl::module::Interface::Settings& settings);

	void initializeContext(esl::object::Interface::ObjectContext& objectContext) override;

	std::unique_ptr<esl::com::basic::client::Interface::Connection> createConnection() const override;

private:
	//const esl::module::Interface::Settings settings;
	std::string brokerId;
	std::string topicName;
	std::string key;
	std::int32_t partition = RD_KAFKA_PARTITION_UA;

	std::vector<std::pair<std::string, std::string>> topicParameters;

	std::shared_ptr<SharedConnectionFactory> sharedConnectionFactory;
	//std::unique_ptr<esl::com::basic::client::Interface::ConnectionFactory> connectionFactory;
};

} /* namespace client */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_COM_BASIC_CLIENT_CONNECTIONFACTORY_H_ */
