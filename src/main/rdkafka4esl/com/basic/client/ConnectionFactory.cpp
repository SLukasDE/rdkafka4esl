#include <rdkafka4esl/com/basic/client/ConnectionFactory.h>
#include <rdkafka4esl/object/Client.h>

#include <esl/object/KafkaClient.h>
#include <esl/system/Stacktrace.h>
#include <esl/utility/String.h>

#include <stdexcept>

namespace rdkafka4esl {
namespace com {
namespace basic {
namespace client {

ConnectionFactory::ConnectionFactory(const esl::com::basic::client::KafkaConnectionFactory::Settings& aSettings)
: settings(aSettings)
{

	if(settings.brokerId.empty()) {
    	throw esl::system::Stacktrace::add(std::runtime_error("Key 'broker-id' is missing"));
	}
	if(settings.topicName.empty()) {
    	throw esl::system::Stacktrace::add(std::runtime_error("Key 'topic' is missing"));
	}
}

void ConnectionFactory::initializeContext(esl::object::Context& objectContext) {
	object::Client* client = objectContext.findObject<object::Client>(settings.brokerId);
	if(client == nullptr) {
		esl::object::KafkaClient* rdkafkaClient = objectContext.findObject<esl::object::KafkaClient>(settings.brokerId);
		if(rdkafkaClient) {
			client = &rdkafkaClient->getClient();
		}
	}
	if(client == nullptr) {
    	throw esl::system::Stacktrace::add(std::runtime_error("Cannot find broker with id '" + settings.brokerId + "'"));
	}

	sharedConnectionFactory = std::unique_ptr<SharedConnectionFactory>(new SharedConnectionFactory(*client, settings.topicParameters, settings.topicName, settings.key, settings.partition));
	if(sharedConnectionFactory == nullptr) {
    	throw esl::system::Stacktrace::add(std::runtime_error("Cannot create shared connection factory for broker with id '" + settings.brokerId + "'"));
	}
}

std::unique_ptr<esl::com::basic::client::Connection> ConnectionFactory::createConnection() const {
	if(sharedConnectionFactory == nullptr) {
		return nullptr;
	}
	return sharedConnectionFactory->createConnection(sharedConnectionFactory);
}

} /* namespace client */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */
