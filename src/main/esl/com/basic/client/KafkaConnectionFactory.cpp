#include <esl/com/basic/client/KafkaConnectionFactory.h>
#include <esl/com/basic/client/ConnectionFactory.h>
#include <esl/utility/String.h>

#include <rdkafka4esl/com/basic/client/ConnectionFactory.h>

#include <esl/system/Stacktrace.h>

#include <stdexcept>

namespace esl {
inline namespace v1_6 {
namespace com {
namespace basic {
namespace client {

namespace {
std::tuple<std::unique_ptr<object::Object>, ConnectionFactory&, object::InitializeContext&> createKafkaConnectionFactory(const KafkaConnectionFactory::Settings& settings) {
	std::unique_ptr<rdkafka4esl::com::basic::client::ConnectionFactory> rdKafkaConnectionFactory(new rdkafka4esl::com::basic::client::ConnectionFactory(settings));

	ConnectionFactory& connectionFactory = *rdKafkaConnectionFactory;
	object::InitializeContext& initContext = *rdKafkaConnectionFactory;
	std::unique_ptr<object::Object> object(rdKafkaConnectionFactory.release());

	return std::tuple<std::unique_ptr<object::Object>, ConnectionFactory&, object::InitializeContext&>(std::move(object), connectionFactory, initContext);
}
}

KafkaConnectionFactory::Settings::Settings(const std::vector<std::pair<std::string, std::string>>& settings) {
	bool hasAcks = false;
	bool hasKey = false;
	bool hasPartition = false;

	for(const auto& setting : settings) {
		if(setting.first == "broker-id") {
			if(!brokerId.empty()) {
	            throw esl::system::Stacktrace::add(std::runtime_error("multiple definition of attribute 'broker-id'."));
			}

			brokerId = setting.second;
		    if(brokerId.empty()) {
		    	throw esl::system::Stacktrace::add(std::runtime_error("Invalid value for \"\" for key 'broker-id'"));
		    }
		}
		else if(setting.first.size() > 6 && setting.first.substr(0, 6) == "kafka.") {
			std::string kafkaKey = setting.first.substr(6);
			if(kafkaKey == "acks") {
				hasAcks = true;
			}
			topicParameters.emplace_back(kafkaKey, setting.second);
		}
		else if(setting.first == "topic") {
			if(!topicName.empty()) {
	            throw esl::system::Stacktrace::add(std::runtime_error("multiple definition of attribute 'topic'."));
			}
			topicName = setting.second;
			if(topicName.empty()) {
				throw esl::system::Stacktrace::add(std::runtime_error("Invalid value \"\" for key 'topic'"));
			}
		}
		else if(setting.first == "key") {
			if(hasKey) {
	            throw esl::system::Stacktrace::add(std::runtime_error("multiple definition of attribute 'key'."));
			}
			hasKey = true;
			key = setting.second;
		}
		else if(setting.first == "partition") {
			if(hasPartition) {
	            throw esl::system::Stacktrace::add(std::runtime_error("multiple definition of attribute 'partition'."));
			}
			hasPartition = true;
			partition = esl::utility::String::toInt(setting.second);
		}
		else {
			throw esl::system::Stacktrace::add(std::runtime_error("Unknown key \"" + setting.first + "\""));
		}
	}

	if(hasAcks == false) {
		topicParameters.emplace_back("acks", "all");
	}

	if(brokerId.empty()) {
    	throw esl::system::Stacktrace::add(std::runtime_error("Key 'broker-id' is missing"));
	}
	if(topicName.empty()) {
    	throw esl::system::Stacktrace::add(std::runtime_error("Key 'topic' is missing"));
	}
}

KafkaConnectionFactory::KafkaConnectionFactory(const Settings& settings)
: KafkaConnectionFactory(createKafkaConnectionFactory(settings))
{ }

KafkaConnectionFactory::KafkaConnectionFactory(std::tuple<std::unique_ptr<Object>, ConnectionFactory&, object::InitializeContext&> aObject)
: object(std::move(aObject))
{ }

std::unique_ptr<ConnectionFactory> KafkaConnectionFactory::create(const std::vector<std::pair<std::string, std::string>>& settings) {
	return std::unique_ptr<ConnectionFactory>(new KafkaConnectionFactory(Settings(settings)));
}

std::unique_ptr<Connection> KafkaConnectionFactory::createConnection() const {
	return std::get<1>(object).createConnection();
}

void KafkaConnectionFactory::initializeContext(object::Context& context) {
	std::get<2>(object).initializeContext(context);
}

} /* namespace client */
} /* namespace basic */
} /* namespace com */
} /* inline namespace v1_6 */
} /* namespace esl */
