#include <rdkafka4esl/com/basic/client/ConnectionFactory.h>
#include <rdkafka4esl/com/basic/broker/Client.h>

#include <esl/system/Stacktrace.h>
#include <esl/utility/String.h>

#include <stdexcept>

namespace rdkafka4esl {
namespace com {
namespace basic {
namespace client {

std::unique_ptr<esl::com::basic::client::ConnectionFactory> ConnectionFactory::create(const std::vector<std::pair<std::string, std::string>>& settings) {
	return std::unique_ptr<esl::com::basic::client::ConnectionFactory>(new ConnectionFactory(settings));
}

ConnectionFactory::ConnectionFactory(const std::vector<std::pair<std::string, std::string>>& settings) {
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

void ConnectionFactory::initializeContext(esl::object::Context& objectContext) {
	broker::Client* client = objectContext.findObject<broker::Client>(brokerId);
	if(client == nullptr) {
    	throw esl::system::Stacktrace::add(std::runtime_error("Cannot find broker with id '" + brokerId + "'"));
	}

	sharedConnectionFactory = std::unique_ptr<SharedConnectionFactory>(new SharedConnectionFactory(*client, topicParameters, topicName, key, partition));
	if(sharedConnectionFactory == nullptr) {
    	throw esl::system::Stacktrace::add(std::runtime_error("Cannot create shared connection factory for broker with id '" + brokerId + "'"));
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
