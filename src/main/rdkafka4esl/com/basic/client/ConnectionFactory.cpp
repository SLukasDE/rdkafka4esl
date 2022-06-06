#include <rdkafka4esl/com/basic/client/ConnectionFactory.h>
#include <rdkafka4esl/com/basic/broker/Client.h>

#include <stdexcept>

namespace rdkafka4esl {
namespace com {
namespace basic {
namespace client {

std::unique_ptr<esl::com::basic::client::Interface::ConnectionFactory> ConnectionFactory::create(const std::vector<std::pair<std::string, std::string>>& settings) {
	return std::unique_ptr<esl::com::basic::client::Interface::ConnectionFactory>(new ConnectionFactory(settings));
}

ConnectionFactory::ConnectionFactory(const std::vector<std::pair<std::string, std::string>>& settings)
//: settings(aSettings)
{
	bool hasAcks = false;
	for(const auto& setting : settings) {
		if(setting.first == "broker-id") {
			brokerId = setting.second;
		    if(brokerId.empty()) {
		    	throw std::runtime_error("Invalid value for \"\" for key 'broker-id'");
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
			topicName = setting.second;
			if(topicName == "") {
				throw std::runtime_error("Invalid value \"\" for key 'topic'");
			}
		}
		else if(setting.first == "key") {
			key = setting.second;
		}
		else if(setting.first == "partition") {
			partition = std::stoi(setting.second);
		}
		else {
			throw std::runtime_error("Unknown key \"" + setting.first + "\"");
		}
	}

	if(hasAcks == false) {
		topicParameters.emplace_back("acks", "all");
	}

	if(brokerId.empty()) {
    	throw std::runtime_error("Key 'broker-id' is missing");
	}
	if(topicName.empty()) {
    	throw std::runtime_error("Key 'topic' is missing");
	}
}

void ConnectionFactory::initializeContext(esl::object::ObjectContext& objectContext) {
	broker::Client* client = objectContext.findObject<broker::Client>(brokerId);
	if(client == nullptr) {
    	throw std::runtime_error("Cannot find broker with id '" + brokerId + "'");
	}

	sharedConnectionFactory = std::unique_ptr<SharedConnectionFactory>(new SharedConnectionFactory(*client, topicParameters, topicName, key, partition));
	if(sharedConnectionFactory == nullptr) {
    	throw std::runtime_error("Cannot create shared connection factory for broker with id '" + brokerId + "'");
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
