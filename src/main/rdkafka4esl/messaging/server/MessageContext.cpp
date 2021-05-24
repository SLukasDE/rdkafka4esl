#include <rdkafka4esl/messaging/server/MessageContext.h>
#include <rdkafka4esl/messaging/server/Connection.h>

namespace rdkafka4esl {
namespace messaging {
namespace server {
namespace {
Connection connection;

std::string getTopicName(rd_kafka_message_t& kafkaMessage) {
	return kafkaMessage.rkt == nullptr ? "" : rd_kafka_topic_name(kafkaMessage.rkt);
}

std::string getOffset(rd_kafka_message_t& kafkaMessage) {
	return std::to_string(kafkaMessage.offset);
}

std::string getPartition(rd_kafka_message_t& kafkaMessage) {
	return std::to_string(kafkaMessage.partition);
}

std::string getKey(rd_kafka_message_t& kafkaMessage) {
	return kafkaMessage.key == nullptr ? "" : std::string(static_cast<char*>(kafkaMessage.key), kafkaMessage.key_len);
}

std::string getLength(rd_kafka_message_t& kafkaMessage) {
	return std::to_string(kafkaMessage.len);
}
}

MessageContext::MessageContext(const Socket& aSocket, rd_kafka_message_t& aKafkaMessage)
: socket(aSocket),
  kafkaMessage(aKafkaMessage)
{
	values.push_back(std::make_pair("topic", getTopicName(kafkaMessage)));
	values.push_back(std::make_pair("offset", getOffset(kafkaMessage)));
	values.push_back(std::make_pair("partition", getPartition(kafkaMessage)));
	values.push_back(std::make_pair("key", getKey(kafkaMessage)));
	values.push_back(std::make_pair("length", getLength(kafkaMessage)));
}

esl::messaging::server::Connection& MessageContext::getConnection() const {
	return connection;
}

bool MessageContext::hasValue(const std::string& key) const {
	if(key == "topic") {
		return kafkaMessage.rkt != nullptr;
	}
	else if(key == "offset") {
		return true;
	}
	else if(key == "partition") {
		return true;
	}
	else if(key == "key") {
		return kafkaMessage.key != nullptr;
	}
	else if(key == "length") {
		return true;
	}
	return false;
}

std::string MessageContext::getValue(const std::string& key) const {
	if(key == "topicName") {
		return getTopicName(kafkaMessage);
	}
	else if(key == "offset") {
		return getOffset(kafkaMessage);
	}
	else if(key == "partition") {
		return getPartition(kafkaMessage);
	}
	else if(key == "key") {
		return getKey(kafkaMessage);
	}
	else if(key == "length") {
		return getLength(kafkaMessage);
	}

	throw std::runtime_error("esl::object::Values: Unknown parameter key=\"" + key + "\"");
}

const std::vector<std::pair<std::string, std::string>>& MessageContext::getValues() const {
	return values;
}

esl::object::Interface::Object* MessageContext::findObject(const std::string& id) const {
	esl::messaging::server::Interface::Socket::ObjectFactory objectFactory = socket.getObjectFactory(id);
	if(objectFactory) {
		return objectFactory(*this);
	}
	return nullptr;
}

} /* namespace server */
} /* namespace messaging */
} /* namespace rdkafka4esl */
