#include <rdkafka4esl/com/basic/server/Request.h>

#include <esl/stacktrace/Stacktrace.h>

#include <stdexcept>

namespace rdkafka4esl {
namespace com {
namespace basic {
namespace server {

namespace {
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
} /* anonymous namespace */


Request::Request(rd_kafka_message_t& aKafkaMessage)
: kafkaMessage(aKafkaMessage)
{
	values.push_back(std::make_pair("topic", getTopicName(kafkaMessage)));
	values.push_back(std::make_pair("offset", getOffset(kafkaMessage)));
	values.push_back(std::make_pair("partition", getPartition(kafkaMessage)));
	values.push_back(std::make_pair("key", getKey(kafkaMessage)));
	values.push_back(std::make_pair("length", getLength(kafkaMessage)));
}

bool Request::hasValue(const std::string& key) const {
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

std::string Request::getValue(const std::string& key) const {
	if(key == "topic") {
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

	throw esl::stacktrace::Stacktrace::add(std::runtime_error("rdkafka4esl::com::basic::server::Request: Unknown parameter key=\"" + key + "\""));
}

const std::vector<std::pair<std::string, std::string>>& Request::getValues() const {
	return values;
}

} /* namespace server */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */
