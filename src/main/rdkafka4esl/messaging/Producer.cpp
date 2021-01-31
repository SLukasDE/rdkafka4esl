#include <rdkafka4esl/messaging/Producer.h>
#include <rdkafka4esl/messaging/Client.h>
#include <rdkafka4esl/messaging/Logger.h>

#include <esl/Stacktrace.h>

#include <stdexcept>

namespace rdkafka4esl {
namespace messaging {

namespace {
Logger logger("kafka4esl::messaging::Producer");
}

Producer::Producer(Client& aClient, const std::string& aId, rd_kafka_t& aProducerRdKafkaHandle, rd_kafka_topic_t& aRdKafkaTopic, const std::vector<std::pair<std::string, std::string>>& parameters)
: esl::messaging::Producer(),
  client(aClient),
  id(aId),
  producerRdKafkaHandle(aProducerRdKafkaHandle),
  rdKafkaTopic(aRdKafkaTopic)
{
	client.createdProducer();
	for(const auto& parameter : parameters) {
		if(parameter.first == "key") {
			defaultKey = parameter.second;
		}
		else if(parameter.first == "partition") {
			defaultPartition = std::stoi(parameter.second);
		}
	}
}

Producer::~Producer() {
	if(wait(0) == false) {
		logger.error << "Wait-Error on destroying producer for topic \"" << id << "\"\n";
	}
	rd_kafka_topic_destroy(&rdKafkaTopic);
	rd_kafka_destroy(&producerRdKafkaHandle);

	client.destroyedProducer();
}

void Producer::send(const void* data, std::size_t length, std::vector<std::pair<std::string, std::string>> parameters) {
	std::string key = defaultKey;
	std::int32_t partition = defaultPartition;

	for(const auto& parameter : parameters) {
		if(parameter.first == "key") {
			key = parameter.second;
		}
		else if(parameter.first == "partition") {
			partition = std::stoi(parameter.second);
		}
	}

	int rc = rd_kafka_produce(&rdKafkaTopic, partition, RD_KAFKA_MSG_F_COPY, const_cast<void*>(data), length, key.c_str(), key.size(), nullptr);

	if(rc == -1) {
		switch(errno) {
		case ENOBUFS:
			logger.error << "Sending message to kafka topic \"" << id << "\" failed. (errno == ENOBUFS)\n";
			logger.error << "- maximum number of outstanding messages has been reached:\n";
			logger.error << "  \"queue.buffering.max.messages\"\n";
			logger.error << "  (RD_KAFKA_RESP_ERR__QUEUE_FULL)\n";
			throw esl::addStacktrace(std::runtime_error("Sending message to kafka topic \"" + id + "\" failed. (errno == ENOBUFS)"));
			break;
		case EMSGSIZE:
			logger.error << "Sending message to kafka topic \"" << id << "\" failed. (errno == EMSGSIZE)\n";
			logger.error << "- message is larger than configured max size:\n";
			logger.error << "  \"messages.max.bytes\"\n";
			logger.error << "  (RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE)\n";
			throw esl::addStacktrace(std::runtime_error("Sending message to kafka topic \"" + id + "\" failed. (errno == EMSGSIZE)"));
			break;
		case ESRCH:
			logger.error << "Sending message to kafka topic \"" << id << "\" failed. (errno == ESRCH)\n";
			logger.error << "- requested partition is unknown in the Kafka cluster.\n";
			logger.error << "  (RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION)\n";
			throw esl::addStacktrace(std::runtime_error("Sending message to kafka topic \"" + id + "\" failed. (errno == ESRCH)"));
		case ENOENT:
			logger.error << "Sending message to kafka topic \"" << id << "\" failed. (errno == ENOENT)\n";
			logger.error << "- topic is unknown in the Kafka cluster.\n";
			logger.error << "  (RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)\n";
			throw esl::addStacktrace(std::runtime_error("Sending message to kafka topic \"" + id + "\" failed. (errno == ENOENT)"));
		default:
			break;
		}
		throw esl::addStacktrace(std::runtime_error("Sending message to kafka topic \"" + id + "\" failed."));
		//throw esl::addStacktrace(std::runtime_error(rd_kafka_err2str(rd_kafka_errno2err(errno))));
	}
}

void Producer::flush() {
	rd_kafka_flush(&producerRdKafkaHandle, 0);
}

bool Producer::wait(std::uint32_t ms) {
	if(ms == 0) {
		while(true) {
			rd_kafka_resp_err_t rc = rd_kafka_flush(&producerRdKafkaHandle, 1000);
			if(rc == RD_KAFKA_RESP_ERR_NO_ERROR) {
				return true;
			}
			if(rc != RD_KAFKA_RESP_ERR__TIMED_OUT) {
				return false;
			}
		};
	}

	return rd_kafka_flush(&producerRdKafkaHandle, ms) == RD_KAFKA_RESP_ERR_NO_ERROR;
}

} /* namespace messaging */
} /* namespace rdkafka4esl */
