#include <rdkafka4esl/messaging/client/Connection.h>
#include <rdkafka4esl/messaging/broker/Client.h>
#include <rdkafka4esl/messaging/Logger.h>

#include <esl/io/Producer.h>
#include <esl/io/Writer.h>
#include <esl/Stacktrace.h>

#include <stdexcept>

namespace rdkafka4esl {
namespace messaging {
namespace client {
namespace {
Logger logger("rdkafka4esl::messaging::client::Connection");

class ToStringWriter : public esl::io::Writer {
public:
	std::size_t write(const void* data, std::size_t size) override;
	std::size_t getSizeWritable() const override;

	const std::string getString() const noexcept;

private:
	std::string str;
};

// if function is called with size=0, this signals that writing is done, so write will not be called anymore.
// -> this can be used for cleanup stuff.
// returns consumed bytes.
// npos is returned if writer will not consume anymore.
std::size_t ToStringWriter::write(const void* data, std::size_t size) {
	if(size == 0 || size == npos) {
		return npos;
	}

	str += std::string(static_cast<const char*>(data), size);
	return size;
}

// returns consumable bytes to write.
// npos is returned if available size is unknown.
std::size_t ToStringWriter::getSizeWritable() const {
	return npos;
}

const std::string ToStringWriter::getString() const noexcept {
	return str;
}

}

Connection::Connection(broker::Client& aClient, rd_kafka_t& aProducerRdKafkaHandle, rd_kafka_topic_t& aRdKafkaTopic, const std::vector<std::pair<std::string, std::string>>& parameters)
: client(aClient),
  producerRdKafkaHandle(aProducerRdKafkaHandle),
  rdKafkaTopic(aRdKafkaTopic)
{
	client.connectionRegister();
	for(const auto& parameter : parameters) {
		if(parameter.first == "key") {
			defaultKey = parameter.second;
		}
		else if(parameter.first == "partition") {
			defaultPartition = std::stoi(parameter.second);
		}
		else if(parameter.first == "topic") {
			topicName = parameter.second;
		}
	}
}

Connection::~Connection() {
	if(wait(0) == false) {
		logger.error << "Wait-Error on destroying connection for topic \"" << topicName << "\"\n";
	}
	rd_kafka_topic_destroy(&rdKafkaTopic);
	rd_kafka_destroy(&producerRdKafkaHandle);

	client.connectionUnregister();
}

esl::io::Output Connection::sendMessage(esl::io::Output output, std::vector<std::pair<std::string, std::string>> parameters) {
	if(!output) {
		return esl::io::Output();
	}

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

	ToStringWriter toStringWriter;
	esl::io::Producer& producer = output.getProducer();
	while(producer.produce(toStringWriter) != esl::io::Writer::npos){}

	int rc = rd_kafka_produce(&rdKafkaTopic, partition, RD_KAFKA_MSG_F_COPY, const_cast<char*>(toStringWriter.getString().data()), toStringWriter.getString().size(), key.c_str(), key.size(), nullptr);

	if(rc == -1) {
		switch(errno) {
		case ENOBUFS:
			logger.error << "Sending message to kafka topic \"" << topicName << "\" failed. (errno == ENOBUFS)\n";
			logger.error << "- maximum number of outstanding messages has been reached:\n";
			logger.error << "  \"queue.buffering.max.messages\"\n";
			logger.error << "  (RD_KAFKA_RESP_ERR__QUEUE_FULL)\n";
			throw esl::addStacktrace(std::runtime_error("Sending message to kafka topic \"" + topicName + "\" failed. (errno == ENOBUFS)"));
			break;
		case EMSGSIZE:
			logger.error << "Sending message to kafka topic \"" << topicName << "\" failed. (errno == EMSGSIZE)\n";
			logger.error << "- message is larger than configured max size:\n";
			logger.error << "  \"messages.max.bytes\"\n";
			logger.error << "  (RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE)\n";
			throw esl::addStacktrace(std::runtime_error("Sending message to kafka topic \"" + topicName + "\" failed. (errno == EMSGSIZE)"));
			break;
		case ESRCH:
			logger.error << "Sending message to kafka topic \"" << topicName << "\" failed. (errno == ESRCH)\n";
			logger.error << "- requested partition is unknown in the Kafka cluster.\n";
			logger.error << "  (RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION)\n";
			throw esl::addStacktrace(std::runtime_error("Sending message to kafka topic \"" + topicName + "\" failed. (errno == ESRCH)"));
		case ENOENT:
			logger.error << "Sending message to kafka topic \"" << topicName << "\" failed. (errno == ENOENT)\n";
			logger.error << "- topic is unknown in the Kafka cluster.\n";
			logger.error << "  (RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)\n";
			throw esl::addStacktrace(std::runtime_error("Sending message to kafka topic \"" + topicName + "\" failed. (errno == ENOENT)"));
		default:
			break;
		}
		throw esl::addStacktrace(std::runtime_error("Sending message to kafka topic \"" + topicName + "\" failed."));
		//throw esl::addStacktrace(std::runtime_error(rd_kafka_err2str(rd_kafka_errno2err(errno))));
	}

	return esl::io::Output();
}

void Connection::flush() {
	if(rd_kafka_flush(&producerRdKafkaHandle, 0) == RD_KAFKA_RESP_ERR_NO_ERROR) {

	}
}

bool Connection::wait(std::uint32_t ms) {
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

} /* namespace client */
} /* namespace messaging */
} /* namespace rdkafka4esl */
