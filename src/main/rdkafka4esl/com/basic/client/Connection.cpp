#include <rdkafka4esl/com/basic/client/Connection.h>
#include <rdkafka4esl/Logger.h>

#if 0
#include <rdkafka4esl/com/basic/client/ConnectionFactory.h>

#include <esl/io/Producer.h>
#include <esl/io/Writer.h>
#include <esl/Stacktrace.h>

#include <stdexcept>
#endif

namespace rdkafka4esl {
namespace com {
namespace basic {
namespace client {

namespace {
Logger logger("rdkafka4esl::com::basic::client::Connection");
}

#if 1
Connection::Connection(std::shared_ptr<SharedConnection> aSharedConnection)
: sharedConnection(aSharedConnection)
{ }

esl::com::basic::client::Response Connection::send(const esl::com::basic::client::Request& request, esl::io::Output output, esl::com::basic::client::Interface::CreateInput createInput) const {
	sharedConnection->send(request, std::move(output));
	return esl::com::basic::client::Response();
}

esl::com::basic::client::Response Connection::send(const esl::com::basic::client::Request& request, esl::io::Output output, esl::io::Input input) const {
	sharedConnection->send(request, std::move(output));
	return esl::com::basic::client::Response();
}
#else
namespace {
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

Connection::Connection(const ConnectionFactory& aConnectionFactory, rd_kafka_t& aProducerRdKafkaHandle, rd_kafka_topic_t& aRdKafkaTopic, const std::string& aTopicName, const std::string& aDefaultKey, std::int32_t aDefaultPartition)
: connectionFactory(aConnectionFactory),
  topicName(aTopicName),
  defaultKey(aDefaultKey),
  defaultPartition(aDefaultPartition),
  producerRdKafkaHandle(aProducerRdKafkaHandle),
  rdKafkaTopic(aRdKafkaTopic)
{
	connectionFactory.connectionRegister();
}

Connection::~Connection() {
	if(wait(0) == false) {
		logger.error << "Wait-Error on destroying connection for topic \"" << topicName << "\"\n";
	}
	rd_kafka_topic_destroy(&rdKafkaTopic);
	rd_kafka_destroy(&producerRdKafkaHandle);

	connectionFactory.connectionUnregister();
}

esl::com::basic::client::Response Connection::send(const esl::com::basic::client::Request& request, esl::io::Output output, esl::com::basic::client::Interface::CreateInput createInput) const {
	send(request, std::move(output));
	return esl::com::basic::client::Response();
}

esl::com::basic::client::Response Connection::send(const esl::com::basic::client::Request& request, esl::io::Output output, esl::io::Input input) const {
	send(request, std::move(output));
	return esl::com::basic::client::Response();
}

void Connection::send(const esl::com::basic::client::Request& request, esl::io::Output output) const {
	if(!output) {
		return;
	}

	std::string key = defaultKey;
	std::int32_t partition = defaultPartition;

	for(const auto& parameter : request) {
		if(parameter.first == "key") {
			key = parameter.second;
		}
		else if(parameter.first == "partition") {
			partition = std::stoi(parameter.second);
		}
		else {
			throw esl::addStacktrace(std::runtime_error("Unknown parameter \"" + parameter.first + "\" = \"" + parameter.second  + "\""));
		}
	}

	ToStringWriter toStringWriter;
	esl::io::Producer& producer = output.getProducer();
	while(producer.produce(toStringWriter) != esl::io::Writer::npos){}

	const char* keyPtr = key.c_str();
	if(key.empty()) {
		keyPtr = nullptr;
	}
	int rc = rd_kafka_produce(&rdKafkaTopic, partition, RD_KAFKA_MSG_F_COPY, const_cast<char*>(toStringWriter.getString().data()), toStringWriter.getString().size(), keyPtr, key.size(), nullptr);

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

	//return esl::io::Output();
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
#endif

} /* namespace client */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */
