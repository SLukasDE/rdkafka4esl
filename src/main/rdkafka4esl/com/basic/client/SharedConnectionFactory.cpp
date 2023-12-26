#include <rdkafka4esl/com/basic/client/SharedConnectionFactory.h>
#include <rdkafka4esl/com/basic/client/SharedConnection.h>
#include <rdkafka4esl/com/basic/client/SharedConnectionFactory.h>
#include <rdkafka4esl/com/basic/client/Connection.h>
#include <rdkafka4esl/object/Client.h>

#include <esl/system/Stacktrace.h>

#include <librdkafka/rdkafka.h>

#include <stdexcept>


namespace rdkafka4esl {
namespace com {
namespace basic {
namespace client {

SharedConnectionFactory::SharedConnectionFactory(object::Client& aClient, const std::vector<std::pair<std::string, std::string>>& aTopicSettings,
		const std::string& aTopicName, const std::string& aKey, std::int32_t aPartition)
: client(aClient),
  kafkaSettings(client.getKafkaSettings()),
  topicParameters(aTopicSettings),
  topicName(aTopicName),
  key(aKey),
  partition(aPartition)
{
	client.registerConnectionFactory(this);
}

SharedConnectionFactory::~SharedConnectionFactory() {
	std::unique_lock<std::mutex> connectionWaitNotifyLock(connectionWaitNotifyMutex);
	connectionWaitCondVar.wait(connectionWaitNotifyLock, [this] {
			std::lock_guard<std::mutex> producerCountLock(sharedConnectionsMutex);
			return sharedConnections.empty();
	});

	client.unregisterConnectionFactory(this);
}

std::unique_ptr<esl::com::basic::client::Connection> SharedConnectionFactory::createConnection(std::shared_ptr<SharedConnectionFactory> sharedConnectionFactory) const {
	char errstr[512];
	rd_kafka_t* producerRdKafkaHandle = nullptr;
	rd_kafka_topic_conf_t* rdKafkaTopicConfig = nullptr;
	rd_kafka_topic_t* rdKafkaTopic = nullptr;

	/* *************************
	 * Create Producer Handle
	 * *************************/

	producerRdKafkaHandle = rd_kafka_new(RD_KAFKA_PRODUCER, &object::Client::createConfig(kafkaSettings), errstr, sizeof(errstr));
	if(producerRdKafkaHandle == nullptr) {
		throw esl::system::Stacktrace::add(std::runtime_error(std::string("rd_kafka_new(RD_KAFKA_PRODUCER, ...) failed: ") + errstr));
	}


	/* *************************
	 * Create Topic Config
	 * *************************/

	rdKafkaTopicConfig = rd_kafka_topic_conf_new();
	if(rdKafkaTopicConfig == nullptr) {
		/* Destroy handle object */
		rd_kafka_destroy(producerRdKafkaHandle);

		throw esl::system::Stacktrace::add(std::runtime_error("rd_kafka_topic_conf_new() failed"));
	}

	for(const auto& topicParameter : topicParameters) {
		if(rd_kafka_topic_conf_set(rdKafkaTopicConfig, topicParameter.first.c_str(), topicParameter.second.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
			rd_kafka_topic_conf_destroy(rdKafkaTopicConfig);

			/* Destroy handle object */
			rd_kafka_destroy(producerRdKafkaHandle);

			throw esl::system::Stacktrace::add(std::runtime_error(errstr));
		}
	}

	/* *************************
	 * Create Topic Handle
	 * *************************/

	rdKafkaTopic = rd_kafka_topic_new(producerRdKafkaHandle, topicName.c_str(), rdKafkaTopicConfig);
	if(rdKafkaTopic == nullptr) {
		rd_kafka_topic_conf_destroy(rdKafkaTopicConfig);

		/* Destroy handle object */
		rd_kafka_destroy(producerRdKafkaHandle);

		throw esl::system::Stacktrace::add(std::runtime_error("rd_kafka_topic_new failed for topic \"" + topicName + "\""));
	}

	//return std::unique_ptr<esl::com::basic::client::Interface::Connection>(new Connection(*this, *producerRdKafkaHandle, *rdKafkaTopic, topicName, key, partition));
	std::shared_ptr<SharedConnection> sharedConnection(new SharedConnection(sharedConnectionFactory, *producerRdKafkaHandle, *rdKafkaTopic, topicName, key, partition));
	return std::unique_ptr<esl::com::basic::client::Connection>(new Connection(sharedConnection));
}

void SharedConnectionFactory::connectionRegister(SharedConnection* sharedConnection) {
	std::lock_guard<std::mutex> sharedConnectionsLock(sharedConnectionsMutex);
	sharedConnections.insert(sharedConnection);

}
void SharedConnectionFactory::connectionUnregister(SharedConnection* sharedConnection) {
	std::lock_guard<std::mutex> sharedConnectionsLock(sharedConnectionsMutex);
	sharedConnections.erase(sharedConnection);
}

//called by broker::Client
void SharedConnectionFactory::release() {
	std::lock_guard<std::mutex> sharedConnectionsLock(sharedConnectionsMutex);
	for(auto sharedConnection : sharedConnections) {
		sharedConnection->release();
	}
}

} /* namespace client */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */
