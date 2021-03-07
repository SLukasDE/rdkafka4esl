#include <rdkafka4esl/messaging/Client.h>
#include <rdkafka4esl/messaging/Producer.h>
#include <rdkafka4esl/messaging/ProducerFactory.h>
#include <rdkafka4esl/messaging/Message.h>
#include <rdkafka4esl/messaging/MessageContext.h>
#include <rdkafka4esl/messaging/MessageReader.h>
#include <rdkafka4esl/messaging/Logger.h>

#include <esl/Stacktrace.h>

#include <string>
#include <map>
#include <stdexcept>

namespace rdkafka4esl {
namespace messaging {

namespace {
Logger logger("kafka4esl::messaging::Client");
}

std::unique_ptr<esl::messaging::Interface::Client> Client::create(const std::string& brokers, const esl::object::Values<std::string>& settings) {
	return std::unique_ptr<esl::messaging::Interface::Client>(new Client(brokers, settings));
}

Client::Client(const std::string& brokers, const esl::object::Values<std::string>& aSettings)
: esl::messaging::Interface::Client(),
  settings(aSettings.getValues()),
  consumer(*this)
{
	bool hasGroupId = false;
	bool hasBrokers = false;

	for(auto& setting : settings) {
		if(setting.first == "group.id") {
			if(setting.second.empty()) {
				continue;
			}
			hasGroupId = true;
		}
		else if(setting.first == "bootstrap.servers") {
			if(setting.second.empty()) {
				if(brokers.empty()) {
					continue;
				}
			}
			else if(setting.second != brokers) {
				logger.warn << "Overwriting \"bootstrap.servers\"=\"" << setting.second << "\" with value from brokers=\"" << brokers << "\".\n";
			}

			setting.second = brokers;
			hasBrokers = true;
		}
	}

	if(!hasBrokers && !brokers.empty()) {
		settings.emplace_back("bootstrap.servers", brokers);
		hasBrokers = true;
	}

	if(!hasGroupId) {
		logger.warn << "Value \"group.id\" not specified.\n";
	}
	if(!hasBrokers) {
		logger.warn << "Value \"bootstrap.servers\" not specified.\n";
	}


	logger.debug << "Begin show settings:\n";
	for(const auto& setting : settings) {
		logger.debug << "- \"" << setting.first << "\"=\"" << setting.second << "\"\n";
	}
	logger.debug << "End show settings.\n";
}

Client::~Client() {
	consumerStop();

	std::unique_lock<std::mutex> producerWaitNotifyLock(producerWaitNotifyMutex);
	producerWaitCondVar.wait(producerWaitNotifyLock, std::bind(&Client::producerIsEmpty, this));

	consumerWait(0);
}

void Client::addObjectFactory(const std::string& id, Client::ObjectFactory objectFactory) {
	if(consumerIsStateNotRunning() == false) {
		throw esl::addStacktrace(std::runtime_error("Calling Client::addObjectFactory not allowed, because Kafka-Client is already listening"));
	}

	consumerObjectFactories[id] = objectFactory;
}

Client::ObjectFactory Client::getObjectFactory(const std::string& id) const {
	auto iter = consumerObjectFactories.find(id);
	if(iter != std::end(consumerObjectFactories)) {
		return iter->second;
	}
	return nullptr;
}

esl::messaging::Consumer& Client::getConsumer() {
	return consumer;
}

std::unique_ptr<esl::messaging::Producer> Client::createProducer(const std::string& id, std::vector<std::pair<std::string, std::string>> parameter) {
	char errstr[512];
	rd_kafka_t* producerRdKafkaHandle = nullptr;
	rd_kafka_topic_conf_t* rdKafkaTopicConfig = nullptr;
	rd_kafka_topic_t* rdKafkaTopic = nullptr;

	/* Create Kafka producer handle */
	producerRdKafkaHandle = rd_kafka_new(RD_KAFKA_PRODUCER, &createConfig(), errstr, sizeof(errstr));
	if(producerRdKafkaHandle == nullptr) {
		throw esl::addStacktrace(std::runtime_error(errstr));
	}

	rdKafkaTopicConfig = rd_kafka_topic_conf_new();
	if(rdKafkaTopicConfig == nullptr) {
		/* Destroy handle object */
		rd_kafka_destroy(producerRdKafkaHandle);

		throw esl::addStacktrace(std::runtime_error("rd_kafka_topic_conf_new() failed"));
	}

	if(rd_kafka_topic_conf_set(rdKafkaTopicConfig, "acks", "all", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		rd_kafka_topic_conf_destroy(rdKafkaTopicConfig);

		/* Destroy handle object */
		rd_kafka_destroy(producerRdKafkaHandle);

		throw esl::addStacktrace(std::runtime_error(errstr));
	}

	rdKafkaTopic = rd_kafka_topic_new(producerRdKafkaHandle, id.c_str(), rdKafkaTopicConfig);
	if(rdKafkaTopic == nullptr) {
		rd_kafka_topic_conf_destroy(rdKafkaTopicConfig);

		/* Destroy handle object */
		rd_kafka_destroy(producerRdKafkaHandle);

		throw esl::addStacktrace(std::runtime_error("rd_kafka_topic_new failed for topic \"" + id + "\""));
	}

	return std::unique_ptr<esl::messaging::Producer>(new Producer(*this, id, *producerRdKafkaHandle, *rdKafkaTopic, parameter));
}

std::unique_ptr<esl::messaging::Interface::ProducerFactory> Client::createProducerFactory(const std::string& id, std::vector<std::pair<std::string, std::string>> parameters) {
	return std::unique_ptr<esl::messaging::Interface::ProducerFactory>(new ProducerFactory(*this, id, std::move(parameters)));
}

rd_kafka_conf_t& Client::createConfig() const {
	char errstr[512];

	/* Note:
	 * The rd_kafka.._conf_t objects are not reusable after they have been passed to rd_kafka.._new().
	 * The application does not need to free any config resources after a rd_kafka.._new() call.
	 */
	rd_kafka_conf_t* rdKafkaConfig(rd_kafka_conf_new());

	if(rdKafkaConfig == nullptr) {
		throw esl::addStacktrace(std::runtime_error("Failed to create kafka configuration object"));
	}

	if(rd_kafka_conf_set(rdKafkaConfig, "client.id", "rdkafka4esl", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		throw esl::addStacktrace(std::runtime_error(errstr));
	}

	/*
	brokers = "localhost:9092";
	if(rd_kafka_conf_set(rdKafkaConfig, "bootstrap.servers", brokers.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		throw esl::addStacktrace(std::runtime_error(errstr));
	}
	if(rd_kafka_conf_set(rdKafkaConfig, "group.id", "foo", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		throw esl::addStacktrace(std::runtime_error(errstr));
	}
	*/
	for(const auto& setting : settings) {
		if(rd_kafka_conf_set(rdKafkaConfig, setting.first.c_str(), setting.second.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
			throw esl::addStacktrace(std::runtime_error(errstr));
		}
	}

	return *rdKafkaConfig;
}

void Client::createdProducer() {
	std::lock_guard<std::mutex> producerCountLock(producerCountMutex);
	++producerCount;
}

void Client::destroyedProducer() {
	{
		std::lock_guard<std::mutex> producerCountLock(producerCountMutex);
		--producerCount;
	}

	producerWaitCondVar.notify_one();
}

bool Client::producerIsEmpty() {
	std::lock_guard<std::mutex> producerCountLock(producerCountMutex);
	return producerCount == 0;
}

void Client::consumerStart(const std::set<std::string>& queues, esl::messaging::messagehandler::Interface::CreateMessageHandler createMessageHandler, std::uint16_t numThreads, bool stopIfEmpty) {
	if(queues.empty()) {
		return;
	}

	{
		std::lock_guard<std::mutex> consumerThreadLock(consumerThreadMutex);

		if(consumerState != CSNotRunning) {
			return;
		}
		consumerState = CSRunning;

		consumerThreadsMax = numThreads > 0 ? numThreads : 1;
	}

	std::thread consumerThread([this, queues, createMessageHandler, stopIfEmpty]{
		consumerStartThread(queues, createMessageHandler, stopIfEmpty);
	});
	consumerThread.detach();
}

void Client::consumerStartThread(const std::set<std::string>& queues, esl::messaging::messagehandler::Interface::CreateMessageHandler createMessageHandler, bool stopIfEmpty) {
	/* ************** *
	 * Initialization *
	 * ************** */
	{
		std::lock_guard<std::mutex> consumerThreadLock(consumerThreadMutex);
		rd_kafka_resp_err_t err;
		char errstr[512];

		/* Create subscription for specified topics */
		consumerRdKafkaSubscription = rd_kafka_topic_partition_list_new(queues.size());
		if(consumerRdKafkaSubscription == nullptr) {
			throw esl::addStacktrace(std::runtime_error("Cannot create kafka consumer subscription object"));
		}
		for(const auto& queue : queues) {
			rd_kafka_topic_partition_list_add(consumerRdKafkaSubscription, queue.c_str(), RD_KAFKA_PARTITION_UA);
		}



		/* Create Kafka consumer handle */
		consumerRdKafkaHandle = rd_kafka_new(RD_KAFKA_CONSUMER, &createConfig(), errstr, sizeof(errstr));
		if(consumerRdKafkaHandle == nullptr) {
			rd_kafka_topic_partition_list_destroy(consumerRdKafkaSubscription);
			consumerRdKafkaSubscription = nullptr;

			throw esl::addStacktrace(std::runtime_error(errstr));
		}



		/* Subscribe */
		if ((err = rd_kafka_subscribe(consumerRdKafkaHandle, consumerRdKafkaSubscription))) {
			std::string errorStr = rd_kafka_err2str(err);

			/* NOTE: There is no need to unsubscribe prior to calling rd_kafka_consumer_close() */
			//rd_kafka_unsubscribe(rdkConsumerHandle);

			/* 1) Leave the consumer group, commit final offsets, etc. */
			err = rd_kafka_consumer_close(consumerRdKafkaHandle);
			if (err) {
				logger.error << "Failed to close consumer: " << rd_kafka_err2str(err) << "\n";
			}

			/* 2) Destroy handle object */
			rd_kafka_destroy(consumerRdKafkaHandle);
			consumerRdKafkaHandle = nullptr;

			rd_kafka_topic_partition_list_destroy(consumerRdKafkaSubscription);
			consumerRdKafkaSubscription = nullptr;

			throw esl::addStacktrace(std::runtime_error("Failed to subscribe topics: " + errorStr));
		}
	}

	while(consumerState == CSRunning) {
		std::unique_lock<std::mutex> consumerThreadsNotifyLock(consumerThreadsNotifyMutex);
		consumerThreadsCondVar.wait(consumerThreadsNotifyLock, std::bind(&Client::consumerIsThreadAvailable, this));

		rd_kafka_message_t* rdKafkaMessage = rd_kafka_consumer_poll(consumerRdKafkaHandle, 500);

		if(rdKafkaMessage == nullptr) {
			if(stopIfEmpty) {
				std::lock_guard<std::mutex> consumerThreadLock(consumerThreadMutex);
				consumerState = CSShutdown;
			}
			continue;
		}

		if (rdKafkaMessage->err) {
			if (rdKafkaMessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
				if(stopIfEmpty) {
					std::lock_guard<std::mutex> guard(consumerThreadMutex);
					consumerState = CSShutdown;
				}
			}
			else {
				logger.debug << "Error message received:\n";
				if(rdKafkaMessage->rkt) {
					logger.trace << "- Topic    : \"" << rd_kafka_topic_name(rdKafkaMessage->rkt) << "\"\n";
				}
				else {
					logger.trace << "- Topic    : (none)\n";
				}
				logger.trace << "- Offset   : " << rdKafkaMessage->offset << "\n";
				logger.trace << "- Partition: " << rdKafkaMessage->partition << "\n";
				if(rdKafkaMessage->key) {
					logger.trace << "- Key      : \"" << std::string(static_cast<char*>(rdKafkaMessage->key), rdKafkaMessage->key_len) << "\"\n";
				}
				else {
					logger.trace << "- Key      : (none)\n";
				}
				if(rdKafkaMessage->payload) {
					logger.trace << "- Payload  : \"" << std::string(static_cast<char*>(rdKafkaMessage->payload), rdKafkaMessage->len) << "\"\n";
				}
				else {
					logger.trace << "- Payload  : (none)\n";
				}

				/* We have NOT reached the topic/partition end. Don't continue to wait for the response.. */
				std::lock_guard<std::mutex> consumerThreadLock(consumerThreadMutex);
				consumerState = CSShutdown;
			}

			rd_kafka_message_destroy(rdKafkaMessage);
			continue;
		}


		++consumerThreadsRunning;
		std::thread consumerThread([this, rdKafkaMessage, createMessageHandler]{
			consumerMessageHandlerThread(*rdKafkaMessage, createMessageHandler);
		});
		consumerThread.detach();

	}

	/* ********************************** *
	 * Shutdown: Wait for running Threads *
	 * ********************************** */
	rd_kafka_unsubscribe(consumerRdKafkaHandle);

	{
		std::unique_lock<std::mutex> consumerThreadsNotifyLock(consumerThreadsNotifyMutex);
		consumerThreadsCondVar.wait(consumerThreadsNotifyLock, std::bind(&Client::consumerIsNoThreadRunning, this));
	}

	/* **************** *
	 * Deinitialization *
	 * **************** */
	{
		std::lock_guard<std::mutex> consumerThreadLock(consumerThreadMutex);
		rd_kafka_resp_err_t err;

		/* NOTE: There is no need to unsubscribe prior to calling rd_kafka_consumer_close() */
		/* 1) Leave the consumer group, commit final offsets, etc. */
		err = rd_kafka_consumer_close(consumerRdKafkaHandle);
		if (err) {
			logger.error << "Failed to close consumer: " << rd_kafka_err2str(err) << "\n";
		}

		/* 2) Destroy handle object */
		rd_kafka_destroy(consumerRdKafkaHandle);
		consumerRdKafkaHandle = nullptr;

		rd_kafka_topic_partition_list_destroy(consumerRdKafkaSubscription);
		consumerRdKafkaSubscription = nullptr;

		consumerState = CSNotRunning;
	}

	consumerWaitCondVar.notify_all();
}

void Client::consumerStop() {
	std::lock_guard<std::mutex> guard(consumerThreadMutex);
	if(consumerState == CSRunning) {
		consumerState = CSShutdown;
	}
}

bool Client::consumerWait(std::uint32_t ms) {
	std::unique_lock<std::mutex> consumerWaitNotifyLock(consumerWaitNotifyMutex);
	/*
	if(consumerIsStateNotRunning()) {
		return true;
	}
	*/
	if(ms == 0) {
		consumerWaitCondVar.wait(consumerWaitNotifyLock, std::bind(&Client::consumerIsStateNotRunning, this));
		return true;
	}

	return consumerWaitCondVar.wait_for(consumerWaitNotifyLock, std::chrono::milliseconds(ms), std::bind(&Client::consumerIsStateNotRunning, this));
}

bool Client::consumerIsThreadAvailable() const {
	std::lock_guard<std::mutex> guard(consumerThreadMutex);
	return consumerThreadsRunning < consumerThreadsMax;
}

bool Client::consumerIsNoThreadRunning() const {
	std::lock_guard<std::mutex> guard(consumerThreadMutex);
	return consumerThreadsRunning == 0;
}

bool Client::consumerIsStateNotRunning() const {
	std::lock_guard<std::mutex> guard(consumerThreadMutex);
	return consumerState == CSNotRunning;
}

void Client::consumerMessageHandlerThread(rd_kafka_message_t& rdKafkaMessage, esl::messaging::messagehandler::Interface::CreateMessageHandler createMessageHandler) {
	Message message(rdKafkaMessage);
	MessageContext messageContext(message, *this);

	{
		std::unique_ptr<esl::utility::Consumer> messageHandler = createMessageHandler(messageContext);

		if(messageHandler) {
			try {
				while(true) {
					std::size_t size = messageHandler->read(message.getReader());
					if(size == esl::utility::Reader::npos) {
						break;
					}
				}
				rd_kafka_commit_message(consumerRdKafkaHandle, &rdKafkaMessage, 0);
			}
			catch(...) {

			}
		}
	}

	rd_kafka_message_destroy(&rdKafkaMessage);

	{
		std::lock_guard<std::mutex> consumerThreadLock(consumerThreadMutex);
		--consumerThreadsRunning;
	}

	consumerThreadsCondVar.notify_one();
}

} /* namespace messaging */
} /* namespace rdkafka4esl */
