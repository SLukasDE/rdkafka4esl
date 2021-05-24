#include <rdkafka4esl/messaging/broker/Client.h>
#include <rdkafka4esl/messaging/server/MessageContext.h>
#include <rdkafka4esl/messaging/Logger.h>

#include <esl/io/Consumer.h>
#include <esl/Stacktrace.h>

#include <string>
#include <map>
#include <stdexcept>

namespace rdkafka4esl {
namespace messaging {
namespace broker {

namespace {
Logger logger("rdkafka4esl::messaging::broker::Client");
}

std::unique_ptr<esl::messaging::broker::Interface::Client> Client::create(const std::string& brokers, const esl::object::Values<std::string>& settings) {
	return std::unique_ptr<esl::messaging::broker::Interface::Client>(new Client(brokers, settings));
}

Client::Client(const std::string& brokers, const esl::object::Values<std::string>& aSettings)
: settings(aSettings.getValues()),
  socket(*this)
{
	bool hasGroupId = false;
	bool hasBrokers = false;

	for(auto& setting : settings) {
		if(setting.first == "kafka.group.id") {
			if(setting.second.empty()) {
				continue;
			}
			hasGroupId = true;
		}
		else if(setting.first == "kafka.bootstrap.servers") {
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
		else if(setting.first == "threads") {
			consumerThreadsMax = static_cast<std::uint16_t>(std::stoul(setting.second));
		}
		else if(setting.first == "stopListeningIfEmpty") {
			consumerStopListeningIfEmpty = static_cast<std::uint16_t>(std::stoul(setting.second));
		}
	}

	if(!hasBrokers && !brokers.empty()) {
		settings.emplace_back("kafka.bootstrap.servers", brokers);
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
	socketRelease();

	std::unique_lock<std::mutex> producerWaitNotifyLock(producerWaitNotifyMutex);
	producerWaitCondVar.wait(producerWaitNotifyLock, std::bind(&Client::producerIsEmpty, this));

	socketWait(0);
}

esl::messaging::server::Interface::Socket& Client::getSocket() {
	return socket;
}

void Client::socketListen(const std::set<std::string>& notifications, esl::messaging::server::messagehandler::Interface::CreateMessageHandler createMessageHandler) {
	if(notifications.empty()) {
		return;
	}

	{
		std::lock_guard<std::mutex> consumerThreadLock(consumerThreadMutex);

		if(consumerState != CSNotRunning) {
			return;
		}
		consumerState = CSRunning;
	}

	std::thread consumerThread([this, notifications, createMessageHandler]{
		consumerStartThread(notifications, createMessageHandler);
	});
	consumerThread.detach();
}

void Client::socketRelease() {
	std::lock_guard<std::mutex> guard(consumerThreadMutex);
	if(consumerState == CSRunning) {
		consumerState = CSShutdown;
	}
}

bool Client::socketWait(std::uint32_t ms) {
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

bool Client::consumerIsStateNotRunning() const {
	std::lock_guard<std::mutex> guard(consumerThreadMutex);
	return consumerState == CSNotRunning;
}

std::unique_ptr<esl::messaging::client::Interface::Connection> Client::createConnection(std::vector<std::pair<std::string, std::string>> parameters) {
//std::unique_ptr<esl::messaging::Producer> Client::createProducer(const std::string& id, std::vector<std::pair<std::string, std::string>> parameter) {
	char errstr[512];
	rd_kafka_t* producerRdKafkaHandle = nullptr;
	rd_kafka_topic_conf_t* rdKafkaTopicConfig = nullptr;
	rd_kafka_topic_t* rdKafkaTopic = nullptr;
	std::string topicName;

	{
		bool hasTopicName = false;
		for(auto& parameter : parameters) {
			if(parameter.first == "topic") {
				topicName = parameter.second;
				hasTopicName = true;
				break;
			}
		}
		if(hasTopicName == false) {
			throw esl::addStacktrace(std::runtime_error("Parameter \"topic\" is missing"));
		}
	}

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

	rdKafkaTopic = rd_kafka_topic_new(producerRdKafkaHandle, topicName.c_str(), rdKafkaTopicConfig);
	if(rdKafkaTopic == nullptr) {
		rd_kafka_topic_conf_destroy(rdKafkaTopicConfig);

		/* Destroy handle object */
		rd_kafka_destroy(producerRdKafkaHandle);

		throw esl::addStacktrace(std::runtime_error("rd_kafka_topic_new failed for topic \"" + topicName + "\""));
	}

	return std::unique_ptr<esl::messaging::client::Interface::Connection>(new client::Connection(*this, *producerRdKafkaHandle, *rdKafkaTopic, parameters));
}

void Client::connectionRegister() {
	std::lock_guard<std::mutex> producerCountLock(producerCountMutex);
	++producerCount;
}

void Client::connectionUnregister() {
	{
		std::lock_guard<std::mutex> producerCountLock(producerCountMutex);
		--producerCount;
	}

	producerWaitCondVar.notify_one();
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

bool Client::producerIsEmpty() {
	std::lock_guard<std::mutex> producerCountLock(producerCountMutex);
	return producerCount == 0;
}

void Client::consumerStartThread(const std::set<std::string>& notifications, esl::messaging::server::messagehandler::Interface::CreateMessageHandler createMessageHandler) {
	/* ************** *
	 * Initialization *
	 * ************** */
	{
		std::lock_guard<std::mutex> consumerThreadLock(consumerThreadMutex);
		rd_kafka_resp_err_t err;
		char errstr[512];

		/* Create subscription for specified topics */
		consumerRdKafkaSubscription = rd_kafka_topic_partition_list_new(notifications.size());
		if(consumerRdKafkaSubscription == nullptr) {
			throw esl::addStacktrace(std::runtime_error("Cannot create kafka consumer subscription object"));
		}
		for(const auto& notification : notifications) {
			rd_kafka_topic_partition_list_add(consumerRdKafkaSubscription, notification.c_str(), RD_KAFKA_PARTITION_UA);
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
			if(consumerStopListeningIfEmpty) {
				std::lock_guard<std::mutex> consumerThreadLock(consumerThreadMutex);
				consumerState = CSShutdown;
			}
			continue;
		}

		if (rdKafkaMessage->err) {
			if (rdKafkaMessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
				if(consumerStopListeningIfEmpty) {
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

bool Client::consumerIsThreadAvailable() const {
	std::lock_guard<std::mutex> guard(consumerThreadMutex);
	return consumerThreadsRunning < consumerThreadsMax;
}

bool Client::consumerIsNoThreadRunning() const {
	std::lock_guard<std::mutex> guard(consumerThreadMutex);
	return consumerThreadsRunning == 0;
}

void Client::consumerMessageHandlerThread(rd_kafka_message_t& rdKafkaMessage, esl::messaging::server::messagehandler::Interface::CreateMessageHandler createMessageHandler) {
	server::MessageContext messageContext(*this, rdKafkaMessage);
	esl::io::Input messageHandler = createMessageHandler(messageContext);

	if(messageHandler) {
		try {
			const char* payload = static_cast<char*>(rdKafkaMessage.payload);
			for(std::size_t pos = 0; pos < rdKafkaMessage.len;) {
				std::size_t rv = messageHandler.getWriter().write(payload, rdKafkaMessage.len - pos);
				if(rv == 0 || rv == esl::io::Writer::npos) {
					break;
				}
				pos += rv;
			}
			rd_kafka_commit_message(consumerRdKafkaHandle, &rdKafkaMessage, 0);
		}
		catch(...) {

		}
	}

	rd_kafka_message_destroy(&rdKafkaMessage);

	{
		std::lock_guard<std::mutex> consumerThreadLock(consumerThreadMutex);
		--consumerThreadsRunning;
	}

	consumerThreadsCondVar.notify_one();
}

} /* namespace broker */
} /* namespace messaging */
} /* namespace rdkafka4esl */
