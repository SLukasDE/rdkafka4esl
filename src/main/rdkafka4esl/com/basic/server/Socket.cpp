#include <rdkafka4esl/com/basic/server/Socket.h>
#include <rdkafka4esl/com/basic/server/RequestContext.h>
#include <rdkafka4esl/object/Client.h>

#include <esl/io/Input.h>
#include <esl/io/Writer.h>
#include <esl/Logger.h>
#include <esl/object/KafkaClient.h>
#include <esl/system/Stacktrace.h>
#include <esl/utility/String.h>

#include <stdexcept>
#include <thread>

namespace rdkafka4esl {
namespace com {
namespace basic {
namespace server {

namespace {
esl::Logger logger("rdkafka4esl::com::basic::server::Socket");
}


Socket::Socket(const esl::com::basic::server::KafkaSocket::Settings& aSettings)
: settings(aSettings)
{ }

Socket::~Socket() {
	if(client) {
		client->unregisterSocket(this);
	}
}

void Socket::initializeContext(esl::object::Context& objectContext) {
	if(settings.kafkaSettings.empty()) {
		object::Client* client = objectContext.findObject<object::Client>(settings.brokerId);
		if(client == nullptr) {
			esl::object::KafkaClient* rdkafkaClient = objectContext.findObject<esl::object::KafkaClient>(settings.brokerId);
			if(rdkafkaClient) {
				client = &rdkafkaClient->getClient();
			}
		}
		if(client == nullptr) {
	    	throw esl::system::Stacktrace::add(std::runtime_error("Cannot find broker with id '" + settings.brokerId + "'"));
		}
		client->registerSocket(this);
		settings.kafkaSettings = client->getKafkaSettings();
	}
}

void Socket::listen(const esl::com::basic::server::RequestHandler& requestHandler) {
	// it's almost there: check non-blocking method 'listen' (below) how it will call method 'listen2'.
}

void Socket::listen(const esl::com::basic::server::RequestHandler& requestHandler, std::function<void()> aOnReleasedHandler) {
	std::lock_guard<std::mutex> stateLock(stateMutex);
	if(state != stopped) {
		throw esl::system::Stacktrace::add(std::runtime_error("Kafka socket is already listening."));
	}

	std::set<std::string> notifications = requestHandler.getNotifiers();
	if(notifications.empty()) {
		throw esl::system::Stacktrace::add(std::runtime_error("Cannot listen on kafka broker for empty topic list"));
	}

	/* prepare subscription for specified topics */
	std::vector<std::pair<std::string, std::int32_t>> topicPartitionList;
	for(const auto& notification : notifications) {
		std::string topic;
		std::int32_t partition = RD_KAFKA_PARTITION_UA;

		auto pos = notification.rfind(':');
		if(pos == std::string::npos) {
			topic = notification;
		}
		else if(pos > 0) {
			topic = notification.substr(0, pos);
			if(pos+1 < notification.size()) {
				std::string partitionStr = notification.substr(pos+1);
				partition = esl::utility::String::toNumber<decltype(partition)>(partitionStr);
			}
		}

		if(topic.empty()) {
			throw esl::system::Stacktrace::add(std::runtime_error("Invalid notifier '" + notification + "' to listen, because topic name is empty"));
		}

		if(partition < 0 && partition != RD_KAFKA_PARTITION_UA) {
			throw esl::system::Stacktrace::add(std::runtime_error("Invalid notifier '" + notification + "' to listen, because partition id is invalid"));
		}

		if(partition == RD_KAFKA_PARTITION_UA) {
			logger.debug << "Add listener for topic '" << topic << "' at partition 'UA'\n";
		}
		else {
			logger.debug << "Add listener for topic '" << topic << "' at partition '" << partition << "'\n";
		}
		topicPartitionList.push_back(std::make_pair(topic, partition));
	}

	/* Create subscription for specified topics */
	rdkTopicPartitionList = rd_kafka_topic_partition_list_new(notifications.size());
	if(rdkTopicPartitionList == nullptr) {
		throw esl::system::Stacktrace::add(std::runtime_error("Cannot create kafka topic-partition-list object"));
	}
	for(const auto& topicPartition : topicPartitionList) {
		rd_kafka_topic_partition_list_add(rdkTopicPartitionList, topicPartition.first.c_str(), topicPartition.second);
	}

	/* Create Kafka consumer handle */
	char errstr[512];
	rdkConsumerHandle = rd_kafka_new(RD_KAFKA_CONSUMER, &object::Client::createConfig(settings.kafkaSettings), errstr, sizeof(errstr));
	if(rdkConsumerHandle == nullptr) {
		rd_kafka_topic_partition_list_destroy(rdkTopicPartitionList);
		rdkTopicPartitionList = nullptr;
		throw esl::system::Stacktrace::add(std::runtime_error(errstr));
	}

	/* Subscribe */
	rd_kafka_resp_err_t err;
	if ((err = rd_kafka_subscribe(rdkConsumerHandle, rdkTopicPartitionList))) {
		std::string errorStr = rd_kafka_err2str(err);

		/* 1) Leave the consumer group, commit final offsets, etc. */
		err = rd_kafka_consumer_close(rdkConsumerHandle);
		if (err) {
			logger.error << "Failed to close consumer: " << rd_kafka_err2str(err) << "\n";
		}

		/* 2) Destroy handle object */
		rd_kafka_destroy(rdkConsumerHandle);
		rdkConsumerHandle = nullptr;

		rd_kafka_topic_partition_list_destroy(rdkTopicPartitionList);
		rdkTopicPartitionList = nullptr;

		throw esl::system::Stacktrace::add(std::runtime_error("Failed to subscribe topics: " + errorStr));
	}

	/* **************************************** *
	 * Initialization done, run listener-thread *
	 * **************************************** */
	onReleasedHandler = aOnReleasedHandler;
	state = started;
	std::thread listenThread([this, &requestHandler]{
		listen2(requestHandler);
	});
	listenThread.detach();
}

void Socket::release() {
	std::lock_guard<std::mutex> stateLock(stateMutex);
	if(state == started) {
		state = stopping;
	}
	if(state == stopped && onReleasedHandler) {
		onReleasedHandler();
	}
}

bool Socket::wait(std::uint32_t ms) {
	{
		std::lock_guard<std::mutex> stateLock(stateMutex);
		if(state == stopped) {
			return true;
		}
	}

	{
		std::unique_lock<std::mutex> stateNotifyLock(stateNotifyMutex);
		if(ms == 0) {
			stateNotifyCondVar.wait(stateNotifyLock, [this] {
					std::lock_guard<std::mutex> stateLock(stateMutex);
					return state == stopped;
			});
			return true;
		}
		else {
			return stateNotifyCondVar.wait_for(stateNotifyLock, std::chrono::milliseconds(ms), [this] {
					std::lock_guard<std::mutex> stateLock(stateMutex);
					return state == stopped;
			});
		}
	}
}

void Socket::listen2(const esl::com::basic::server::RequestHandler& requestHandler) {
	while([this] {
		std::lock_guard<std::mutex> stateLock(stateMutex);
		return state == started;
	}()) {
		/* ************************************************** *
		 * poll and check, later create thread for processing *
		 * ************************************************** */
		rd_kafka_message_t* rdKafkaMessage = rd_kafka_consumer_poll(rdkConsumerHandle, settings.pollTimeoutMs);

		if(rdKafkaMessage == nullptr) {
			if(settings.stopIfEmpty) {
				std::lock_guard<std::mutex> stateLock(stateMutex);
				state = stopping;
			}
			continue;
		}

		if (rdKafkaMessage->err) {
			if (rdKafkaMessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
				if(settings.stopIfEmpty) {
					std::lock_guard<std::mutex> stateLock(stateMutex);
					state = stopping;
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
				std::lock_guard<std::mutex> stateLock(stateMutex);
				state = stopping;
			}

			rd_kafka_message_destroy(rdKafkaMessage);
			continue;
		}


		/* **************** *
		 * process message  *
		 * **************** */
		if(settings.maxThreads == 0) {
			accept(*rdKafkaMessage, requestHandler);
		}
		else {
			/* ************************************ *
			 * wait for available processing thread *
			 * ************************************ */
			std::unique_lock<std::mutex> threadsNotifyLock(threadsNotifyMutex);
			threadsCondVar.wait(threadsNotifyLock, [this] {
				std::lock_guard<std::mutex> stateLock(stateMutex);
				return threadsRunning < settings.maxThreads;
			});

			std::lock_guard<std::mutex> stateLock(stateMutex);
			++threadsRunning;

			/* *********************** *
			 * start processing thread *
			 * *********************** */
			std::thread consumerThread([this, rdKafkaMessage, &requestHandler]{
				accept(*rdKafkaMessage, requestHandler);
			});
			consumerThread.detach();
		}
	}

	/* ********************************** *
	 * Shutdown: Wait for running Threads *
	 * ********************************** */
	rd_kafka_unsubscribe(rdkConsumerHandle);
	{
		std::unique_lock<std::mutex> threadsNotifyLock(threadsNotifyMutex);
		threadsCondVar.wait(threadsNotifyLock, [this]{
			std::lock_guard<std::mutex> stateLock(stateMutex);
			return threadsRunning == 0;
		});
	}

	/* **************** *
	 * Deinitialization *
	 * **************** */
	{
		std::lock_guard<std::mutex> stateLock(stateMutex);
		rd_kafka_resp_err_t err;

		/* NOTE: There is no need to unsubscribe prior to calling rd_kafka_consumer_close() */
		/* 1) Leave the consumer group, commit final offsets, etc. */
		err = rd_kafka_consumer_close(rdkConsumerHandle);
		if (err) {
			logger.error << "Failed to close consumer: " << rd_kafka_err2str(err) << "\n";
		}

		/* 2) Destroy handle object */
		rd_kafka_destroy(rdkConsumerHandle);
		rdkConsumerHandle = nullptr;

		rd_kafka_topic_partition_list_destroy(rdkTopicPartitionList);
		rdkTopicPartitionList = nullptr;

		state = stopped;
		if(onReleasedHandler) {
			onReleasedHandler();
		}
	}

	stateNotifyCondVar.notify_all();
}

void Socket::accept(rd_kafka_message_t& rdkMessage, const esl::com::basic::server::RequestHandler& requestHandler) {
	RequestContext requestContext(*this, rdkMessage);
	esl::io::Input messageHandler = requestHandler.accept(requestContext);

	if(messageHandler) {
		try {
			const char* payload = static_cast<char*>(rdkMessage.payload);
			for(std::size_t pos = 0; pos < rdkMessage.len;) {
				std::size_t rv = messageHandler.getWriter().write(payload, rdkMessage.len - pos);
				if(rv == 0 || rv == esl::io::Writer::npos) {
					break;
				}
				pos += rv;
			}
			messageHandler.getWriter().write(0, 0);
			rd_kafka_commit_message(rdkConsumerHandle, &rdkMessage, 0);
		}
		catch(...) {

		}
	}

	rd_kafka_message_destroy(&rdkMessage);

	if(settings.maxThreads > 0) {
		{
			std::lock_guard<std::mutex> stateLock(stateMutex);
			--threadsRunning;
		}
		threadsCondVar.notify_one();
	}
}

} /* namespace server */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */
