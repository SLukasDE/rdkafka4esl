#include <rdkafka4esl/com/basic/broker/Client.h>
#include <rdkafka4esl/com/basic/client/ConnectionFactory.h>
#include <rdkafka4esl/com/basic/client/SharedConnectionFactory.h>
#include <rdkafka4esl/com/basic/server/RequestContext.h>
#include <esl/Logger.h>

#include <esl/io/Consumer.h>
#include <esl/system/Stacktrace.h>

#include <map>
#include <stdexcept>
#include <string>

namespace rdkafka4esl {
namespace com {
namespace basic {
namespace broker {

namespace {
esl::Logger logger("rdkafka4esl::com::basic::broker::Client");
} /* anonymous namespace */

Client::Client(const esl::object::KafkaClient::Settings& settings)
: kafkaSettings(settings.kafkaSettings)
{
	bool hasGroupId = false;

	logger.debug << "Begin show kafka settings:\n";
	for(const auto& setting : kafkaSettings) {
		logger.debug << "- \"" << setting.first << "\"=\"" << setting.second << "\"\n";
		if(setting.first == "kafka.group.id") {
			hasGroupId = true;
		}
	}
	logger.debug << "End show kafka settings.\n";

	if(!hasGroupId) {
		throw esl::system::Stacktrace::add(std::runtime_error("Value \"kafka.group.id\" not specified."));
	}
}

Client::~Client() {
	stop();
	wait(0);
}

void Client::start(std::function<void()> aOnReleasedHandler) {
	std::lock_guard<std::mutex> stateLock(stateMutex);
	if(state == stopping) {
		throw esl::system::Stacktrace::add(std::runtime_error("Calling 'broker.start' failed because broker is shutting down"));
	}

	state = started;
	onReleasedHandler = aOnReleasedHandler;
}

void Client::stop() {
	std::lock_guard<std::mutex> stateLock(stateMutex);
	if(state != started) {
		return;
	}
	state = stopping;

	for(auto socket : sockets) {
		socket->release();
	}

	for(auto connectionFactory : connectionFactories) {
		connectionFactory->release();
	}

	if(connectionFactories.empty() && sockets.empty()) {
		state = stopped;
		if(onReleasedHandler) {
			onReleasedHandler();
		}
	}
}

bool Client::wait(std::uint32_t ms) {
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

rd_kafka_conf_t& Client::createConfig(const std::vector<std::pair<std::string, std::string>>& kafkaSettings) {
	char errstr[512];

	/* Note:
	 * The rd_kafka.._conf_t objects are not reusable after they have been passed to rd_kafka.._new().
	 * The application does not need to free any config resources after a rd_kafka.._new() call.
	 */
	rd_kafka_conf_t* rdKafkaConfig(rd_kafka_conf_new());

	if(rdKafkaConfig == nullptr) {
		throw esl::system::Stacktrace::add(std::runtime_error("Failed to create kafka configuration object"));
	}

	if(rd_kafka_conf_set(rdKafkaConfig, "client.id", "rdkafka4esl", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		throw esl::system::Stacktrace::add(std::runtime_error(errstr));
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
	for(const auto& setting : kafkaSettings) {
		if(rd_kafka_conf_set(rdKafkaConfig, setting.first.c_str(), setting.second.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
			throw esl::system::Stacktrace::add(std::runtime_error(errstr));
		}
	}

	return *rdKafkaConfig;
}

const std::vector<std::pair<std::string, std::string>>& Client::getKafkaSettings() const {
	return kafkaSettings;
}

void Client::registerConnectionFactory(client::SharedConnectionFactory* sharedConnectionFactory) {
	std::lock_guard<std::mutex> stateLock(stateMutex);
	connectionFactories.insert(sharedConnectionFactory);
}

void Client::unregisterConnectionFactory(client::SharedConnectionFactory* sharedConnectionFactory) {
	{
		std::lock_guard<std::mutex> stateLock(stateMutex);
		connectionFactories.erase(sharedConnectionFactory);

		if(state == stopping && connectionFactories.empty() && sockets.empty()) {
			state = stopped;
			if(onReleasedHandler) {
				onReleasedHandler();
			}
		}
	}

	stateNotifyCondVar.notify_all();
}

void Client::registerSocket(server::Socket* socket) {
	std::lock_guard<std::mutex> stateLock(stateMutex);
	sockets.insert(socket);
}

void Client::unregisterSocket(server::Socket* socket) {
	{
		std::lock_guard<std::mutex> stateLock(stateMutex);
		sockets.erase(socket);

		if(state == stopping && connectionFactories.empty() && sockets.empty()) {
			state = stopped;
			if(onReleasedHandler) {
				onReleasedHandler();
			}
		}
	}

	stateNotifyCondVar.notify_all();
}

} /* namespace broker */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */
