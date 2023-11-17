#ifndef RDKAFKA4ESL_COM_BASIC_BROKER_CLIENT_H_
#define RDKAFKA4ESL_COM_BASIC_BROKER_CLIENT_H_

#include <rdkafka4esl/com/basic/server/Socket.h>
#include <rdkafka4esl/com/basic/client/ConnectionFactory.h>

#include <esl/object/KafkaClient.h>
#include <esl/object/Object.h>

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <librdkafka/rdkafka.h>

namespace rdkafka4esl {
namespace com {
namespace basic {
namespace broker {

class Client final : public esl::object::Object {
public:
	Client(const esl::object::KafkaClient::Settings& settings);
	~Client();

	void start(std::function<void()> onReleasedHandler);
	void stop();
	bool wait(std::uint32_t ms);

	static rd_kafka_conf_t& createConfig(const std::vector<std::pair<std::string, std::string>>& kafkaSettings);
	const std::vector<std::pair<std::string, std::string>>& getKafkaSettings() const;

	void registerConnectionFactory(client::SharedConnectionFactory* sharedConnectionFactory);
	void unregisterConnectionFactory(client::SharedConnectionFactory* sharedConnectionFactory);

	void registerSocket(server::Socket* socket);
	void unregisterSocket(server::Socket* socket);

private:
	std::vector<std::pair<std::string, std::string>> kafkaSettings;
	std::function<void()> onReleasedHandler;

	std::mutex stateMutex;
	enum {
		stopped,
		started,
		stopping
	} state = stopped;

	std::mutex stateNotifyMutex;
	std::condition_variable stateNotifyCondVar;

	/* ****************** *
	 * Producer variables *
	 * ****************** */
	std::set<client::SharedConnectionFactory*> connectionFactories;

	/* ****************** *
	 * Consumer variables *
	 * ****************** */
	std::set<server::Socket*> sockets;
};

} /* namespace broker */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_COM_BASIC_BROKER_CLIENT_H_ */
