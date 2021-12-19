#ifndef RDKAFKA4ESL_COM_BASIC_BROKER_CLIENT_H_
#define RDKAFKA4ESL_COM_BASIC_BROKER_CLIENT_H_

#include <rdkafka4esl/com/basic/server/Socket.h>
#include <rdkafka4esl/com/basic/client/ConnectionFactory.h>

#include <esl/object/Interface.h>
#include <esl/module/Interface.h>

#include <cstdint>
#include <utility>
#include <string>
#include <vector>
#include <set>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <functional>

#include <librdkafka/rdkafka.h>

namespace rdkafka4esl {
namespace com {
namespace basic {
namespace broker {

class Client final : public esl::object::Interface::Object {
public:
	static inline const char* getImplementation() {
		return "rdkafka4esl";
	}

	static std::unique_ptr<esl::object::Interface::Object> create(const esl::object::Interface::Settings& settings);

	Client(const esl::object::Interface::Settings& settings);
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
