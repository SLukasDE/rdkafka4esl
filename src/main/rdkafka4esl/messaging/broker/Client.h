#ifndef RDKAFKA4ESL_MESSAGING_BROKER_CLIENT_H_
#define RDKAFKA4ESL_MESSAGING_BROKER_CLIENT_H_

#include <rdkafka4esl/messaging/client/Connection.h>
#include <rdkafka4esl/messaging/server/Socket.h>

#include <esl/messaging/broker/Interface.h>
#include <esl/messaging/server/requesthandler/Interface.h>

#include <cstdint>
#include <utility>
#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <condition_variable>
#include <memory>

#include <librdkafka/rdkafka.h>

namespace rdkafka4esl {
namespace messaging {
namespace broker {

class Client final : public esl::messaging::broker::Interface::Client {
public:
	static inline const char* getImplementation() {
		return "rdkafka4esl";
	}

	static std::unique_ptr<esl::messaging::broker::Interface::Client> create(const std::string& brokers, const esl::object::Values<std::string>& settings);

	Client(const std::string& brokers, const esl::object::Values<std::string>& settings);
	~Client();

	esl::messaging::server::Interface::Socket& getSocket() override;

	void socketListen(const std::set<std::string>& notifications, esl::messaging::server::requesthandler::Interface::CreateInput createInput);
	void socketRelease();
	bool socketWait(std::uint32_t ms);
	bool consumerIsStateNotRunning() const;

	std::unique_ptr<esl::messaging::client::Interface::Connection> createConnection(std::vector<std::pair<std::string, std::string>> parameters) override;
	void connectionRegister();
	void connectionUnregister();

private:
	std::vector<std::pair<std::string, std::string>> settings;

	/* ****************** *
	 * Producer variables *
	 * ****************** */
	std::mutex producerWaitNotifyMutex;
	std::condition_variable producerWaitCondVar;

	std::mutex producerCountMutex;
	std::size_t producerCount = 0;

	/* ****************** *
	 * Consumer variables *
	 * ****************** */
	server::Socket socket;
	bool consumerStopListeningIfEmpty = false;
	//esl::object::ObjectContext& consumerObjectContext;

	std::mutex consumerThreadsNotifyMutex;
	std::condition_variable consumerThreadsCondVar;

	std::mutex consumerWaitNotifyMutex;
	std::condition_variable consumerWaitCondVar;

	mutable std::mutex consumerThreadMutex;
	rd_kafka_t* consumerRdKafkaHandle = nullptr;
	rd_kafka_topic_partition_list_t* consumerRdKafkaSubscription = nullptr;
	enum {
		CSNotRunning,
		CSRunning,
		CSShutdown
	} consumerState = CSNotRunning;
	std::uint16_t consumerThreadsRunning = 0;
	std::uint16_t consumerThreadsMax = 0;

	rd_kafka_conf_t& createConfig() const;

	bool producerIsEmpty();

	void consumerStartThread(const std::set<std::string>& queues, esl::messaging::server::requesthandler::Interface::CreateInput createInput);

	//void consumerInit(const std::set<std::string>& queues);
	//void consumerRelease();

	bool consumerIsThreadAvailable() const;
	bool consumerIsNoThreadRunning() const;
	void consumerMessageHandlerThread(rd_kafka_message_t& rdKafkaMessage, esl::messaging::server::requesthandler::Interface::CreateInput createInput);
};

} /* namespace broker */
} /* namespace messaging */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_MESSAGING_BROKER_CLIENT_H_ */
