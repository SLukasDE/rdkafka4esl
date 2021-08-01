#ifndef RDKAFKA4ESL_COM_BASIC_BROKER_CLIENT_H_
#define RDKAFKA4ESL_COM_BASIC_BROKER_CLIENT_H_

#include <rdkafka4esl/com/basic/client/Connection.h>
#include <rdkafka4esl/com/basic/server/Socket.h>

#include <esl/com/basic/broker/Interface.h>
#include <esl/com/basic/server/requesthandler/Interface.h>
#include <esl/object/Interface.h>

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
namespace com {
namespace basic {
namespace broker {

class Client final : public esl::com::basic::broker::Interface::Client {
public:
	static inline const char* getImplementation() {
		return "rdkafka4esl";
	}

	static std::unique_ptr<esl::com::basic::broker::Interface::Client> create(const esl::object::Interface::Settings& settings);

	Client(const esl::object::Interface::Settings& settings);
	~Client();

	esl::com::basic::server::Interface::Socket& getSocket() override;

	void socketListen(const std::set<std::string>& notifications, esl::com::basic::server::requesthandler::Interface::CreateInput createInput);
	void socketRelease();
	bool socketWait(std::uint32_t ms);
	bool consumerIsStateNotRunning() const;

	std::unique_ptr<esl::com::basic::client::Interface::Connection> createConnection(const esl::com::basic::broker::Interface::Settings& parameters) override;
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

	void consumerStartThread(const std::set<std::string>& queues, esl::com::basic::server::requesthandler::Interface::CreateInput createInput);

	//void consumerInit(const std::set<std::string>& queues);
	//void consumerRelease();

	bool consumerIsThreadAvailable() const;
	bool consumerIsNoThreadRunning() const;
	void consumerMessageHandlerThread(rd_kafka_message_t& rdKafkaMessage, esl::com::basic::server::requesthandler::Interface::CreateInput createInput);
};

} /* namespace broker */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_COM_BASIC_BROKER_CLIENT_H_ */
