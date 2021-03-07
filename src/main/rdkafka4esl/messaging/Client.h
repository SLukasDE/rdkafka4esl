#ifndef RDKAFKA4ESL_MESSAGING_CLIENT_H_
#define RDKAFKA4ESL_MESSAGING_CLIENT_H_

#include <rdkafka4esl/messaging/Consumer.h>

#include <esl/messaging/Interface.h>
#include <esl/messaging/messagehandler/Interface.h>
#include <esl/messaging/Consumer.h>
#include <esl/messaging/Producer.h>

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

class Client final : public esl::messaging::Interface::Client {
public:
	static inline const char* getImplementation() {
		return "rdkafka4esl";
	}

	static std::unique_ptr<esl::messaging::Interface::Client> create(const std::string& brokers, const esl::object::Values<std::string>& settings);

	Client(const std::string& brokers, const esl::object::Values<std::string>& settings);
	~Client();

	void addObjectFactory(const std::string& id, ObjectFactory objectFactory) override;
	ObjectFactory getObjectFactory(const std::string& id) const;

	esl::messaging::Consumer& getConsumer() override;
	std::unique_ptr<esl::messaging::Producer> createProducer(const std::string& id, std::vector<std::pair<std::string, std::string>> parameter);
	std::unique_ptr<esl::messaging::Interface::ProducerFactory> createProducerFactory(const std::string& id, std::vector<std::pair<std::string, std::string>> parameters) override;

private:
	friend class Consumer;
	friend class Producer;
	friend class ProducerFactory;

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
	Consumer consumer;
	//esl::object::ObjectContext& consumerObjectContext;
	std::map<std::string, ObjectFactory> consumerObjectFactories;

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

	void createdProducer();
	void destroyedProducer();
	bool producerIsEmpty();

	void consumerStart(const std::set<std::string>& queues, esl::messaging::messagehandler::Interface::CreateMessageHandler createMessageHandler, std::uint16_t numThreads, bool stopIfEmpty);
	void consumerStartThread(const std::set<std::string>& queues, esl::messaging::messagehandler::Interface::CreateMessageHandler createMessageHandler, bool stopIfEmpty);
	void consumerStop();
	bool consumerWait(std::uint32_t ms);

	//void consumerInit(const std::set<std::string>& queues);
	//void consumerRelease();

	bool consumerIsThreadAvailable() const;
	bool consumerIsNoThreadRunning() const;
	bool consumerIsStateNotRunning() const;
	void consumerMessageHandlerThread(rd_kafka_message_t& rdKafkaMessage, esl::messaging::messagehandler::Interface::CreateMessageHandler createMessageHandler);
};

} /* namespace messaging */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_MESSAGING_CLIENT_H_ */
