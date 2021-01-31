#ifndef RDKAFKA4ESL_MESSAGING_CONSUMER_H_
#define RDKAFKA4ESL_MESSAGING_CONSUMER_H_

#include <esl/messaging/Consumer.h>
#include <esl/messaging/messagehandler/Interface.h>

#include <string>
#include <set>
#include <cstdint>

namespace rdkafka4esl {
namespace messaging {

class Client;

class Consumer : public esl::messaging::Consumer {
public:
	Consumer(Client& client);

	void start(const std::set<std::string>& queues, esl::messaging::messagehandler::Interface::CreateMessageHandler createMessageHandler, std::uint16_t numThreads, bool stopOnEOF);
	void stop();
	bool wait(std::uint32_t ms);

private:
	Client& client;
};

} /* namespace messaging */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_MESSAGING_CONSUMER_H_ */
