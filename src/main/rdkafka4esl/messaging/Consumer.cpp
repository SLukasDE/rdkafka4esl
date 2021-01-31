#include <rdkafka4esl/messaging/Consumer.h>
#include <rdkafka4esl/messaging/Client.h>

namespace rdkafka4esl {
namespace messaging {

Consumer::Consumer(Client& aClient)
: esl::messaging::Consumer(),
  client(aClient)
{ }

void Consumer::start(const std::set<std::string>& queues, esl::messaging::messagehandler::Interface::CreateMessageHandler createMessageHandler, std::uint16_t numThreads, bool stopOnEOF) {
	client.consumerStart(queues, createMessageHandler, numThreads, stopOnEOF);
}

void Consumer::stop() {
	client.consumerStop();
}

bool Consumer::wait(std::uint32_t ms) {
	return client.consumerWait(ms);
}

} /* namespace messaging */
} /* namespace rdkafka4esl */
