#include <rdkafka4esl/messaging/ProducerFactory.h>
#include <rdkafka4esl/messaging/Client.h>

namespace rdkafka4esl {
namespace messaging {

ProducerFactory::ProducerFactory(Client& aClient, const std::string& aId, std::vector<std::pair<std::string, std::string>> aParameters)
: client(aClient),
  id(aId),
  parameters(std::move(aParameters))
{
	client.createdProducer();
}

ProducerFactory::~ProducerFactory() {
	client.destroyedProducer();
}

std::unique_ptr<esl::messaging::Producer> ProducerFactory::createProducer() {
	return client.createProducer(id, parameters);
}

} /* namespace messaging */
} /* namespace rdkafka4esl */
