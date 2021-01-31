#ifndef RDKAFKA4ESL_MESSAGING_PRODUCERFACTORY_H_
#define RDKAFKA4ESL_MESSAGING_PRODUCERFACTORY_H_

#include <esl/messaging/Interface.h>

#include <string>
#include <vector>
#include <utility>
#include <memory>

namespace rdkafka4esl {
namespace messaging {

class Client;

class ProducerFactory : public esl::messaging::Interface::ProducerFactory {
public:
	ProducerFactory(Client& client, const std::string& id, std::vector<std::pair<std::string, std::string>> parameters);
	~ProducerFactory();

	std::unique_ptr<esl::messaging::Producer> createProducer() override;

private:
	Client& client;
	const std::string id;
	std::vector<std::pair<std::string, std::string>> parameters;
};

} /* namespace messaging */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_MESSAGING_PRODUCERFACTORY_H_ */
