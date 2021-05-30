#ifndef RDKAFKA4ESL_MESSAGING_SERVER_SOCKET_H_
#define RDKAFKA4ESL_MESSAGING_SERVER_SOCKET_H_

#include <esl/messaging/server/Interface.h>
#include <esl/messaging/server/requesthandler/Interface.h>

#include <set>
#include <string>
#include <memory>
#include <map>

namespace rdkafka4esl {
namespace messaging {

namespace broker {
class Client;
} /* namespace broker */

namespace server {

class Socket final : public esl::messaging::server::Interface::Socket {
public:
	Socket(broker::Client& client);

	void addObjectFactory(const std::string& id, ObjectFactory objectFactory) override;
	ObjectFactory getObjectFactory(const std::string& id) const;

	void listen(const std::set<std::string>& notifications, esl::messaging::server::requesthandler::Interface::CreateInput createInput) override;
	void release() override;
	bool wait(std::uint32_t ms) override;

private:
	broker::Client& client;

	std::map<std::string, ObjectFactory> objectFactories;
};

} /* namespace server */
} /* namespace messaging */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_MESSAGING_SERVER_SOCKET_H_ */
