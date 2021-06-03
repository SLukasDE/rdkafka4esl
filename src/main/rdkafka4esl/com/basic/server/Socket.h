#ifndef RDKAFKA4ESL_COM_BASIC_SERVER_SOCKET_H_
#define RDKAFKA4ESL_COM_BASIC_SERVER_SOCKET_H_

#include <esl/com/basic/server/Interface.h>
#include <esl/com/basic/server/requesthandler/Interface.h>

#include <set>
#include <string>
#include <memory>
#include <map>

namespace rdkafka4esl {
namespace com {
namespace basic {

namespace broker {
class Client;
} /* namespace broker */

namespace server {

class Socket final : public esl::com::basic::server::Interface::Socket {
public:
	Socket(broker::Client& client);

	void addObjectFactory(const std::string& id, ObjectFactory objectFactory) override;
	ObjectFactory getObjectFactory(const std::string& id) const;

	void listen(const std::set<std::string>& notifications, esl::com::basic::server::requesthandler::Interface::CreateInput createInput) override;
	void release() override;
	bool wait(std::uint32_t ms) override;

private:
	broker::Client& client;

	std::map<std::string, ObjectFactory> objectFactories;
};

} /* namespace server */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_COM_BASIC_SERVER_SOCKET_H_ */
