#include <rdkafka4esl/com/basic/client/Connection.h>

namespace rdkafka4esl {
namespace com {
namespace basic {
namespace client {

Connection::Connection(std::shared_ptr<SharedConnection> aSharedConnection)
: sharedConnection(aSharedConnection)
{ }

esl::com::basic::client::Response Connection::send(const esl::com::basic::client::Request& request, esl::io::Output output, std::function<esl::io::Input (const esl::com::basic::client::Response&)> createInput) const {
	sharedConnection->send(request, std::move(output));
	return esl::com::basic::client::Response();
}

esl::com::basic::client::Response Connection::send(const esl::com::basic::client::Request& request, esl::io::Output output, esl::io::Input input) const {
	sharedConnection->send(request, std::move(output));
	return esl::com::basic::client::Response();
}

} /* namespace client */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */
