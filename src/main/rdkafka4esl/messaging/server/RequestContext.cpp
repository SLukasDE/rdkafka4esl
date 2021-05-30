#include <rdkafka4esl/messaging/server/RequestContext.h>
#include <rdkafka4esl/messaging/server/Connection.h>

namespace rdkafka4esl {
namespace messaging {
namespace server {

namespace {
Connection connection;
} /* anonymous namespace */

RequestContext::RequestContext(const Socket& aSocket, rd_kafka_message_t& kafkaMessage)
: socket(aSocket),
  request(kafkaMessage)
{ }

esl::messaging::server::Connection& RequestContext::getConnection() const {
	return connection;
}

const Request& RequestContext::getRequest() const {
	return request;
}

esl::object::Interface::Object* RequestContext::findObject(const std::string& id) const {
	esl::messaging::server::Interface::Socket::ObjectFactory objectFactory = socket.getObjectFactory(id);
	if(objectFactory) {
		return objectFactory(*this);
	}
	return nullptr;
}

} /* namespace server */
} /* namespace messaging */
} /* namespace rdkafka4esl */
