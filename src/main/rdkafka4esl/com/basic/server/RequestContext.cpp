#include <rdkafka4esl/com/basic/server/RequestContext.h>
#include <rdkafka4esl/com/basic/server/Connection.h>

namespace rdkafka4esl {
namespace com {
namespace basic {
namespace server {

namespace {
Connection connection;
} /* anonymous namespace */

RequestContext::RequestContext(const Socket& aSocket, rd_kafka_message_t& kafkaMessage)
: socket(aSocket),
  request(kafkaMessage)
{ }

esl::com::basic::server::Connection& RequestContext::getConnection() const {
	return connection;
}

const Request& RequestContext::getRequest() const {
	return request;
}

} /* namespace server */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */
