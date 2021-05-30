#ifndef RDKAFKA4ESL_MESSAGING_SERVER_REQUESTCONTEXT_H_
#define RDKAFKA4ESL_MESSAGING_SERVER_REQUESTCONTEXT_H_

#include <rdkafka4esl/messaging/server/Socket.h>
#include <rdkafka4esl/messaging/server/Request.h>

#include <esl/messaging/server/RequestContext.h>
#include <esl/messaging/server/Connection.h>
#include <esl/object/Interface.h>

#include <string>

#include <librdkafka/rdkafka.h>

namespace rdkafka4esl {
namespace messaging {
namespace server {

class RequestContext : public esl::messaging::server::RequestContext {
public:
	RequestContext(const Socket& socket, rd_kafka_message_t& kafkaMessage);

	esl::messaging::server::Connection& getConnection() const override;
	const Request& getRequest() const override;

protected:
	esl::object::Interface::Object* findObject(const std::string& id) const override;

private:
	const Socket& socket;
	Request request;
};

} /* namespace server */
} /* namespace messaging */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_MESSAGING_SERVER_REQUESTCONTEXT_H_ */
