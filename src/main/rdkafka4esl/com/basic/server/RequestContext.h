#ifndef RDKAFKA4ESL_COM_BASIC_SERVER_REQUESTCONTEXT_H_
#define RDKAFKA4ESL_COM_BASIC_SERVER_REQUESTCONTEXT_H_

#include <rdkafka4esl/com/basic/server/Socket.h>
#include <rdkafka4esl/com/basic/server/Request.h>
#include <rdkafka4esl/com/basic/server/ObjectContext.h>

#include <esl/com/basic/server/RequestContext.h>
#include <esl/com/basic/server/Connection.h>
//#include <esl/object/Object.h>
#include <esl/object/Context.h>

#include <librdkafka/rdkafka.h>

#include <string>

namespace rdkafka4esl {
namespace com {
namespace basic {
namespace server {

class RequestContext : public esl::com::basic::server::RequestContext {
public:
	RequestContext(const Socket& socket, rd_kafka_message_t& kafkaMessage);

	esl::com::basic::server::Connection& getConnection() const override;
	const Request& getRequest() const override;
	esl::object::Context& getObjectContext() override;
	const esl::object::Context& getObjectContext() const override;

private:
	const Socket& socket;
	Request request;
	ObjectContext objectContext;
};

} /* namespace server */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_COM_BASIC_SERVER_REQUESTCONTEXT_H_ */
