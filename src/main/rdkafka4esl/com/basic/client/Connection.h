#ifndef RDKAFKA4ESL_COM_BASIC_CLIENT_CONNECTION_H_
#define RDKAFKA4ESL_COM_BASIC_CLIENT_CONNECTION_H_

#include <rdkafka4esl/com/basic/client/SharedConnection.h>

#include <esl/com/basic/client/Connection.h>
#include <esl/com/basic/client/Request.h>
#include <esl/com/basic/client/Response.h>
#include <esl/io/Input.h>
#include <esl/io/Output.h>

#include <memory>
#include <functional>

namespace rdkafka4esl {
namespace com {
namespace basic {
namespace client {

class Connection final : public esl::com::basic::client::Connection {
public:
	Connection(std::shared_ptr<SharedConnection> sharedConnection);

	esl::com::basic::client::Response send(const esl::com::basic::client::Request& request, esl::io::Output output, std::function<esl::io::Input (const esl::com::basic::client::Response&)> createInput) const override;
	esl::com::basic::client::Response send(const esl::com::basic::client::Request& request, esl::io::Output output, esl::io::Input input) const override;

private:
	std::shared_ptr<SharedConnection> sharedConnection;
};

} /* namespace client */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_COM_BASIC_CLIENT_CONNECTION_H_ */
